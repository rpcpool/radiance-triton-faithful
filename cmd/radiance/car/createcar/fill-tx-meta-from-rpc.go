package createcar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"
)

type RpcFiller struct {
	rpcClient  *rpc.Client
	slockLocks *SlotLocks
	blockCache *BlockCache
	fillerDB   *BlockFillerStorage
}

func NewRpcFiller(endpoint string, fillerDB *BlockFillerStorage) (*RpcFiller, error) {
	rpcClient := rpc.New(endpoint)
	// try a sample request to check if the endpoint is valid;
	_, err := rpcClient.GetSlot(context.Background(), rpc.CommitmentFinalized)
	if err != nil {
		return nil, err
	}
	return &RpcFiller{
		rpcClient:  rpcClient,
		slockLocks: NewSlotLocks(),
		blockCache: NewBlockCache(),
		fillerDB:   fillerDB,
	}, nil
}

func (f *RpcFiller) RemoveFromCache(slot uint64) {
	if f == nil || f.blockCache == nil {
		return
	}
	f.blockCache.DeleteBlock(slot)
}

func (f *RpcFiller) FillTxMetaFromRPC(
	ctx context.Context,
	slot uint64,
	tx solana.Signature,
) ([]byte, error) {
	{
		// try from cache first
		block, ok := f.blockCache.GetBlock(slot)
		if ok {
			if txMeta, ok := block[tx]; ok {
				return txMeta, nil
			}
			// the block doesn't have the tx; ERROR
			return nil, ErrTxMetaNotFound
		}
	}
	// This will either fetch the block, parse it, and cache it,
	// or wait for the current download to finish,
	// or return if the download already finished.
	// - not started (can start)
	// - started (wait until finished)
	// - finished (check cache again)
	thisRun, err := f.slockLocks.OncePerSlot(slot, func() error {
		// TODO: make so that only one request is made for the same slot, and other requests are blocked until the first one is done, and
		// then the other requests can get the data from the cache.
		block, err := f.GetBlock(ctx, slot)
		if err != nil {
			return fmt.Errorf("failed to get block: %w", err)
		}
		blockMeta := make(ParsedBlock)

		for _, tx := range block.Transactions {
			parsedtx, err := tx.GetTransaction()
			if err != nil {
				return fmt.Errorf("failed to get parsed tx: %w", err)
			}
			sig := parsedtx.Signatures[0]
			txMeta, err := fromRpcTxToProtobufTxMeta(&tx)
			if err != nil {
				return fmt.Errorf("failed to convert tx meta to protobuf for tx %s: %w", sig, err)
			}
			encoded, err := proto.Marshal(txMeta)
			blockMeta[sig] = encoded
		}
		f.blockCache.SetBlock(slot, blockMeta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !thisRun {
		startedWaitingAt := time.Now()
		for {
			// another thread is downloading the block; wait for it to finish;
			time.Sleep(100 * time.Millisecond)
			block, ok := f.blockCache.GetBlock(slot)
			if ok {
				if txMeta, ok := block[tx]; ok {
					return txMeta, nil
				}
				// the block doesn't have the tx; ERROR
				return nil, ErrTxMetaNotFound
			}
			timeout := 60 * time.Second
			if time.Since(startedWaitingAt) > timeout {
				return nil, fmt.Errorf("timed out waiting for block %d to be downloaded", slot)
			}
		}
	} else {
		// the block was downloaded and cached; check the cache
		block, ok := f.blockCache.GetBlock(slot)
		if ok {
			if txMeta, ok := block[tx]; ok {
				return txMeta, nil
			}
			// the block doesn't have the tx; ERROR
			return nil, ErrTxMetaNotFound
		}
		// the block is not in the cache; ERROR
		return nil, ErrTxMetaNotFound
	}
}

func (f *RpcFiller) lockedOp(slot uint64, op func()) {
}

var ErrTxMetaNotFound = errors.New("tx not found in cached block")

// GetBlock will try to get the block from RPC.
func (f *RpcFiller) GetBlock(ctx context.Context, slot uint64) (*rpc.GetBlockResult, error) {
	klog.Infof("Fetching block %d from RPC", slot)
	// try getting it from FillerDB first
	{
		blockRaw, err := f.fillerDB.Get(slot)
		if err == nil {
			return bytesToBlock(blockRaw)
		}
	}
	blockRaw, err := retryExpotentialBackoff(NumRpcRetries, func() (*json.RawMessage, error) {
		return f.GetRawBlockWithOpts(
			ctx,
			slot,
			&rpc.GetBlockOpts{
				Encoding:                       solana.EncodingBase64,
				MaxSupportedTransactionVersion: &rpc.MaxSupportedTransactionVersion1,
			},
		)
	})
	if err != nil {
		return nil, err
	}
	if blockRaw == nil {
		return nil, fmt.Errorf("block is nil")
	}
	// save it to FillerDB
	err = f.fillerDB.Set(slot, *blockRaw)
	if err != nil {
		return nil, fmt.Errorf("failed to save block to fillerDB: %w", err)
	}
	return bytesToBlock(*blockRaw)
}

type MessageLine struct {
	mu sync.Mutex
}

func (m *MessageLine) Print(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// clear the line and print the message
	fmt.Printf("\r%s", msg)
}

func (f *RpcFiller) FetchBlocksToFillerStorage(
	ctx context.Context,
	concurrency int,
	slots []uint64,
) {
	wg := errgroup.Group{}
	wg.SetLimit(concurrency)
	defer wg.Wait()

	msg := new(MessageLine)
	numDownloaded := new(atomic.Uint64)
	numTotal := uint64(len(slots))

	for _, slot := range slots {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		wg.Go(func() error {
			return func(slot uint64) error {
				// check if the block is already in the cache
				_, ok := f.blockCache.GetBlock(slot)
				if ok {
					return nil
				}
				// check if the block is already in the fillerDB
				_, err := f.fillerDB.Get(slot)
				if err == nil {
					msg.Print(fmt.Sprintf("Block %d is already in fillerDB", slot))
					numDownloaded.Add(1)
					return nil
				}
				// download the block
				{
					blockRaw, err := retryExpotentialBackoff(NumRpcRetries, func() (*json.RawMessage, error) {
						return f.GetRawBlockWithOpts(
							ctx,
							slot,
							&rpc.GetBlockOpts{
								Encoding:                       solana.EncodingBase64,
								MaxSupportedTransactionVersion: &rpc.MaxSupportedTransactionVersion1,
							},
						)
					})
					if err != nil {
						klog.Errorf("failed to get block %d: %v", slot, err)
						return nil
					}
					if blockRaw == nil {
						klog.Errorf("block %d is nil", slot)
						return nil
					}
					// save it to FillerDB
					err = f.fillerDB.Set(slot, *blockRaw)
					if err != nil {
						klog.Errorf("failed to save block %d to fillerDB: %v", slot, err)
						return nil
					}
					currentCountDownloaded := numDownloaded.Add(1)
					percentDone := float64(currentCountDownloaded) / float64(numTotal) * 100
					msg.Print(fmt.Sprintf("Block %d downloaded and saved to block filler storage; %.2f%% done", slot, percentDone))
				}
				return nil
			}(slot)
		})
	}
}

func bytesToBlock(raw []byte) (*rpc.GetBlockResult, error) {
	block := new(rpc.GetBlockResult)
	err := json.Unmarshal(raw, block)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}
	return block, nil
}

func (f *RpcFiller) GetRawBlockWithOpts(
	ctx context.Context,
	slot uint64,
	opts *rpc.GetBlockOpts,
) (out *json.RawMessage, err error) {
	obj := rpc.M{
		"encoding": solana.EncodingBase64,
	}

	if opts != nil {
		if opts.TransactionDetails != "" {
			obj["transactionDetails"] = opts.TransactionDetails
		}
		if opts.Rewards != nil {
			obj["rewards"] = opts.Rewards
		}
		if opts.Commitment != "" {
			obj["commitment"] = opts.Commitment
		}
		if opts.Encoding != "" {
			if !solana.IsAnyOfEncodingType(
				opts.Encoding,
				// Valid encodings:
				// solana.EncodingJSON, // TODO
				solana.EncodingJSONParsed, // TODO
				solana.EncodingBase58,
				solana.EncodingBase64,
				solana.EncodingBase64Zstd,
			) {
				return nil, fmt.Errorf("provided encoding is not supported: %s", opts.Encoding)
			}
			obj["encoding"] = opts.Encoding
		}
		if opts.MaxSupportedTransactionVersion != nil {
			obj["maxSupportedTransactionVersion"] = *opts.MaxSupportedTransactionVersion
		}
	}

	params := []interface{}{slot, obj}

	err = f.rpcClient.RPCCallForInto(ctx, &out, "getBlock", params)
	if err != nil {
		return nil, err
	}
	if out == nil {
		// Block is not confirmed.
		return nil, rpc.ErrNotConfirmed
	}
	return
}

func retryLinear[T any](times uint8, fn func() (T, error)) (T, error) {
	var err error
	var res T
	for i := uint8(0); i < times; i++ {
		res, err = fn()
		if err == nil {
			return res, nil
		}
	}
	return res, err
}

var NumRpcRetries = uint8(4)

func retryExpotentialBackoff[T any](times uint8, fn func() (T, error)) (T, error) {
	var err error
	var res T
	for i := uint8(0); i < times; i++ {
		res, err = fn()
		if err == nil {
			return res, nil
		}
		time.Sleep(time.Duration(2<<i) * time.Second)
	}
	return res, err
}
