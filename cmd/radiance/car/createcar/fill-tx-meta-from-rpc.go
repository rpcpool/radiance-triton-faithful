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
	rpcClient *rpc.Client
	storage   *BlockFillerStorage
}

func NewRpcFiller(endpoint string, fillerDB *BlockFillerStorage) (*RpcFiller, error) {
	rpcClient := rpc.New(endpoint)
	// try a sample request to check if the endpoint is valid;
	_, err := rpcClient.GetSlot(context.Background(), rpc.CommitmentFinalized)
	if err != nil {
		return nil, err
	}
	return &RpcFiller{
		rpcClient: rpcClient,
		storage:   fillerDB,
	}, nil
}

type ParsedBlock = map[solana.Signature][]byte

func fromRpcBlockToParsedBlock(
	block *rpc.GetBlockResult,
) (ParsedBlock, error) {
	blockMeta := make(ParsedBlock)
	for _, tx := range block.Transactions {
		parsedtx, err := tx.GetTransaction()
		if err != nil {
			return nil, fmt.Errorf("failed to get parsed tx: %w", err)
		}
		sig := parsedtx.Signatures[0]
		txMeta, err := fromRpcTxToProtobufTxMeta(&tx)
		if err != nil {
			return nil, fmt.Errorf("failed to convert tx meta to protobuf for tx %s: %w", sig, err)
		}
		encoded, err := proto.Marshal(txMeta)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tx meta to protobuf for tx %s: %w", sig, err)
		}
		if len(encoded) == 0 {
			return nil, fmt.Errorf("failed to marshal tx meta to protobuf for tx %s: empty encoded", sig)
		}
		blockMeta[sig] = encoded
	}
	return blockMeta, nil
}

var ErrTxMetaNotFound = errors.New("tx not found in cached block")

// GetBlock will try to get the block from RPC.
func (f *RpcFiller) GetBlock(ctx context.Context, slot uint64) (ParsedBlock, error) {
	if f == nil || f.storage == nil {
		return nil, errors.New("filler is nil")
	}
	got, err := f.storage.Get(slot)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %d from fillerDB: %w", slot, err)
	}
	block, err := bytesToRpcBlock(got)
	if err != nil {
		return nil, fmt.Errorf("failed to convert block %d to protobuf: %w", slot, err)
	}
	parsedBlock, err := fromRpcBlockToParsedBlock(block)
	if err != nil {
		return nil, fmt.Errorf("failed to convert block %d to protobuf: %w", slot, err)
	}
	return parsedBlock, nil
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

func (f *RpcFiller) PrefetchBlocks(
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
				// check if the block is already in the fillerDB
				has, err := f.storage.Has(slot)
				if err != nil {
					klog.Errorf("failed to check if block %d is in fillerDB: %v", slot, err)
					return nil
				}
				if has {
					// msg.Print(fmt.Sprintf("Block %d is already in fillerDB", slot))
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
					err = f.storage.Set(slot, *blockRaw)
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

func bytesToRpcBlock(raw []byte) (*rpc.GetBlockResult, error) {
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
