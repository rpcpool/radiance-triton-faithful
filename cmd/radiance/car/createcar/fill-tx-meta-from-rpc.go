package createcar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"google.golang.org/protobuf/proto"
)

type RpcFiller struct {
	rpcClient  *rpc.Client
	slockLocks *SlotLocks
	blockCache *BlockCache
}

func NewRpcFiller(endpoint string) (*RpcFiller, error) {
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
	}, nil
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
			parsedtx, err := tx.GetParsedTransaction()
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
	block, err := retry(3, func() (*rpc.GetBlockResult, error) {
		return f.rpcClient.GetBlock(ctx, slot)
	})
	if err != nil {
		return nil, err
	}
	return block, nil
}

func retry[T any](times uint8, fn func() (T, error)) (T, error) {
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
