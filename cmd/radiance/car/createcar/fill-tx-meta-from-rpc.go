package createcar

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gogo/protobuf/proto"
	binc "github.com/rpcpool/yellowstone-faithful/parse_legacy_transaction_status_meta"
	"go.firedancer.io/radiance/pkg/ledger_bigtable"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type BigTableFiller struct {
	btClient *bigtable.Client
	table    *bigtable.Table
	storage  *BlockFillerStorage
}

func NewBigTableFiller(ctx context.Context, fillerDB *BlockFillerStorage) (*BigTableFiller, error) {
	btClient, err := ledger_bigtable.MainnetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create bigtable client: %w", err)
	}

	table := btClient.Open(ledger_bigtable.BlocksTable)

	return &BigTableFiller{
		btClient: btClient,
		table:    table,
		storage:  fillerDB,
	}, nil
}

type ParsedBlock = map[solana.Signature][]byte

var ErrTxMetaNotFound = errors.New("tx not found in cached block")

// GetBlock will try to get the block from cache first, then from BigTable.
func (f *BigTableFiller) GetBlock(ctx context.Context, slot uint64) (ParsedBlock, error) {
	if f == nil || f.storage == nil {
		return nil, errors.New("filler is nil")
	}
	{
		err := f.fetchAndSaveBlockIfNotFound(ctx, slot)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch and save block %d: %w", slot, err)
		}
	}
	rawBlock, encoding, err := f.storage.Get(slot)
	if err != nil {
		return nil, fmt.Errorf("failed to get block %d from fillerDB: %w", slot, err)
	}
	switch encoding {
	case ledger_bigtable.BigTableDataEncodingProto:
		{
			parsed, err := ledger_bigtable.ParseProtoBlock(rawBlock)
			if err != nil {
				return nil, fmt.Errorf("failed to parse proto block %d: %w", slot, err)
			}
			parsedBlock := make(ParsedBlock, len(parsed.Transactions))
			for _, tx := range parsed.Transactions {
				sig := solana.SignatureFromBytes(tx.Transaction.Signatures[0])
				meta := tx.Meta
				metaBuf, err := proto.Marshal(meta)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal transaction meta for block %d: %w", slot, err)
				}
				parsedBlock[sig] = metaBuf
			}
			return parsedBlock, nil
		}
	case ledger_bigtable.BigTableDataEncodingBincode:
		{
			parsed, err := ledger_bigtable.ParseBincodeBlock(rawBlock)
			if err != nil {
				return nil, fmt.Errorf("failed to parse bincode block %d: %w", slot, err)
			}
			parsedBlock := make(ParsedBlock, len(parsed.Transactions))
			{
				for _, tx := range parsed.Transactions {
					sig := tx.Transaction.Signatures[0]
					converted := &binc.TransactionStatusMeta{}
					if tx.Meta.Err == nil {
						converted.Status = &binc.Result__Ok{}
					} else {
						converted.Status = &binc.Result__Err{
							Value: *tx.Meta.Err,
						}
					}
					converted.Fee = tx.Meta.Fee
					converted.PostBalances = tx.Meta.PostBalances
					converted.PreBalances = tx.Meta.PreBalances
					metaBuf, err := converted.BincodeSerializeStored()
					if err != nil {
						panic(err)
					}
					parsedBlock[sig] = metaBuf
				}
			}
			return parsedBlock, nil
		}
	default:
		return nil, fmt.Errorf("unknown encoding %s for block %d", encoding, slot)
	}
	return nil, fmt.Errorf("block %d is not in fillerDB", slot)
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

func (f *BigTableFiller) fetchAndSaveBlockIfNotFound(
	ctx context.Context,
	slot uint64,
) error {
	// check if the block is already in the fillerDB
	has, err := f.storage.Has(slot)
	if err != nil {
		return fmt.Errorf("failed to check if block %d is in fillerDB: %w", slot, err)
	}
	if has {
		klog.Infof("Block %d is already in fillerDB", slot)
		return nil
	}

	// download the block
	blockRaw, blockEncoding, err := retryExpotentialBackoff(NumBigTableRetries, func() ([]byte, ledger_bigtable.BigTableDataEncoding, error) {
		return f.GetRawBlockWithOpts(ctx, slot)
	})
	if err != nil {
		return fmt.Errorf("failed to get block %d: %w", slot, err)
	}
	if blockRaw == nil {
		return fmt.Errorf("block %d is nil", slot)
	}

	// save it to FillerDB
	err = f.storage.Set(slot, blockEncoding, blockRaw)
	if err != nil {
		return fmt.Errorf("failed to save block %d to fillerDB: %w", slot, err)
	}

	klog.Infof("Block %d downloaded and saved to block filler storage", slot)
	return nil
}

func (f *BigTableFiller) PrefetchBlocks(
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
					blockRaw, blockEncoding, err := retryExpotentialBackoff(NumBigTableRetries, func() ([]byte, ledger_bigtable.BigTableDataEncoding, error) {
						return f.GetRawBlockWithOpts(
							ctx,
							slot,
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
					err = f.storage.Set(slot, blockEncoding, blockRaw)
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

func (f *BigTableFiller) GetRawBlockWithOpts(
	ctx context.Context,
	slot uint64,
) ([]byte, ledger_bigtable.BigTableDataEncoding, error) {
	rowKey := ledger_bigtable.SlotKey(slot)
	row, err := f.table.ReadRow(ctx, rowKey)
	if err != nil {
		log.Fatalf("Could not read row: %v", err)
	}

	rawBlockBytes, encoding, err := ledger_bigtable.GetRawUncompressedBlock(row)
	if err != nil {
		log.Fatalf("Could not parse row: %v", err)
		return nil, encoding, fmt.Errorf("failed to parse row: %w", err)
	}
	if rawBlockBytes == nil {
		return nil, encoding, rpc.ErrNotConfirmed
	}
	return rawBlockBytes, encoding, nil
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

var NumBigTableRetries = uint8(4)

func retryExpotentialBackoff[T any, Q any](times uint8, fn func() (T, Q, error)) (T, Q, error) {
	var err error
	var res T
	var resQ Q
	for i := uint8(0); i < times; i++ {
		res, resQ, err = fn()
		if err == nil {
			return res, resQ, nil
		}
		time.Sleep(time.Duration(2<<i) * time.Second)
	}
	return res, resQ, err
}
