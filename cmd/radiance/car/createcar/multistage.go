package createcar

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/ipld/ipldbindcode"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/registry"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/ipld/ipldgen"
	"go.firedancer.io/radiance/pkg/shred"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"google.golang.org/protobuf/proto"
)

const (
	KindTransaction = iota
	KindEntry
	KindBlock
	KindSubset
	KindEpoch
)

type Multistage struct {
	settingConcurrency int
	carFilepath        string

	linkSystem    linking.LinkSystem
	linkPrototype cidlink.LinkPrototype

	carFile    *os.File
	storageCar *storage.StorageCar

	mu  sync.Mutex
	reg *registry.Registry
}

func NewMultistage(finalCARFilepath string) (*Multistage, error) {
	lsys := cidlink.DefaultLinkSystem()
	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,                          // TODO: what is this?
			Codec:    uint64(multicodec.DagCbor), // See the multicodecs table: https://github.com/multiformats/multicodec/
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}

	cw := &Multistage{
		carFilepath:   finalCARFilepath,
		linkSystem:    lsys,
		linkPrototype: lp,
	}
	{
		exists, err := fileExists(finalCARFilepath)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("file %s already exists", finalCARFilepath)
		}
		file, err := os.Create(finalCARFilepath)
		if err != nil {
			return nil, fmt.Errorf("failed to create file: %w", err)
		}
		cw.carFile = file

		// make a cid with the right length that we eventually will patch with the root.
		proxyRoot, err := cw.linkPrototype.WithCodec(uint64(multicodec.DagCbor)).Sum([]byte{})
		if err != nil {
			return nil, fmt.Errorf("failed to create proxy root: %w", err)
		}

		{
			registryFilepath := filepath.Join(filepath.Dir(finalCARFilepath), "registry.bin")
			reg, err := registry.New(registryFilepath, proxyRoot.ByteLen())
			if err != nil {
				return nil, fmt.Errorf("failed to create registry: %w", err)
			}
			cw.reg = reg
		}

		writableCar, err := storage.NewReadableWritable(
			file,
			[]cid.Cid{proxyRoot}, // NOTE: the root CIDs are replaced later.
			car.UseWholeCIDs(true),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create writable CAR: %w", err)
		}
		cw.storageCar = writableCar

		// Set the link system to use the writable CAR.
		cw.linkSystem.SetWriteStorage(writableCar)
		cw.linkSystem.SetReadStorage(writableCar)
	}

	return cw, nil
}

func (cw *Multistage) Finalize() error {
	if cw.carFile == nil {
		return fmt.Errorf("car file is nil")
	}
	if cw.storageCar == nil {
		return fmt.Errorf("storageCar is nil")
	}
	return cw.storageCar.Finalize()
}

func (cw *Multistage) Close() error {
	if cw.carFile != nil {
		return cw.carFile.Close()
	}
	return nil
}

// replaceRoot is used to replace the root CID in the CAR file.
// This is done when we have the epoch root CID.
func (cw *Multistage) replaceRoot(newRoot cid.Cid) error {
	return carv2.ReplaceRootsInFile(
		cw.carFilepath,
		[]cid.Cid{newRoot},
	)
}

func (cw *Multistage) SetConcurrency(concurrency int) {
	cw.settingConcurrency = concurrency
}

func (cw *Multistage) Store(lnkCtx linking.LinkContext, n datamodel.Node) (datamodel.Link, error) {
	// TODO: is this safe to use concurrently?
	cw.mu.Lock()
	defer cw.mu.Unlock()

	return cw.linkSystem.Store(
		lnkCtx,
		cw.linkPrototype,
		n,
	)
}

// OnBlock is called when a block is received.
// This fuction can be called concurrently.
// This function creates the subgraph for the block and writes the objects to the CAR file,
// and then registers the block CID.
func (cw *Multistage) OnBlock(
	slotMeta blockstore.SlotMeta,
	entries []blockstore.Entries,
	metas []*confirmed_block.TransactionStatusMeta,
) (datamodel.Link, error) {
	// TODO:
	// - [ ] construct the block
	// - [ ] register the block CID
	blockRootLink, err := cw.constructBlock(slotMeta, entries, metas)
	if err != nil {
		return nil, fmt.Errorf("failed to construct epoch: %w", err)
	}
	return blockRootLink, cw.reg.SetCID(slotMeta.Slot, blockRootLink.(cidlink.Link).Cid.Bytes())
}

// TODO:
// - [ ] in stage 1, we create a CAR file for all the slots
// - there's a registry of all slots that have been written, and their CIDs (very important)
// - write is protected by a mutex
// - [ ] in stage 2, we add the missing parts of the DAG (same CAR file)

func (cw *Multistage) constructBlock(
	slotMeta blockstore.SlotMeta,
	entries []blockstore.Entries,
	metas []*confirmed_block.TransactionStatusMeta,
) (datamodel.Link, error) {
	shredding, err := cw.buildShredding(slotMeta, entries)
	if err != nil {
		return nil, fmt.Errorf("failed to build shredding: %w", err)
	}
	blockNode, err := qp.BuildMap(ipldbindcode.Prototypes.Block, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "kind", qp.Int(KindBlock))
		qp.MapEntry(ma, "slot", qp.Int(int64(slotMeta.Slot)))
		qp.MapEntry(ma, "shredding",
			qp.List(-1, func(la datamodel.ListAssembler) {
				for _, shred := range shredding {
					qp.ListEntry(la,
						qp.Map(-1, func(ma datamodel.MapAssembler) {
							qp.MapEntry(ma, "entryEndIdx", qp.Int(int64(shred.EntryEndIdx)))
							qp.MapEntry(ma, "shredEndIdx", qp.Int(int64(shred.ShredEndIdx)))
						}),
					)
				}
			}),
		)
		qp.MapEntry(ma, "entries",
			qp.List(-1, func(la datamodel.ListAssembler) {
				// - call onEntry() which will write the CIDs of the Entries to the Block object
				err := cw.onEntry(
					slotMeta,
					entries,
					metas,
					func(cidOfAnEntry datamodel.Link) {
						qp.ListEntry(la,
							qp.Link(cidOfAnEntry),
						)
					},
				)
				if err != nil {
					panic(err)
				}
			}),
		)
	})
	if err != nil {
		return nil, err
	}
	// printer.Print(blockNode)

	// - store the Block object to storage
	blockLink, err := cw.Store(
		linking.LinkContext{},
		blockNode.(schema.TypedNode).Representation(),
	)
	if err != nil {
		return nil, err
	}
	return blockLink, nil
}

func (cw *Multistage) buildShredding(
	slotMeta blockstore.SlotMeta,
	entries []blockstore.Entries,
) ([]ipldbindcode.Shredding, error) {
	entryNum := 0
	txNum := 0
	out := make([]ipldbindcode.Shredding, 0)
	for i, batch := range entries {
		for j, entry := range batch.Entries {

			pos := ipldgen.EntryPos{
				Slot:       slotMeta.Slot,
				EntryIndex: entryNum,
				Batch:      i,
				BatchIndex: j,
				LastShred:  -1,
			}
			if j == len(batch.Entries)-1 {
				// We map "last shred of batch" to each "last entry of batch"
				// so we can reconstruct the shred/entry-batch assignments.
				if i >= len(slotMeta.EntryEndIndexes) {
					return nil, fmt.Errorf("out-of-bounds batch index %d (have %d batches in slot %d)",
						i, len(slotMeta.EntryEndIndexes), slotMeta.Slot)
				}
				pos.LastShred = int(slotMeta.EntryEndIndexes[i])
			}

			out = append(out, ipldbindcode.Shredding{
				EntryEndIdx: (pos.EntryIndex),
				ShredEndIdx: (pos.LastShred),
			})
			txNum += len(entry.Txns)
			entryNum++
		}
	}
	return out, nil
}

func (cw *Multistage) onEntry(
	slotMeta blockstore.SlotMeta,
	entries []blockstore.Entries,
	metas []*confirmed_block.TransactionStatusMeta,
	onEntry func(cidOfAnEntry datamodel.Link),
) error {
	entryNum := 0
	txNum := 0
	for _, batch := range entries {
		for _, entry := range batch.Entries {

			// TODO: is this correct?
			transactionMetas := metas[txNum : txNum+len(entry.Txns)]

			// - construct a Entry object
			entryNode, err := qp.BuildMap(ipldbindcode.Prototypes.Entry, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "kind", qp.Int(KindEntry))
				qp.MapEntry(ma, "numHashes", qp.Int(int64(entry.NumHashes)))
				qp.MapEntry(ma, "hash", qp.Bytes(entry.Hash[:]))

				qp.MapEntry(ma, "transactions",
					qp.List(-1, func(la datamodel.ListAssembler) {
						// - call onTx() which will write the CIDs of the Transactions to the Entry object
						cw.onTx(
							slotMeta,
							entry,
							transactionMetas,
							func(cidOfATx datamodel.Link) {
								qp.ListEntry(la,
									qp.Link(cidOfATx),
								)
							},
						)
					}),
				)
			})
			if err != nil {
				return fmt.Errorf("failed to build Entry %d: %w", entryNum, err)
			}
			// printer.Print(entryNode)

			// - store the Entry object to storage
			entryLink, err := cw.Store(
				linking.LinkContext{},
				entryNode.(schema.TypedNode).Representation(),
			)
			if err != nil {
				return fmt.Errorf("failed to store Entry %d: %w", entryNum, err)
			}
			onEntry(entryLink)

			txNum += len(entry.Txns)
			entryNum++
		}
	}
	return nil
}

func (cw *Multistage) onTx(
	slotMeta blockstore.SlotMeta,
	entry shred.Entry,
	metas []*confirmed_block.TransactionStatusMeta,
	onTx func(cidOfATx datamodel.Link),
) error {
	for txIndex, transaction := range entry.Txns {
		firstSig := transaction.Signatures[0]

		txData, err := transaction.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal transaction %s: %w", firstSig, err)
		}
		meta := metas[txIndex]
		txMeta, err := proto.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal transaction meta %s: %w", firstSig, err)
		}

		// - construct a Transaction object
		transactionNode, err := qp.BuildMap(ipldbindcode.Prototypes.Transaction, -1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "kind", qp.Int(KindTransaction))
			qp.MapEntry(ma, "data", qp.Bytes(txData))
			qp.MapEntry(ma, "metadata", qp.Bytes(txMeta))
		})
		if err != nil {
			return fmt.Errorf("failed to construct Transaction %s: %w", firstSig, err)
		}
		// printer.Print(transactionNode)

		// - store the Transaction object to storage
		txLink, err := cw.Store(
			linking.LinkContext{},
			transactionNode.(schema.TypedNode).Representation(),
		)
		if err != nil {
			return fmt.Errorf("failed to store Transaction %s: %w", firstSig, err)
		}
		onTx(txLink)
	}
	return nil
}
