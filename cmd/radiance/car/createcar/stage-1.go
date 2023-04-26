package createcar

import (
	"fmt"
	"os"

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
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/ipld/ipldgen"
	"go.firedancer.io/radiance/pkg/shred"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"google.golang.org/protobuf/proto"
)

type StageOne struct {
	settingConcurrency int
	linkSystem         linking.LinkSystem
	linkPrototype      cidlink.LinkPrototype
}

func NewStageOne() *StageOne {
	lsys := cidlink.DefaultLinkSystem()
	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,                          // TODO: what is this?
			Codec:    uint64(multicodec.DagCbor), // See the multicodecs table: https://github.com/multiformats/multicodec/
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}

	return &StageOne{
		linkSystem:    lsys,
		linkPrototype: lp,
	}
}

func (cw *StageOne) SetConcurrency(concurrency int) {
	cw.settingConcurrency = concurrency
}

func (cw *StageOne) Store(lnkCtx linking.LinkContext, n datamodel.Node) (datamodel.Link, error) {
	return cw.linkSystem.Store(
		lnkCtx,
		cw.linkPrototype,
		n,
	)
}

func (cw *StageOne) Build(
	filepath string,
	slotMeta blockstore.SlotMeta,
	entries []blockstore.Entries,
	metas []*confirmed_block.TransactionStatusMeta,
) (datamodel.Link, error) {
	exists, err := fileExists(filepath)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("file %s already exists", filepath)
	}
	file, err := os.Create(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// make a cid with the right length that we eventually will patch with the root.
	proxyRoot, err := cw.linkPrototype.WithCodec(uint64(multicodec.DagCbor)).Sum([]byte{})
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy root: %w", err)
	}

	writableCar, err := storage.NewReadableWritable(
		file,
		[]cid.Cid{proxyRoot}, // NOTE: the root CIDs are replaced later.
		car.UseWholeCIDs(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create writable CAR: %w", err)
	}

	// Set the link system to use the writable CAR.
	cw.linkSystem.SetWriteStorage(writableCar)
	cw.linkSystem.SetReadStorage(writableCar)

	blockRootLink, err := cw.constructBlock(slotMeta, entries, metas)
	if err != nil {
		return nil, fmt.Errorf("failed to construct epoch: %w", err)
	}

	err = writableCar.Finalize()
	if err != nil {
		return nil, fmt.Errorf("failed to finalize writable CAR: %w", err)
	}
	err = file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close file: %w", err)
	}

	err = carv2.ReplaceRootsInFile(
		filepath,
		[]cid.Cid{blockRootLink.(cidlink.Link).Cid}, // Use the epoch CID as the root CID.
	)
	if err != nil {
		return nil, fmt.Errorf("failed to replace roots in file: %w", err)
	}
	return blockRootLink, nil
}

const (
	KindTransaction = iota
	KindEntry
	KindBlock
)

func (cw *StageOne) constructBlock(
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

func (cw *StageOne) buildShredding(
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

func (cw *StageOne) onEntry(
	slotMeta blockstore.SlotMeta,
	entries []blockstore.Entries,
	metas []*confirmed_block.TransactionStatusMeta,
	onEntry func(cidOfARange datamodel.Link),
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

func (cw *StageOne) onTx(
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
