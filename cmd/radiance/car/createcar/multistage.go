package createcar

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	carv2 "github.com/ipfs/boxo/ipld/car/v2"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/printer"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/multiformats/go-multicodec"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/ipld/ipldbindcode"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/registry"
	radianceblockstore "go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/ipld/ipldgen"
	"go.firedancer.io/radiance/pkg/shred"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"
)

const (
	KindTransaction = iota
	KindEntry
	KindBlock
	KindSubset
	KindEpoch
)

type Multistage struct {
	settingConcurrency uint
	carFilepath        string

	linkSystem    linking.LinkSystem
	linkPrototype cidlink.LinkPrototype

	carFile    *os.File
	storageCar storage.WritableCar

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

		writableCar, err := storage.NewWritable(
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
		// cw.linkSystem.SetReadStorage(writableCar)
	}

	return cw, nil
}

func (cw *Multistage) Store(lnkCtx linking.LinkContext, node datamodel.Node) (datamodel.Link, error) {
	// TODO: is this safe to use concurrently?
	// cw.mu.Lock()
	// defer cw.mu.Unlock()

	return cw.linkSystem.Store(
		lnkCtx,
		cw.linkPrototype,
		node,
	)
}

func (cw *Multistage) finalizeCAR() error {
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

func (cw *Multistage) SetConcurrency(concurrency uint) {
	cw.settingConcurrency = concurrency
}

// OnBlock is called when a block is received.
// This fuction can be called concurrently.
// This function creates the subgraph for the block and writes the objects to the CAR file,
// and then registers the block CID.
func (cw *Multistage) OnBlock(
	slotMeta *radianceblockstore.SlotMeta,
	entries [][]shred.Entry,
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
	slotMeta *radianceblockstore.SlotMeta,
	entries [][]shred.Entry,
	metas []*confirmed_block.TransactionStatusMeta,
) (datamodel.Link, error) {
	shredding, err := cw.buildShredding(slotMeta, entries)
	if err != nil {
		return nil, fmt.Errorf("failed to build shredding: %w", err)
	}
	entryLinks := make([]datamodel.Link, 0)
	err = cw.onEntry(
		slotMeta,
		entries,
		metas,
		func(cidOfAnEntry datamodel.Link) {
			entryLinks = append(entryLinks, cidOfAnEntry)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to process entries: %w", err)
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
				for _, entryLink := range entryLinks {
					qp.ListEntry(la, qp.Link(entryLink))
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
	slotMeta *radianceblockstore.SlotMeta,
	entries [][]shred.Entry,
) ([]ipldbindcode.Shredding, error) {
	entryNum := 0
	txNum := 0
	out := make([]ipldbindcode.Shredding, 0)
	for i, batch := range entries {
		for j, entry := range batch {

			pos := ipldgen.EntryPos{
				Slot:       slotMeta.Slot,
				EntryIndex: entryNum,
				Batch:      i,
				BatchIndex: j,
				LastShred:  -1,
			}
			if j == len(batch)-1 {
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
	slotMeta *radianceblockstore.SlotMeta,
	entries [][]shred.Entry,
	metas []*confirmed_block.TransactionStatusMeta,
	onEntry func(cidOfAnEntry datamodel.Link),
) error {
	entryNum := 0
	txNum := 0
	for _, batch := range entries {
		for _, entry := range batch {

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
	slotMeta *radianceblockstore.SlotMeta,
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

// FinalizeDAG constructs the DAG for the given epoch and replaces the root of
// the CAR file with the root of the DAG (the Epoch object CID).
func (cw *Multistage) FinalizeDAG(
	epoch uint64,
) (datamodel.Link, error) {
	allRegistered, err := cw.reg.GetAll()
	if err != nil {
		return nil, fmt.Errorf("failed to get all links: %w", err)
	}
	allSlots := make([]uint64, 0, len(allRegistered))
	for _, slot := range allRegistered {
		if slot.CID == nil || len(slot.CID) == 0 || slot.Slot == 0 || !slot.Status.Is(registry.SlotStatusIncluded) {
			continue
		}
		allSlots = append(allSlots, slot.Slot)
	}
	klog.Infof("Got list of %d slots", len(allSlots))

	numSlotsPerSubset := 432000 / 18 // TODO: make this configurable

	schedule := SplitSlotsIntoRanges(numSlotsPerSubset, allSlots)

	klog.Infof("Completing DAG for epoch %d...", epoch)
	epochRootLink, err := cw.constructEpoch(epoch, schedule)
	if err != nil {
		return nil, fmt.Errorf("failed to construct epoch: %w", err)
	}
	klog.Infof("Completed DAG for epoch %d", epoch)

	klog.Infof("Finalizing CAR for epoch %d...", epoch)
	err = cw.finalizeCAR()
	if err != nil {
		return nil, fmt.Errorf("failed to finalize writable CAR: %w", err)
	}
	klog.Infof("Finalized CAR for epoch %d; closing...", epoch)
	err = cw.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close file: %w", err)
	}
	klog.Infof("Closed CAR for epoch %d", epoch)

	klog.Infof("Replacing root in CAR with CID of epoch %d", epoch)
	err = cw.replaceRoot(epochRootLink.(cidlink.Link).Cid)
	if err != nil {
		return nil, fmt.Errorf("failed to replace roots in file: %w", err)
	}
	klog.Infof("Replaced root in CAR with CID of epoch %d", epoch)

	cw.reg.Destroy()
	return epochRootLink, nil
}

func (cw *Multistage) constructEpoch(
	epoch uint64,
	schedule SlotRangeSchedule,
) (datamodel.Link, error) {
	// - declare an Epoch object
	epochNode, err := qp.BuildMap(ipldbindcode.Prototypes.Epoch, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "kind", qp.Int(KindEpoch))
		qp.MapEntry(ma, "epoch", qp.Int(int64(epoch)))
		qp.MapEntry(ma, "subsets",
			qp.List(-1, func(la datamodel.ListAssembler) {
				// - call onSubset() which will write the CIDs of the Subsets to the Epoch object
				cw.onSubset(
					schedule,
					func(cidOfASubset datamodel.Link) {
						qp.ListEntry(la,
							qp.Link(cidOfASubset),
						)
					},
				)
			}),
		)
	})
	if err != nil {
		return nil, err
	}
	printer.Print(epochNode)

	// - store the Epoch object to storage
	epochLink, err := cw.Store(
		linking.LinkContext{},
		epochNode.(schema.TypedNode).Representation(),
	)
	if err != nil {
		return nil, err
	}
	return epochLink, nil
}

func (cw *Multistage) onSubset(schedule SlotRangeSchedule, out func(datamodel.Link)) error {
	for subsetIndex, slots := range schedule {
		first := slots[0]
		last := slots[len(slots)-1]
		klog.Infof("Subset %d: first %d, last %d", subsetIndex, first, last)
		{
			slot2Link := make(SlotToLink)
			// - call onSlot() which will accumulate the CIDs of the Slots (of this Range) in slotLinks
			err := cw.onSlot(
				slots,
				func(slotNum uint64, cidOfASlot datamodel.Link) {
					slot2Link[slotNum] = cidOfASlot
				})
			if err != nil {
				return fmt.Errorf("error constructing slots for range %d: %w", subsetIndex, err)
			}

			// - sort the CIDs of the Slots (because they were processed in parallel)
			slotLinks := slot2Link.GetLinksSortedBySlot()

			// - declare a Subset object
			subsetNode, err := qp.BuildMap(ipldbindcode.Prototypes.Subset, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "kind", qp.Int(KindSubset))
				qp.MapEntry(ma, "first", qp.Int(int64(first)))
				qp.MapEntry(ma, "last", qp.Int(int64(last)))
				qp.MapEntry(ma, "blocks",
					qp.List(-1, func(la datamodel.ListAssembler) {
						for _, slotLink := range slotLinks {
							qp.ListEntry(la,
								qp.Link(slotLink),
							)
						}
					}),
				)
			})
			if err != nil {
				return err
			}
			debugPrintNode(subsetNode)

			// - store the Subset object
			subsetLink, err := cw.Store(
				linking.LinkContext{},
				subsetNode.(schema.TypedNode).Representation(),
			)
			if err != nil {
				return err
			}
			// - call out(subsetLink).
			out(subsetLink)
		}
	}
	return nil
}

func (cw *Multistage) getSettingConcurrency() int {
	if cw.settingConcurrency > 0 {
		return int(cw.settingConcurrency)
	}
	return runtime.NumCPU()
}

func (cw *Multistage) onSlot(
	mySlots []uint64,
	out func(uint64, datamodel.Link),
) error {
	// The responsibility of sorting the CIDs of the Slots is left to the caller of this function.
	// The use of concurrency makes the generated CAR file non-deterministic (the fragments/slots are written to it in a non-deterministic order).
	// BUT the root CID of the CAR file is
	// deterministic, as it is the root CID of a merkle dag.
	// What needs to to be assured is that in any order the slots are processed, they are always sorted afterwards.
	mu := sync.Mutex{}
	wg := new(errgroup.Group)

	// Process slots in parallel.
	concurrency := cw.getSettingConcurrency()
	wg.SetLimit(concurrency)

	for slotI := range mySlots {
		slot := mySlots[slotI]
		wg.Go(func() error {
			blockLink, err := cw.onBlock(
				slot,
			)
			if err != nil {
				return fmt.Errorf("error constructing pieces for slot %d: %w", slot, err)
			}

			mu.Lock()
			out(slot, blockLink)
			mu.Unlock()
			return nil
		})
	}
	return wg.Wait()
}

func (cw *Multistage) onBlock(
	slot uint64,
) (datamodel.Link, error) {
	c, err := cw.reg.GetCID(slot)
	if err != nil {
		return nil, err
	}
	// return the CID as a link
	return cidlink.Link{Cid: *c}, nil
}
