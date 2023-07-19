package createcar

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/printer"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/klauspost/compress/zstd"
	"github.com/multiformats/go-multicodec"
	"github.com/rpcpool/yellowstone-faithful/ipld/ipldbindcode"
	"github.com/rpcpool/yellowstone-faithful/iplddecoders"
	concurrently "github.com/tejzpr/ordered-concurrently/v3"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/registry"
	"go.firedancer.io/radiance/pkg/blockstore"
	radianceblockstore "go.firedancer.io/radiance/pkg/blockstore"
	firecar "go.firedancer.io/radiance/pkg/ipld/car"
	"go.firedancer.io/radiance/pkg/shred"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type blockWorker struct {
	slotMeta  *radianceblockstore.SlotMeta
	CIDSetter func(slot uint64, cid []byte) error
	walker    *radianceblockstore.BlockWalk
	done      func(numTx uint64)
}

func newBlockWorker(
	slotMeta *radianceblockstore.SlotMeta,
	CIDSetter func(slot uint64, cid []byte) error,
	walker *radianceblockstore.BlockWalk,
	done func(uint64),
) *blockWorker {
	return &blockWorker{
		slotMeta:  slotMeta,
		CIDSetter: CIDSetter,
		walker:    walker,
		done:      done,
	}
}

func BlocktimeKey(slot uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key[0:8], slot)
	return key
}

// The input type should implement the WorkFunction interface
func (w blockWorker) Run(ctx context.Context) interface{} {
	var numTx uint64
	defer func() {
		w.done(numTx)
	}()
	slot := w.slotMeta.Slot
	entries, err := w.walker.Entries(w.slotMeta)
	if err != nil {
		return err
	}

	transactionMetaKeys, err := transactionMetaKeysFromEntries(slot, entries)
	if err != nil {
		return err
	}
	numTx = uint64(len(transactionMetaKeys))

	metas, err := w.walker.TransactionMetas(transactionMetaKeys...)
	if err != nil {
		return fmt.Errorf("failed to get transaction metas for slot %d: %w", slot, err)
	}

	blockTime, err := w.walker.BlockTime(slot)
	if err != nil {
		return fmt.Errorf("failed to get block time for slot %d: %w", slot, err)
	}

	blockHeight, err := w.walker.BlockHeight(slot)
	if err != nil {
		return fmt.Errorf("failed to get block height for slot %d: %w", slot, err)
	}

	rewards, err := w.walker.Rewards(slot)
	if err != nil {
		return fmt.Errorf("failed to get rewards for slot %d: %w", slot, err)
	}
	// if true {
	// 	rewards = getRandomBytes(10 * MiB)
	// }

	// subgraphStore will contain the subgraph for the block
	subgraphStore := newMemoryBlockstore(w.slotMeta.Slot)
	// - [x] construct the block
	blockRootLink, err := constructBlock(
		subgraphStore,
		w.slotMeta,
		blockTime,
		blockHeight,
		entries,
		metas,
		rewards,
	)
	if err != nil {
		return fmt.Errorf("failed to construct block: %w", err)
	}
	// - [x] register the block CID
	err = w.CIDSetter(w.slotMeta.Slot, blockRootLink.(cidlink.Link).Cid.Bytes())
	if err != nil {
		return fmt.Errorf("failed to register block CID: %w", err)
	}
	// - [x] return the subgraph
	return subgraphStore
}

type memSubtreeStore struct {
	slot   uint64
	blocks []firecar.Block
}

func newMemoryBlockstore(slot uint64) *memSubtreeStore {
	return &memSubtreeStore{
		slot:   slot,
		blocks: make([]firecar.Block, 0),
	}
}

func (ms *memSubtreeStore) Store(node datamodel.Node) (datamodel.Link, error) {
	block, err := firecar.NewBlockFromCBOR(node, uint64(multicodec.DagCbor))
	if err != nil {
		return nil, err
	}
	ms.pushBlock(block)
	return cidlink.Link{Cid: block.Cid}, nil
}

func (ms *memSubtreeStore) pushBlock(block firecar.Block) {
	ms.blocks = append(ms.blocks, block)
}

type Multistage struct {
	settingConcurrency uint
	carFilepath        string

	storageCar *carHandle

	reg *registry.Registry

	workerInputChan     chan concurrently.WorkFunction
	waitExecuted        *sync.WaitGroup
	waitResultsReceived sync.WaitGroup
	numReceivedAtomic   *atomic.Int64
	numTxAtomic         *atomic.Uint64
	walker              *radianceblockstore.BlockWalk
	numWrittenObjects   *atomic.Uint64
}

// - [x] in stage 1, we create a CAR file for all the slots
// - there's a registry of all slots that have been written, and their CIDs (very important)
// - [x] in stage 2, we add the missing parts of the DAG (same CAR file).
func NewMultistage(
	finalCARFilepath string,
	numWorkers uint,
	walker *radianceblockstore.BlockWalk,
) (*Multistage, error) {
	if numWorkers == 0 {
		numWorkers = uint(runtime.NumCPU())
	}
	cw := &Multistage{
		carFilepath:       finalCARFilepath,
		workerInputChan:   make(chan concurrently.WorkFunction, numWorkers),
		waitExecuted:      new(sync.WaitGroup), // used to wait for results
		walker:            walker,
		numReceivedAtomic: new(atomic.Int64),
		numTxAtomic:       new(atomic.Uint64),
		numWrittenObjects: new(atomic.Uint64),
	}

	cw.walker.SetOnBeforePop(func() error {
		// wait for the current batch processing to finish before closing the DB and opening the next one
		cw.waitExecuted.Wait()
		// reset the group
		cw.waitExecuted = new(sync.WaitGroup)
		return nil
	})

	outputChan := concurrently.Process(
		context.Background(),
		cw.workerInputChan,
		&concurrently.Options{PoolSize: int(numWorkers), OutChannelBuffer: int(numWorkers)},
	)
	go func(ms *Multistage) {
		latestSlot := uint64(0)
		// process the results from the workers
		for result := range outputChan {
			switch resValue := result.Value.(type) {
			case error:
				panic(resValue)
			case *memSubtreeStore:
				subtree := resValue
				if subtree.slot > latestSlot || subtree.slot == 0 {
					latestSlot = subtree.slot
				} else if subtree.slot < latestSlot {
					panic(fmt.Errorf("slot %d is out of order (latest slot: %d)", subtree.slot, latestSlot))
				} else {
					// subtree.slot == latestSlot
					panic(fmt.Errorf("slot %d is already processed", subtree.slot))
				}
				// write the blocks to the CAR file:
				for blockIndex, block := range subtree.blocks {
					err := cw.storageCar.WriteBlock(block)
					if err != nil {
						panic(fmt.Errorf("failed to write node #%d for slot %d: %w", blockIndex, subtree.slot, err))
					}
				}
				ms.waitResultsReceived.Done()
				ms.numReceivedAtomic.Add(-1)
			default:
				panic(fmt.Errorf("unexpected result type: %T", result.Value))
			}
		}
	}(cw)
	{
		exists, err := fileExists(finalCARFilepath)
		if err != nil {
			return nil, err
		}
		if exists {
			return nil, fmt.Errorf("file %q already exists", finalCARFilepath)
		}

		{
			registryFilepath := filepath.Join(finalCARFilepath + ".registry.bin")
			cidLen := 36
			reg, err := registry.New(registryFilepath, cidLen)
			if err != nil {
				return nil, fmt.Errorf("failed to create registry: %w", err)
			}
			cw.reg = reg
		}

		writableCar := new(carHandle)
		err = writableCar.open(finalCARFilepath, cw.numWrittenObjects)
		if err != nil {
			return nil, fmt.Errorf("failed to create writable CAR: %w", err)
		}
		cw.storageCar = writableCar
	}

	return cw, nil
}

// Store is used to store a node in the CAR file.
func (cw *Multistage) Store(node datamodel.Node) (datamodel.Link, error) {
	block, err := firecar.NewBlockFromCBOR(node, uint64(multicodec.DagCbor))
	if err != nil {
		return nil, err
	}
	err = cw.storageCar.WriteBlock(block)
	if err != nil {
		return nil, err
	}
	return cidlink.Link{Cid: block.Cid}, nil
}

func (cw *Multistage) Close() error {
	return cw.storageCar.close()
}

// replaceRoot is used to replace the root CID in the CAR file.
// This is done when we have the epoch root CID.
func (cw *Multistage) replaceRoot(newRoot cid.Cid) error {
	return car.ReplaceRootsInFile(
		cw.carFilepath,
		[]cid.Cid{newRoot},
	)
}

func (cw *Multistage) getConcurrency() int {
	if cw.settingConcurrency > 0 {
		return int(cw.settingConcurrency)
	}
	return runtime.NumCPU()
}

// OnSlotFromDB is called when a block is received.
// This MUST be called in order of the slot number.
func (cw *Multistage) OnSlotFromDB(
	slotMeta *radianceblockstore.SlotMeta,
) error {
	cw.waitExecuted.Add(1)
	cw.waitResultsReceived.Add(1)
	cw.numReceivedAtomic.Add(1)
	cw.workerInputChan <- newBlockWorker(
		slotMeta,
		cw.reg.SetCID,
		cw.walker,
		func(numTx uint64) {
			cw.numTxAtomic.Add(numTx)
			cw.waitExecuted.Done()
		},
	)
	return nil
}

func getEmptyCIDLink() datamodel.Link {
	return cidlink.Link{Cid: firecar.DummyCID}
}

func getRandomBytes(l int) []byte {
	b := make([]byte, l)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func constructBlock(
	ms *memSubtreeStore,
	slotMeta *radianceblockstore.SlotMeta,
	blockTime uint64,
	blockHeight *uint64,
	entries [][]shred.Entry,
	metas []*blockstore.TransactionStatusMetaWithRaw,
	rewardsBlob []byte,
) (datamodel.Link, error) {
	shredding, err := buildShredding(slotMeta, entries)
	if err != nil {
		return nil, fmt.Errorf("failed to build shredding: %w", err)
	}
	entryLinks := make([]datamodel.Link, 0)
	err = onEntry(
		ms,
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

	var rewardsNodeLink datamodel.Link

	if len(rewardsBlob) > 0 {
		rewardsFirstFrame, err := CreateAndStoreFrames(
			ms.Store,
			CompressZstd(rewardsBlob),
			(MaxObjectSize - 263 - 300),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create rewards frames: %w", err)
		}

		// add the rewards entry
		rewardsNode, err := qp.BuildMap(ipldbindcode.Prototypes.Rewards, -1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindRewards)))
			qp.MapEntry(ma, "slot", qp.Int(int64(slotMeta.Slot)))
			qp.MapEntry(ma, "data", qp.Map(-1, frameToDatamodelNodeAssembler(rewardsFirstFrame)))
		})
		if err != nil {
			return nil, fmt.Errorf("failed to build reward node: %w", err)
		}
		rewardsLink, err := ms.Store(
			rewardsNode.(schema.TypedNode).Representation(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to store rewards node: %w", err)
		}
		rewardsNodeLink = rewardsLink
	} else {
		rewardsNodeLink = getEmptyCIDLink()
	}

	blockNode, err := qp.BuildMap(ipldbindcode.Prototypes.Block, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindBlock)))
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
		qp.MapEntry(ma, "meta",
			qp.Map(-1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "parent_slot", qp.Int(int64(slotMeta.ParentSlot)))
				qp.MapEntry(ma, "blocktime", qp.Int(int64(blockTime)))
				if blockHeight == nil {
					qp.MapEntry(ma, "block_height", qp.Null())
				} else {
					qp.MapEntry(ma, "block_height", qp.Int(int64(*blockHeight)))
				}
			}),
		)
		qp.MapEntry(ma, "rewards", qp.Link(rewardsNodeLink))
	})
	if err != nil {
		return nil, err
	}
	// printer.Print(blockNode)

	// - store the Block object to storage
	blockLink, err := ms.Store(
		blockNode.(schema.TypedNode).Representation(),
	)
	if err != nil {
		return nil, err
	}
	return blockLink, nil
}

type EntryPos struct {
	Slot       uint64
	EntryIndex int
	Batch      int
	BatchIndex int
	LastShred  int
}

func buildShredding(
	slotMeta *radianceblockstore.SlotMeta,
	entries [][]shred.Entry,
) ([]ipldbindcode.Shredding, error) {
	entryNum := 0
	txNum := 0
	out := make([]ipldbindcode.Shredding, 0)
	for i, batch := range entries {
		for j, entry := range batch {

			pos := EntryPos{
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

func onEntry(
	ms *memSubtreeStore,
	slotMeta *radianceblockstore.SlotMeta,
	entries [][]shred.Entry,
	metas []*blockstore.TransactionStatusMetaWithRaw,
	onEntry func(cidOfAnEntry datamodel.Link),
) error {
	entryNum := 0
	txNum := 0
	for _, batch := range entries {
		for _, entry := range batch {

			transactionMetas := metas[txNum : txNum+len(entry.Txns)]

			// - construct a Entry object
			entryNode, err := qp.BuildMap(ipldbindcode.Prototypes.Entry, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindEntry)))
				qp.MapEntry(ma, "numHashes", qp.Int(int64(entry.NumHashes)))
				qp.MapEntry(ma, "hash", qp.Bytes(entry.Hash[:]))

				qp.MapEntry(ma, "transactions",
					qp.List(-1, func(la datamodel.ListAssembler) {
						// - call onTx() which will write the CIDs of the Transactions to the Entry object
						onTx(
							ms,
							slotMeta,
							entry,
							transactionMetas,
							func(cidOfATx datamodel.Link) {
								qp.ListEntry(la,
									qp.Link(cidOfATx),
								)
							},
							txNum,
						)
					}),
				)
			})
			if err != nil {
				return fmt.Errorf("failed to build Entry %d: %w", entryNum, err)
			}
			// printer.Print(entryNode)

			// - store the Entry object to storage
			entryLink, err := ms.Store(
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

func onTx(
	ms *memSubtreeStore,
	slotMeta *radianceblockstore.SlotMeta,
	entry shred.Entry,
	metas []*blockstore.TransactionStatusMetaWithRaw,
	callback func(cidOfATx datamodel.Link),
	numTxBeforeThisEntry int,
) error {
	for txIndexEntry, transaction := range entry.Txns {
		firstSig := transaction.Signatures[0]

		txData, err := transaction.MarshalBinary()
		if err != nil {
			return fmt.Errorf("failed to marshal transaction %s: %w", firstSig, err)
		}
		meta := metas[txIndexEntry]
		txMeta := meta.Raw

		// if true && !gotOneRandomBigTx {
		// 	txData = getRandomBytes(3 * MiB)
		// 	txMeta = getRandomBytes(3 * MiB)
		// 	gotOneRandomBigTx = true
		// }

		firstFrameSize := (MaxObjectSize - 100 - 300) / 2

		txDataFirstFrame, err := CreateAndStoreFrames(
			ms.Store,
			txData,
			firstFrameSize,
		)
		if err != nil {
			return fmt.Errorf("failed to create frames for data of transaction %s: %w", firstSig, err)
		}

		txMetaFirstFrame, err := CreateAndStoreFrames(
			ms.Store,
			CompressZstd(txMeta),
			firstFrameSize,
		)
		if err != nil {
			return fmt.Errorf("failed to create frames for metadata of transaction %s: %w", firstSig, err)
		}

		// - construct a Transaction object
		transactionNode, err := qp.BuildMap(ipldbindcode.Prototypes.Transaction, -1, func(ma datamodel.MapAssembler) {
			qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindTransaction)))
			qp.MapEntry(ma, "data", qp.Map(-1, frameToDatamodelNodeAssembler(txDataFirstFrame)))
			qp.MapEntry(ma, "metadata", qp.Map(-1, frameToDatamodelNodeAssembler(txMetaFirstFrame)))
			qp.MapEntry(ma, "slot", qp.Int(int64(slotMeta.Slot)))
			qp.MapEntry(ma, "index", qp.Int(int64(numTxBeforeThisEntry+txIndexEntry)))
		})
		if err != nil {
			return fmt.Errorf("failed to construct Transaction %s: %w", firstSig, err)
		}
		// printer.Print(transactionNode)

		// - store the Transaction object to storage
		txLink, err := ms.Store(
			transactionNode.(schema.TypedNode).Representation(),
		)
		if err != nil {
			return fmt.Errorf("failed to store Transaction %s: %w", firstSig, err)
		}
		callback(txLink)
	}
	return nil
}

var encoder, _ = zstd.NewWriter(nil,
	zstd.WithEncoderLevel(zstd.SpeedFastest),
)

func CompressZstd(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

type MultistageRecap struct {
	TotalSlots             uint64
	FirstSlot              uint64
	LastSlot               uint64
	NumberOfWrittenObjects uint64
}

// FinalizeDAG constructs the DAG for the given epoch and replaces the root of
// the CAR file with the root of the DAG (the Epoch object CID).
func (cw *Multistage) FinalizeDAG(
	epoch uint64,
) (datamodel.Link, *MultistageRecap, error) {
	{
		// wait for all slots to be registered
		klog.Infof("Waiting for all slots to be registered...")
		cw.waitExecuted.Wait()
		klog.Infof("All slots registered")

		klog.Infof("Wait to receive all results...")
		close(cw.workerInputChan)
		cw.waitResultsReceived.Wait()
		klog.Infof("All results received")
	}
	allRegistered, err := cw.reg.GetAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get all links: %w", err)
	}
	allSlots := make([]uint64, 0, len(allRegistered))
	for _, slot := range allRegistered {
		if slot.CID == nil || len(slot.CID) == 0 || !slot.Status.Is(registry.SlotStatusIncluded) {
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
		return nil, nil, fmt.Errorf("failed to construct epoch: %w", err)
	}
	klog.Infof("Completed DAG for epoch %d", epoch)

	slotRecap := &MultistageRecap{
		TotalSlots:             uint64(len(allSlots)),
		FirstSlot:              allSlots[0],
		LastSlot:               allSlots[len(allSlots)-1],
		NumberOfWrittenObjects: cw.NumberOfWrittenObjects(),
	}

	klog.Infof("Closing CAR...")
	err = cw.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to close file: %w", err)
	}
	klog.Infof("Closed CAR for epoch %d", epoch)

	klog.Infof("CAR contains %d transactions", cw.numTxAtomic.Load())
	klog.Infof("CAR contains %d objects", cw.NumberOfWrittenObjects())

	{
		epochCid := epochRootLink.(cidlink.Link).Cid
		klog.Infof("Replacing root in CAR with CID of epoch %d", epoch)
		err = cw.replaceRoot(epochCid)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to replace roots in file: %w", err)
		}
		klog.Infof("Replaced root in CAR with CID of epoch %d", epoch)
	}

	cw.reg.Destroy()
	return epochRootLink, slotRecap, err
}

func (cw *Multistage) NumberOfTransactions() uint64 {
	return cw.numTxAtomic.Load()
}

func (cw *Multistage) NumberOfWrittenObjects() uint64 {
	return cw.numWrittenObjects.Load()
}

func (cw *Multistage) constructEpoch(
	epoch uint64,
	schedule SlotRangeSchedule,
) (datamodel.Link, error) {
	// - declare an Epoch object
	epochNode, err := qp.BuildMap(ipldbindcode.Prototypes.Epoch, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindEpoch)))
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
	time.Sleep(1 * time.Second)
	printer.Print(epochNode)

	// - store the Epoch object to storage
	epochLink, err := cw.Store(
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
				qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindSubset)))
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
	concurrency := cw.getConcurrency()
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
