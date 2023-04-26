package createcar

import (
	"crypto/rand"
	"fmt"
	"os"
	"runtime"
	"sort"
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
	"golang.org/x/sync/errgroup"
)

type StageTwo struct {
	settingConcurrency int
	linkSystem         linking.LinkSystem
	linkPrototype      cidlink.LinkPrototype
}

func NewStageTwo() *StageTwo {
	lsys := cidlink.DefaultLinkSystem()
	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,                          // TODO: what is this?
			Codec:    uint64(multicodec.DagCbor), // See the multicodecs table: https://github.com/multiformats/multicodec/
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}

	return &StageTwo{
		linkSystem:    lsys,
		linkPrototype: lp,
	}
}

func (cw *StageTwo) SetConcurrency(concurrency int) {
	cw.settingConcurrency = concurrency
}

func (cw *StageTwo) Build(
	filepath string,
	epoch int64,
	schedule SlotRangeSchedule,
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

	epochRootLink, err := cw.constructEpoch(epoch, schedule)
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
		[]cid.Cid{epochRootLink.(cidlink.Link).Cid}, // Use the epoch CID as the root CID.
	)
	if err != nil {
		return nil, fmt.Errorf("failed to replace roots in file: %w", err)
	}
	return epochRootLink, nil
}

func (cw *StageTwo) constructEpoch(
	epoch int64,
	schedule SlotRangeSchedule,
) (datamodel.Link, error) {
	// - declare an Epoch object
	epochNode, err := qp.BuildMap(ipldbindcode.Prototypes.Epoch, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "kind", qp.Int(KindEpoch))
		qp.MapEntry(ma, "epoch", qp.Int(epoch))
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

type SlotToLink map[uint64]datamodel.Link

func (s2l SlotToLink) GetLinksSortedBySlot() []datamodel.Link {
	slots := make([]uint64, 0)
	for slot := range s2l {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i] < slots[j]
	})
	links := make([]datamodel.Link, 0)
	for _, slot := range slots {
		links = append(links, s2l[slot])
	}
	return links
}

func (cw *StageTwo) onSubset(schedule SlotRangeSchedule, out func(datamodel.Link)) error {
	for rangeIndex, slots := range schedule {
		start := slots[0]
		end := slots[len(slots)-1]
		fmt.Println("Range", rangeIndex, "from", start, "to", end)
		{
			slot2Link := make(SlotToLink)
			// - call onSlot() which will accumulate the CIDs of the Slots (of this Range) in slotLinks
			err := cw.onSlot(
				slots,
				func(slotNum uint64, cidOfASlot datamodel.Link) {
					slot2Link[slotNum] = cidOfASlot
				})
			if err != nil {
				return fmt.Errorf("error constructing slots for range %d: %w", rangeIndex, err)
			}

			// - sort the CIDs of the Slots (because they were processed in parallel)
			slotLinks := slot2Link.GetLinksSortedBySlot()

			// - declare a Subset object
			subsetNode, err := qp.BuildMap(ipldbindcode.Prototypes.Subset, -1, func(ma datamodel.MapAssembler) {
				qp.MapEntry(ma, "kind", qp.Int(KindSubset))
				qp.MapEntry(ma, "first", qp.Int(int64(start)))
				qp.MapEntry(ma, "last", qp.Int(int64(end)))
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

			// - store the Range object
			rangeLink, err := cw.Store(
				linking.LinkContext{},
				subsetNode.(schema.TypedNode).Representation(),
			)
			if err != nil {
				return err
			}
			// - call out(cidOfARange).
			out(rangeLink)
		}
	}
	return nil
}

func (cw *StageTwo) getSettingConcurrency() int {
	if cw.settingConcurrency > 0 {
		return cw.settingConcurrency
	}
	return runtime.NumCPU()
}

func (cw *StageTwo) onSlot(
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

func (cw *StageTwo) onBlock(
	slot uint64,
) (datamodel.Link, error) {
	info, pack, err := cw.readTemporaryBlockCAR(slot)
	if err != nil {
		return nil, fmt.Errorf("error reading package for slot %d: %w", info.Slot, err)
	}
	if info.Slot != slot {
		return nil, fmt.Errorf("error reading package for slot %d: got package for slot %d instead", slot, info.Slot)
	}
	_ = pack
	// TODO:
	// - read the temporary per-block CAR file
	// - read root CID of the CAR file (which is the CID of the Block object)
	// - copy all all objects from temporary CAR file to the final CAR file (this).
	// - return the root CID of the Block object.

	// buf := bytes.NewReader(pack)
	// // - split the file into fragments
	// chunkSize := int64(chunker.ChunkSizeLimit) - 400 // TODO: set this size considering the size of the other fields in the Fragment object.
	// ck := chunker.NewSizeSplitter(buf, chunkSize)

	// fragmentIndex := 0
	// for {
	// 	fragmentBytes, err := ck.NextBytes()
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			break
	// 		}
	// 		return fmt.Errorf("error reading next fragmen %d for slot %d: %w", fragmentIndex, info.Slot, err)
	// 	}

	// 	{
	// 		fragmentNode, err := qp.BuildMap(ipldbindcode.Prototypes.Fragment, -1, func(ma datamodel.MapAssembler) {
	// 			qp.MapEntry(ma, "Version", qp.Int(Version))
	// 			qp.MapEntry(ma, "Slot", qp.Int(int64(slot)))
	// 			qp.MapEntry(ma, "Index", qp.Int(int64(fragmentIndex)))
	// 			qp.MapEntry(ma, "Data", qp.Bytes(fragmentBytes))
	// 			// TODO: add info (???)
	// 		})
	// 		if err != nil {
	// 			return fmt.Errorf("error constructing fragment %d for slot %d: %w", fragmentIndex, info.Slot, err)
	// 		}
	// 		// debugNode(fragmentNode)

	// 		fragmentLink, err := cw.Store(
	// 			linking.LinkContext{},
	// 			fragmentNode.(schema.TypedNode).Representation(),
	// 		)
	// 		if err != nil {
	// 			return fmt.Errorf("error storing fragment %d for slot %d: %w", fragmentIndex, info.Slot, err)
	// 		}
	// 		out(fragmentLink)
	// 	}
	// 	fragmentIndex++
	// }
	return nil, err
}

func (cw *StageTwo) Store(lnkCtx linking.LinkContext, n datamodel.Node) (datamodel.Link, error) {
	return cw.linkSystem.Store(
		lnkCtx,
		cw.linkPrototype,
		n,
	)
}

type PackageInfo struct {
	Slot uint64
}

// readTemporaryBlockCAR returns a compressed file (which contains transactions and tx metadata for a given slot).
func (cw *StageTwo) readTemporaryBlockCAR(slot uint64) (PackageInfo, []byte, error) {
	// TODO: implement this function.
	randomPayload := make([]byte, KiB*200)
	_, err := rand.Read(randomPayload)
	if err != nil {
		return PackageInfo{}, nil, err
	}
	return PackageInfo{Slot: slot}, randomPayload, nil
	panic("not implemented yet")
}
