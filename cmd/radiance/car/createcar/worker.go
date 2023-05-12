// Package cargen transforms blockstores into CAR files.
package createcar

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/vbauerster/mpb/v8"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/shred"
	"k8s.io/klog/v2"
)

// MaxCARSize is the maximum size of a CARv1 file.
//
// Dictated by Filecoin's preferred sector size (currently 32 GiB).
// cargen will attempt to pack CARs as large as possible but never exceed.
//
// Filecoin miners may append CARv2 indexes, which would exceed the total CAR size.
const MaxCARSize = 1 << 35

type Worker struct {
	walk  blockstore.BlockWalker
	epoch uint64
	stop  uint64 // exclusive

	callback Callback

	totalSlotsToProcess uint64
	bar                 *mpb.Bar
	numTxns             *atomic.Uint64
}

type Callback func(slotMeta *blockstore.SlotMeta, latestCarIndex int) error

// Uint64RangesHavePartialOverlapIncludingEdges returns true if the two ranges have any overlap.
func Uint64RangesHavePartialOverlapIncludingEdges(r1 [2]uint64, r2 [2]uint64) bool {
	if r1[0] < r2[0] {
		return r1[1] >= r2[0]
	} else {
		return r2[1] >= r1[0]
	}
}

const EpochLen = 432000

func CalcEpochLimits(epoch uint64) (uint64, uint64) {
	epochStart := epoch * EpochLen
	epochStop := epochStart + EpochLen - 1
	return epochStart, epochStop
}

func NewIterator(
	epoch uint64,
	walk blockstore.BlockWalker,
	requireFullEpoch bool,
	limitSlots uint64,
	callback Callback,
) (*Worker, error) {
	if callback == nil {
		return nil, fmt.Errorf("callback must be provided")
	}
	if epoch == 0 {
		klog.Warningf("Epoch is set to 0; please be sure this is what you want")
	}

	// Seek to epoch start and make sure we have all data
	officialEpochStart, officialEpochStop := CalcEpochLimits(epoch)
	if requireFullEpoch && !walk.Seek(officialEpochStart) {
		return nil, fmt.Errorf("slot %d not available in any DB", officialEpochStart)
	}

	slotsAvailable := walk.SlotsAvailable()
	if requireFullEpoch && slotsAvailable < EpochLen {
		return nil, fmt.Errorf("need slots [%d:%d] (epoch %d) but only have up to %d",
			officialEpochStart, officialEpochStop, epoch, officialEpochStart+slotsAvailable)
	}

	haveStart, haveStop := walk.SlotEdges()

	var stopAt uint64
	var totalSlotsToProcess uint64
	if !requireFullEpoch {
		totalSlotsToProcess = haveStop - haveStart
		stopAt = haveStop
		klog.Infof(
			"Not requiring full epoch; will process available slots [%d:%d] (~%d slots)",
			haveStart, haveStop,
			// NOTE: there might be gaps in the data (as we are considering the min/max of the provided DBs),
			// so this is not a reliable estimate.
			haveStop-haveStart,
		)
	} else {
		totalSlotsToProcess = officialEpochStop - officialEpochStart
		stopAt = officialEpochStop
		klog.Infof(
			"Will process slots only in the %d epoch range [%d:%d] (discarding slots outside)",
			epoch, officialEpochStart, officialEpochStop,
		)
	}

	if limitSlots > 0 && limitSlots < totalSlotsToProcess {
		totalSlotsToProcess = limitSlots
		stopAt = haveStart + limitSlots
		klog.Infof(
			"Limiting slots to %d (discarding slots after %d)",
			limitSlots, stopAt,
		)
	}

	if !Uint64RangesHavePartialOverlapIncludingEdges(
		[2]uint64{officialEpochStart, officialEpochStop},
		[2]uint64{haveStart, haveStop},
	) {
		return nil, fmt.Errorf("no overlap between requested epoch [%d:%d] and available slots [%d:%d]",
			officialEpochStart, officialEpochStop,
			haveStart, haveStop)
	}

	w := &Worker{
		walk:                walk,
		epoch:               epoch,
		stop:                stopAt,
		totalSlotsToProcess: totalSlotsToProcess,
		callback:            callback,
	}

	return w, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.initStatsTracker(ctx); err != nil {
		return err
	}
	defer w.bar.Abort(false)
	for ctx.Err() == nil {
		w.bar.Increment()
		next, err := w.step()
		if err != nil {
			return err
		}
		if !next {
			break
		}
	}
	return ctx.Err()
}

// step iterates one block forward.
func (w *Worker) step() (next bool, err error) {
	meta, ok := w.walk.Next()
	if !ok {
		return false, nil
	}
	if meta.Slot > w.stop {
		return false, nil
	}
	latestDBIndex := w.walk.DBIndex()
	if err := w.processSlot(meta, latestDBIndex); err != nil {
		return false, err
	}
	return true, nil
}

func (w *Worker) processSlot(meta *blockstore.SlotMeta, latestCarIndex int) error {
	slot := meta.Slot
	klog.V(3).Infof("Slot %d", slot)
	return w.callback(meta, latestCarIndex)
}

func transactionMetaKeysFromEntries(slot uint64, entries [][]shred.Entry) ([][]byte, error) {
	ln := 0
	for _, batch := range entries {
		for _, entry := range batch {
			ln += len(entry.Txns)
		}
	}
	keys := make([][]byte, ln)
	index := 0
	for _, batch := range entries {
		for _, entry := range batch {
			for _, tx := range entry.Txns {
				firstSig := tx.Signatures[0]
				keys[index] = blockstore.FormatTxMetadataKey(slot, firstSig)
				index++
			}
		}
	}
	return keys, nil
}
