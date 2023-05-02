// Package cargen transforms blockstores into CAR files.
package createcar

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/vbauerster/mpb/v8"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/shred"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
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
	walk  blockstore.BlockWalkI
	epoch uint64
	stop  uint64 // exclusive

	callback func(slotMeta *blockstore.SlotMeta, entries [][]shred.Entry, txMetas []*confirmed_block.TransactionStatusMeta) error

	totalSlotsToProcess uint64
	bar                 *mpb.Bar
	numTxns             *atomic.Uint64
}

// uint64RangesHavePartialOverlapIncludingEdges returns true if the two ranges have any overlap.
func uint64RangesHavePartialOverlapIncludingEdges(r1 [2]uint64, r2 [2]uint64) bool {
	if r1[0] < r2[0] {
		return r1[1] >= r2[0]
	} else {
		return r2[1] >= r1[0]
	}
}

func NewIterator(
	epoch uint64,
	walk blockstore.BlockWalkI,
	requireFullEpoch bool,
	limitSlots uint64,
	callback func(slotMeta *blockstore.SlotMeta, entries [][]shred.Entry, txMetas []*confirmed_block.TransactionStatusMeta) error,
) (*Worker, error) {
	if callback == nil {
		return nil, fmt.Errorf("callback must be provided")
	}
	if epoch == 0 {
		return nil, fmt.Errorf("epoch must be > 0")
	}

	// Seek to epoch start and make sure we have all data
	const epochLen = 432000
	officialEpochStart := epoch * epochLen
	officialEpochStop := officialEpochStart + epochLen
	if requireFullEpoch && !walk.Seek(officialEpochStart) {
		return nil, fmt.Errorf("slot %d not available in any DB", officialEpochStart)
	}

	slotsAvailable := walk.SlotsAvailable()
	if requireFullEpoch && slotsAvailable < epochLen {
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

	if !uint64RangesHavePartialOverlapIncludingEdges(
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
	entries, err := w.walk.Entries(meta)
	if err != nil {
		return false, fmt.Errorf("failed to get entry at slot %d: %w", meta.Slot, err)
	}
	if err := w.processSlot(meta, entries); err != nil {
		return false, err
	}
	return true, nil
}

// processSlot writes a filled Solana slot to the CAR.
// Creates multiple IPLD blocks internally.
func (w *Worker) processSlot(meta *blockstore.SlotMeta, entries [][]shred.Entry) error {
	slot := meta.Slot

	transactionMetaKeys, err := transactionMetaKeysFromEntries(slot, entries)
	if err != nil {
		return err
	}

	txMetas, err := w.walk.TransactionMetas(transactionMetaKeys...)
	if err != nil {
		return fmt.Errorf("failed to get transaction metas for slot %d: %w", slot, err)
	}

	klog.V(3).Infof("Slot %d", slot)
	return w.callback(meta, entries, txMetas)
}

func transactionMetaKeysFromEntries(slot uint64, entries [][]shred.Entry) ([][]byte, error) {
	keys := make([][]byte, 0)
	for _, batch := range entries {
		for _, entry := range batch {
			for _, tx := range entry.Txns {
				firstSig := tx.Signatures[0]
				keys = append(keys, blockstore.FormatTxMetadataKey(slot, firstSig))
			}
		}
	}
	return keys, nil
}
