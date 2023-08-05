package blockstore

import (
	"sort"

	"go.firedancer.io/radiance/pkg/shred"
)

type WalkHandle struct {
	DB    *DB
	Start uint64
	Stop  uint64 // inclusive

	shredRevision                   int
	nextShredRevisionActivationSlot *uint64
}

// sortWalkHandles detects bounds of each DB and sorts handles.
func sortWalkHandles(h []*WalkHandle, shredRevision int, nextRevisionActivationSlot *uint64) error {
	for i, db := range h {
		// Find lowest and highest available slot in DB.
		start, err := getLowestCompletedSlot(db.DB, shredRevision, nextRevisionActivationSlot)
		if err != nil {
			return err
		}
		stop, err := db.DB.MaxRoot()
		if err != nil {
			return err
		}
		h[i] = &WalkHandle{
			Start: start,
			Stop:  stop,
			DB:    db.DB,
		}
	}
	sort.Slice(h, func(i, j int) bool {
		return h[i].Start < h[j].Start
	})
	return nil
}

func (wh *WalkHandle) Entries(meta *SlotMeta) ([][]shred.Entry, error) {
	// TODO: handle concurrent calls to Entries() on the same WalkHandle.
	if wh.nextShredRevisionActivationSlot != nil && meta.Slot >= *wh.nextShredRevisionActivationSlot {
		wh.shredRevision++
		wh.nextShredRevisionActivationSlot = nil
	}
	mapping, err := wh.DB.GetEntries(meta, wh.shredRevision)
	if err != nil {
		return nil, err
	}
	batches := make([][]shred.Entry, len(mapping))
	for i, batch := range mapping {
		batches[i] = batch.Entries
	}
	return batches, nil
}
