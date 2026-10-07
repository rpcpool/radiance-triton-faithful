package blockstore

import (
	"fmt"
	"sort"

	"github.com/rpcpool/yellowstone-faithful/blockmarker"
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
	batches, _, err := wh.EntriesAndMarkers(meta)
	return batches, err
}

// EntriesAndMarkers returns the entry batches and block markers of a slot (see SplitMarkers).
func (wh *WalkHandle) EntriesAndMarkers(meta *SlotMeta) ([][]shred.Entry, []*blockmarker.Marker, error) {
	// TODO: handle concurrent calls to Entries() on the same WalkHandle.
	if wh.nextShredRevisionActivationSlot != nil && meta.Slot >= *wh.nextShredRevisionActivationSlot {
		wh.shredRevision++
		wh.nextShredRevisionActivationSlot = nil
	}
	mapping, err := wh.DB.GetEntries(meta, wh.shredRevision)
	if err != nil {
		return nil, nil, err
	}
	return SplitMarkers(meta, mapping)
}

// SplitMarkers returns the replayed entry batches and all Alpenglow block markers
// in block order, validating the marker layout. A data-complete range holding a
// marker yields an empty batch, so batch i lines up with meta.ReplayEntryEndIndexes()[i].
func SplitMarkers(meta *SlotMeta, mapping []Entries) ([][]shred.Entry, []*blockmarker.Marker, error) {
	batches := make([][]shred.Entry, 0, len(mapping))
	var markers []*blockmarker.Marker
	for _, batch := range mapping {
		if batch.Marker != nil {
			markers = append(markers, batch.Marker)
		}
		if !batch.BeforeReplay {
			batches = append(batches, batch.Entries)
		}
	}
	if len(markers) > 0 {
		if err := ValidateMarkerLayout(mapping); err != nil {
			return nil, nil, fmt.Errorf("slot %d: %w", meta.Slot, err)
		}
	}
	return batches, markers, nil
}
