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

// EntriesAndMarkers returns the entry batches of a slot and its Alpenglow block
// markers. A data-complete range holding a marker yields an empty batch, so batch
// i still lines up with meta.ReplayEntryEndIndexes()[i].
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
	batches := make([][]shred.Entry, len(mapping))
	var markers []*blockmarker.Marker
	for i, batch := range mapping {
		batches[i] = batch.Entries
		if batch.Marker != nil {
			markers = append(markers, batch.Marker)
		}
	}
	return batches, markers, nil
}

// BlockFooterMarker returns the BlockFooter marker among markers, if any.
func BlockFooterMarker(markers []*blockmarker.Marker) (*blockmarker.Marker, error) {
	var footer *blockmarker.Marker
	for _, m := range markers {
		if m.Variant != blockmarker.VariantFooter {
			continue
		}
		if footer != nil {
			return nil, fmt.Errorf("%w: multiple block footers", blockmarker.ErrInvalid)
		}
		footer = m
	}
	return footer, nil
}
