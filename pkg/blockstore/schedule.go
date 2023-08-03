package blockstore

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/vbauerster/mpb/v8"
	"go.firedancer.io/radiance/pkg/slotedges"
	"k8s.io/klog/v2"
)

type TraversalSchedule struct {
	shredRevision                   int
	nextShredRevisionActivationSlot *uint64

	///
	totalSlotsToProcess uint64
	bar                 *mpb.Bar
	///

	schedule []DBtoSlots
}

type ScheduleIterator struct {
	schedule *TraversalSchedule
	limit    uint64
}

func (s TraversalSchedule) NewIterator(limit uint64) *ScheduleIterator {
	return &ScheduleIterator{
		schedule: &s,
		limit:    limit,
	}
}

// TraversalSchedule.Iterate iterates over the schedule.
func (i *ScheduleIterator) Iterate(
	ctx context.Context,
	f func(dbIdex int, h WalkHandle, slot uint64, shredRevision int) error,
) error {
	numDone := uint64(0)
	return i.schedule.EachSlot(
		ctx,
		func(dbIdex int, h WalkHandle, slot uint64, shredRevision int) error {
			if err := f(dbIdex, h, slot, shredRevision); err != nil {
				return err
			}
			numDone++
			if i.limit > 0 && numDone >= i.limit {
				return ErrStopIteration
			}
			return nil
		})
}

var ErrStopIteration = fmt.Errorf("stop iteration")

func (s TraversalSchedule) FirstSlot() (uint64, bool) {
	first := s.firstNonEmptyDB()
	if first == nil {
		return 0, false
	}
	return first.FirstSlot()
}

func (s TraversalSchedule) LastSlot() (uint64, bool) {
	last := s.lastNonEmptyDB()
	if last == nil {
		return 0, false
	}
	return last.LastSlot()
}

// Each iterates over each DB in the schedule.
func (s TraversalSchedule) Each(
	ctx context.Context,
	f func(dbIndex int, db WalkHandle, slots []uint64) error,
) error {
	for dbIndex, db := range s.schedule {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := f(dbIndex, db.handle, db.slots); err != nil {
			if err == ErrStopIteration {
				return nil
			}
			return err
		}
	}
	return nil
}

// EachSlot iterates over each slot in the schedule.
func (s TraversalSchedule) EachSlot(
	ctx context.Context,
	f func(dbIdex int, h WalkHandle, slot uint64, shredRevision int) error,
) error {
	if err := s.initStatsTracker(ctx); err != nil {
		return err
	}
	defer s.bar.Abort(true)
	shredRevision := s.shredRevision
	nextRevisionActivationSlot := s.nextShredRevisionActivationSlot

	activationSlot := cloneUint64Ptr(nextRevisionActivationSlot)
	return s.Each(
		ctx,
		func(dbIndex int, db WalkHandle, slots []uint64) error {
			for _, slot := range slots {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				s.bar.Increment()
				// If we have a shred revision, we need to check if the slot is in the range of the next revision activation.
				// If so, we need to increment the shred revision.
				if activationSlot != nil && slot >= *activationSlot {
					shredRevision++
					activationSlot = nil
				}
				if err := f(dbIndex, db, slot, shredRevision); err != nil {
					return err
				}
			}
			return nil
		})
}

func (s TraversalSchedule) firstNonEmptyDB() *DBtoSlots {
	for _, db := range s.schedule {
		if len(db.slots) > 0 {
			return &db
		}
	}
	return nil
}

func (s TraversalSchedule) lastNonEmptyDB() *DBtoSlots {
	for i := len(s.schedule) - 1; i >= 0; i-- {
		db := s.schedule[i]
		if len(db.slots) > 0 {
			return &db
		}
	}
	return nil
}

func (s TraversalSchedule) NumSlots() int {
	var numSlots int
	for _, db := range s.schedule {
		numSlots += len(db.slots)
	}
	return numSlots
}

func (s TraversalSchedule) SatisfiesEpochEdges(epoch uint64) error {
	if len(s.schedule) == 0 {
		return fmt.Errorf("schedule is empty")
	}

	start, end := slotedges.CalcEpochLimits(epoch)

	{ // The first DB's first slot must be <= the start slot.
		firstDB := s.firstNonEmptyDB()
		if firstDB == nil {
			return fmt.Errorf("all DBs are empty")
		}
		firstSlot, ok := firstDB.FirstSlot()
		if !ok {
			return fmt.Errorf("first DB has no slots")
		}
		if firstSlot > start {
			// NOTE: in exceptional cases, the epoch has a few skipped slots at the beginning,
			// and the DB has no slots before that.
			return fmt.Errorf(
				"epoch %d first slot %d is not available in any DB (first available slot is %d)",
				epoch,
				start,
				firstSlot,
			)
		}
	}

	{ // The first DB's last slot must be >= the end slot.
		lastDB := s.lastNonEmptyDB()
		if lastDB == nil {
			return fmt.Errorf("all DBs are empty")
		}
		lastSlot, ok := lastDB.LastSlot()
		if !ok {
			return fmt.Errorf("last DB has no slots")
		}
		if lastSlot < end {
			return fmt.Errorf(
				"epoch %d last slot %d is not available in any DB (last available slot is %d)",
				epoch,
				end,
				lastSlot,
			)
		}
	}

	return nil
}

type DBtoSlots struct {
	handle WalkHandle
	slots  []uint64
}

// DBtoSlots.Slots returns the slots in ascending order.
func (d DBtoSlots) Slots() []uint64 {
	Uint64Ascending(d.slots)
	return d.slots
}

func (d DBtoSlots) FirstSlot() (uint64, bool) {
	if len(d.slots) == 0 {
		return 0, false
	}
	return d.slots[0], true
}

func (d DBtoSlots) LastSlot() (uint64, bool) {
	if len(d.slots) == 0 {
		return 0, false
	}
	return d.slots[len(d.slots)-1], true
}

// DBtoSlots.DBHandle returns the DBHandle.
func (d DBtoSlots) DBHandle() WalkHandle {
	return d.handle
}

func (s TraversalSchedule) Slots() []uint64 {
	var slots []uint64
	for _, db := range s.schedule {
		slots = append(slots, db.slots...)
	}
	Uint64Ascending(slots)
	if !uint64SlicesAreEqual(slots, unique(slots)) {
		klog.Error("slots contains duplicates")
	}
	return unique(slots)
}

func contains(slots []uint64, slot uint64) bool {
	i := SearchUint64(slots, slot)
	return i < len(slots) && slots[i] == slot
}

func SearchUint64(a []uint64, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

// Uint64Ascending sorts a slice of uint64 in increasing order.
func Uint64Ascending(x []uint64) { sort.Sort(Uint64Slice(x)) }

func Uint64Descending(x []uint64) {
	sort.Sort(sort.Reverse(Uint64Slice(x)))
}

type Uint64Slice []uint64

func (x Uint64Slice) Len() int           { return len(x) }
func (x Uint64Slice) Less(i, j int) bool { return x[i] < x[j] }
func (x Uint64Slice) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

// Sort is a convenience method: x.Sort() calls Sort(x).
func (x Uint64Slice) Sort() { sort.Sort(x) }

func unique(slots []uint64) []uint64 {
	var out []uint64
	for _, slot := range slots {
		if !contains(out, slot) {
			out = append(out, slot)
		}
	}
	return out
}

func uint64OrNil(x *uint64) any {
	if x == nil {
		return nil
	}
	return *x
}

func NewSchedule(
	epoch uint64,
	requireFullEpoch bool,
	handles []WalkHandle,
	shredRevision int,
	nextRevisionActivationSlot uint64,
) (*TraversalSchedule, error) {
	var activationSlot *uint64
	if nextRevisionActivationSlot != 0 {
		activationSlot = &nextRevisionActivationSlot
	}
	if err := sortWalkHandles(handles, shredRevision, activationSlot); err != nil {
		return nil, err
	}
	for ii := range handles {
		handles[ii].shredRevision = shredRevision
		handles[ii].nextShredRevisionActivationSlot = activationSlot
	}
	officialEpochStart, officialEpochStop := slotedges.CalcEpochLimits(epoch)
	haveStart, haveStop := slotEdges(handles)

	var startFrom, stopAt uint64
	if requireFullEpoch {
		startFrom, stopAt = officialEpochStart, officialEpochStop
	} else {
		startFrom, stopAt = haveStart, haveStop
	}

	if !slotedges.Uint64RangesHavePartialOverlapIncludingEdges(
		[2]uint64{officialEpochStart, officialEpochStop},
		[2]uint64{startFrom, stopAt},
	) {
		klog.Exitf("no overlap between requested epoch [%d:%d] and available slots [%d:%d]",
			officialEpochStart, officialEpochStop,
			startFrom, stopAt)
	}

	schedule := new(TraversalSchedule)
	err := schedule.init(epoch, startFrom, stopAt, handles, shredRevision, activationSlot)
	if err != nil {
		return nil, err
	}
	schedule.totalSlotsToProcess = uint64(schedule.NumSlots())
	return schedule, nil
}

func (s *TraversalSchedule) Close() error {
	for _, db := range s.schedule {
		db.handle.DB.Close()
	}
	return nil
}

func (s *TraversalSchedule) getHandles() []WalkHandle {
	var out []WalkHandle
	for _, db := range s.schedule {
		out = append(out, db.handle)
	}
	return out
}

func (s *TraversalSchedule) RootExistsInAnyDB(slot uint64) (string, error) {
	for _, h := range s.getHandles() {
		key := encodeSlotAsKey(slot)
		opts := getReadOptions()
		defer putReadOptions(opts)
		got, err := h.DB.DB.GetCF(opts, h.DB.CfRoot, key)
		if err != nil {
			continue
		}
		defer got.Free()
		if got == nil || got.Size() == 0 {
			continue
		}
		if !got.Exists() {
			continue
		}
		return h.DB.DB.Name(), nil
	}
	return "", fmt.Errorf("slot %d not found in any DB", slot)
}

func (m *TraversalSchedule) GetSlotMetaFromAnyDB(slot uint64) (*SlotMeta, error) {
	for _, h := range m.getHandles() {
		got, err := h.DB.GetSlotMeta(slot)
		if err == nil && got != nil {
			return got, nil
		}
	}
	return nil, fmt.Errorf("meta for slot %d not found in any DB", slot)
}

func slotEdges(handles []WalkHandle) (low, high uint64) {
	if len(handles) == 0 {
		return 0, 0
	}
	low = handles[0].Start
	high = handles[len(handles)-1].Stop
	return
}

func (m *TraversalSchedule) SlotEdges() (low, high uint64) {
	if len(m.schedule) == 0 {
		return 0, 0
	}
	low = m.firstNonEmptyDB().DBHandle().Start
	high = m.lastNonEmptyDB().handle.Stop
	return
}

func (schedule *TraversalSchedule) init(
	epoch uint64,
	start,
	stop uint64,
	handles []WalkHandle,
	shredRevision int,
	nextRevisionActivationSlot *uint64,
) error {
	schedule.shredRevision = shredRevision
	schedule.nextShredRevisionActivationSlot = cloneUint64Ptr(nextRevisionActivationSlot)

	reversedHandles := make([]WalkHandle, len(handles))
	copy(reversedHandles, handles)
	for i := len(reversedHandles)/2 - 1; i >= 0; i-- {
		opp := len(reversedHandles) - 1 - i
		reversedHandles[i], reversedHandles[opp] = reversedHandles[opp], reversedHandles[i]
	}

	overrides := make(map[uint64]uint64)
	defer func() {
		if len(overrides) > 0 {
			klog.Infof("Overrides:")
			for k, v := range overrides {
				klog.Infof("  %d -> %d", k, v)
			}
		}
	}()

	activationSlot := cloneUint64Ptr(nextRevisionActivationSlot)
	var prevProcessedSlot *uint64
	wanted := stop // NOTE: this is a guess; if not found, we will get the next bigger slot
	for hi, h := range reversedHandles {
		var lastOfPReviousDB *uint64
		if hi > 0 {
			prevSlots := schedule.schedule[hi-1].slots
			if len(prevSlots) > 0 {
				lastOfPReviousDB = &prevSlots[len(prevSlots)-1]
			}
		}
		klog.Infof(
			("wanted=%d, prevProcessed=%v; lastOfPreviousDB=%v; starting with db %s"),
			wanted,
			uint64OrNil(prevProcessedSlot),
			uint64OrNil(lastOfPReviousDB),
			h.DB.DB.Name(),
		)
		var slots []uint64
		opts := getReadOptions()
		defer putReadOptions(opts)
		// Open Next database
		iter := h.DB.DB.NewIteratorCF(opts, h.DB.CfRoot)
		defer iter.Close()

		// now get the meta for the parent slot
		for {
			// FIND THE FIRST SLOT IN THE FIRST DB, ROOT
			key := MakeSlotKey(wanted)
			iter.Seek(key[:])
			if !iter.Valid() {
				klog.Infof(("seeked to slot %d but got invalid"), wanted)
				break
			}
			gotRoot, ok := ParseSlotKey(iter.Key().Data())
			if !ok {
				return fmt.Errorf("Invalid slot key: %x", iter.Key().Data())
			}
			if prevProcessedSlot != nil && gotRoot == *prevProcessedSlot {
				// slots = append(slots, wanted)
				{
					// go back one slot
					iter.Prev()
					gotRoot, ok = ParseSlotKey(iter.Key().Data())
					if !ok {
						return fmt.Errorf("Invalid slot key: %x", iter.Key().Data())
					}
				}
			}
			if start != 0 && gotRoot < start-1 {
				// inlude one more slot
				klog.Infof(("reached slot %d which is lower than the low bound %d (OK)"), gotRoot, start)
				break
			}
			// Check what we got (might be a different gotRoot than what we wanted):
			if gotRoot != wanted {
				klog.Errorf(("seeked to slot %d but got slot %d; trying override"), wanted, gotRoot)
				// The wanted root was not found in the CfRoot; if it exists in the CfMeta, we can still use it.
				_, err := h.DB.GetSlotMeta(wanted)
				if err == nil {
					klog.Infof(("override worked: %d -> %d (recovered missing root)"), gotRoot, wanted)
					if existingOverride, ok := overrides[gotRoot]; ok {
						if existingOverride != wanted {
							klog.Infof("Override already exists: %d -> %d", gotRoot, existingOverride)
						}
					} else {
						overrides[gotRoot] = wanted
					}
					gotRoot = wanted
				} else {
					// override failed.
					if prevProcessedSlot != nil {
						// We really wanted that slot because it was the parent of the previous slot.
						// If we can't find it, we can't continue.
						return fmt.Errorf("Failed to get meta for slot %d (parent of %d): %s", wanted, err, *prevProcessedSlot)
					}
					// This is fine because the first wanted slot was a guess, and it wasn't found (and the DB returned a different slot).
					// We can just use the slot we got.
					klog.Infof(("override failed: %d -> %d (which doesn't exist)"), gotRoot, wanted)
					if wanted == stop && gotRoot < stop {
						// We wanted the last slot, but we got a smaller slot.
						// This means that we can't be sure the DB is complete.
						// We can't continue.
						return fmt.Errorf(
							"Wanted to get slot %d but got %d instead, which is smaller than the stop slot %d (and we can't be sure the DB is complete)",
							wanted,
							gotRoot,
							stop,
						)
					}
				}
			}

			// now get the meta for this slot
			meta, err := h.DB.GetSlotMeta(gotRoot)
			if err != nil {
				return fmt.Errorf("Failed to get meta for slot %d: %s", gotRoot, err)
			}
			if meta == nil {
				return fmt.Errorf("Meta for slot %d is nil", gotRoot)
			}

			// If we have a shred revision, we need to check if the slot is in the range of the next revision activation.
			// If so, we need to increment the shred revision.
			if activationSlot != nil && gotRoot >= *activationSlot {
				shredRevision++
				activationSlot = nil
			}

			if false {
				_, err = h.DB.GetEntries(meta, shredRevision)
				if err == nil {
					// Success!
				} else {
					return fmt.Errorf("Failed to get entries for slot %d: %s", gotRoot, err)
				}
			}
			// fmt.Printf("slot=%d has %d entries; prev=%d\n", gotRoot, len(entries), meta.ParentSlot)
			if meta.ParentSlot == math.MaxUint64 {
				// Has all the data except for what's the parent slot.
				// We skip adding it here, and hope we will find it in the next DB.
				// We also assume that if meta.ParentSlot == math.MaxUint64, then this is the end of the DB.
				break
			}
			slots = append(slots, gotRoot)
			if gotRoot == 0 {
				break
			}
			// if gotRoot == meta.ParentSlot {
			// 	// TODO: is this correct?
			// 	break
			// }

			prevProcessedSlot = &gotRoot
			wanted = meta.ParentSlot
		}

		Uint64Ascending(slots)

		if !uint64SlicesAreEqual(slots, unique(slots)) {
			panic("slots are not unique")
		}

		schedule.schedule = append(schedule.schedule, DBtoSlots{
			handle: h,
			slots:  unique(slots),
		})
	}
	// reverse the schedule so that it is in ascending order
	for i := len(schedule.schedule)/2 - 1; i >= 0; i-- {
		opp := len(schedule.schedule) - 1 - i
		schedule.schedule[i], schedule.schedule[opp] = schedule.schedule[opp], schedule.schedule[i]
	}
	return nil
}

func greenBG(s string) string {
	return blackFG("\033[48;5;2m" + s + "\033[0m")
}

func blackFG(s string) string {
	return "\033[38;5;0m" + s + "\033[0m"
}

func uint64SlicesAreEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if b[i] != x {
			return false
		}
	}
	return true
}
