//go:build !lite

package blockstore

import (
	"fmt"
	"sort"

	"github.com/linxGnu/grocksdb"
	"go.firedancer.io/radiance/pkg/shred"
	"k8s.io/klog/v2"
)

// BlockWalk walks blocks in ascending order over multiple RocksDB databases.
type BlockWalk struct {
	handles                         []WalkHandle // sorted
	shredRevision                   int
	dbIndex                         int
	nextShredRevisionActivationSlot *uint64

	root        *grocksdb.Iterator
	onBeforePop func() error
}

func NewBlockWalk(handles []WalkHandle, shredRevision int) (*BlockWalk, error) {
	if err := sortWalkHandles(handles, shredRevision, nil); err != nil {
		return nil, err
	}
	return &BlockWalk{
		handles:       handles,
		shredRevision: shredRevision,
		dbIndex:       -1,
	}, nil
}

// NewBlockWalkWithNextShredRevisionActivationSlot creates a BlockWalk that
// that will shredRevision+1 when it reaches nextRevisionActivationSlot.
func NewBlockWalkWithNextShredRevisionActivationSlot(
	handles []WalkHandle,
	shredRevision int,
	nextRevisionActivationSlot uint64,
) (*BlockWalk, error) {
	var activationSlot *uint64
	if nextRevisionActivationSlot != 0 {
		activationSlot = &nextRevisionActivationSlot
	}
	if err := sortWalkHandles(handles, shredRevision, activationSlot); err != nil {
		return nil, err
	}
	return &BlockWalk{
		handles:                         handles,
		shredRevision:                   shredRevision,
		dbIndex:                         -1,
		nextShredRevisionActivationSlot: activationSlot,
	}, nil
}

func (m *BlockWalk) SetOnBeforePop(f func() error) {
	m.onBeforePop = f
}

// Seek skips ahead to a specific slot.
// The caller must call BlockWalk.Next after Seek.
func (m *BlockWalk) Seek(slot uint64) bool {
	for len(m.handles) > 0 {
		h := m.handles[0]
		if slot < h.Start {
			// trying to Seek to slot below lowest available
			return false
		}
		if slot <= h.Stop {
			h.Start = slot
			return true
		}
		m.pop()
	}
	return false
}

// NumSlotsAvailable returns the number of contiguous slots that lay ahead.
func (m *BlockWalk) NumSlotsAvailable() (total uint64) {
	if len(m.handles) == 0 {
		return 0
	}
	return calcContiguousSum(m.handles)
}

func calcContiguousSum(nums []WalkHandle) uint64 {
	// sort the ranges by start
	sort.Slice(nums, func(i, j int) bool {
		return nums[i].Start < nums[j].Start
	})

	type StartStop struct {
		Start uint64
		Stop  uint64
	}

	mergedRanges := make([]StartStop, 0, len(nums))
	// merge overlapping ranges
	for _, r := range nums {
		if len(mergedRanges) == 0 {
			mergedRanges = append(mergedRanges, StartStop{Start: r.Start, Stop: r.Stop})
			continue
		}
		last := mergedRanges[len(mergedRanges)-1]
		if last.Stop >= r.Start {
			if last.Stop < r.Stop {
				last.Stop = r.Stop
			}
			mergedRanges[len(mergedRanges)-1] = last
		} else {
			mergedRanges = append(mergedRanges, StartStop{Start: r.Start, Stop: r.Stop})
		}
	}

	var sum uint64
	for _, r := range mergedRanges {
		sum += r.Stop - r.Start + 1
	}
	return sum
}

// SlotEdges returns the lowest and highest slot numbers that are available
// among all the databases.
func (m *BlockWalk) SlotEdges() (low, high uint64) {
	if len(m.handles) == 0 {
		return 0, 0
	}
	low = m.handles[0].Start
	high = m.handles[len(m.handles)-1].Stop
	return
}

func (m *BlockWalk) DBIndex() int {
	return m.dbIndex
}

// Next seeks to the next slot.
func (m *BlockWalk) Next() (meta *SlotMeta, ok bool) {
	if len(m.handles) == 0 {
		return nil, false
	}
	h := m.handles[0]
	if m.root == nil {
		opts := getReadOptions()
		defer putReadOptions(opts)
		// Open Next database
		m.root = h.DB.DB.NewIteratorCF(opts, h.DB.CfRoot)
		m.dbIndex++
		klog.Infof("Opening next DB: %s", h.DB.DB.Name())
		key := MakeSlotKey(h.Start)
		m.root.Seek(key[:])
	}
	if !m.root.Valid() {
		// Close current DB and go to Next
		err := m.pop()
		if err != nil {
			klog.Exitf("Failed to pop: %s", err)
			// TODO: return error instead of exiting
		}
		return m.Next() // TODO tail recursion optimization?
	}

	// Get key at current position.
	slot, ok := ParseSlotKey(m.root.Key().Data())
	if !ok {
		klog.Exitf("Invalid slot key: %x", m.root.Key().Data())
	}
	if slot > h.Stop {
		m.pop()
		return m.Next()
	}
	h.Start = slot

	// Get value at current position.
	var err error
	meta, err = h.DB.GetSlotMeta(slot)
	if err != nil {
		// Invalid slot metas are irrecoverable.
		// The CAR generation process must stop here.
		klog.Errorf("FATAL: invalid slot meta at slot %d, aborting CAR generation: %s", slot, err)
		return nil, false
	}

	// Seek iterator to Next entry.
	m.root.Next()

	return meta, true
}

// Entries returns the entries at the current cursor.
// Caller must have made an ok call to BlockWalk.Next before calling this.
func (m *BlockWalk) Entries(meta *SlotMeta) ([][]shred.Entry, error) {
	if m.nextShredRevisionActivationSlot != nil && meta.Slot >= *m.nextShredRevisionActivationSlot {
		m.shredRevision++
		m.nextShredRevisionActivationSlot = nil
	}
	h := m.handles[0]
	mapping, err := h.DB.GetEntries(meta, m.shredRevision)
	if err != nil {
		return nil, err
	}
	batches := make([][]shred.Entry, len(mapping))
	for i, batch := range mapping {
		batches[i] = batch.Entries
	}
	return batches, nil
}

func (m *BlockWalk) TransactionMetas(keys ...[]byte) ([]*TransactionStatusMetaWithRaw, error) {
	h := m.handles[0]
	return h.DB.GetTransactionMetas(keys...)
}

func (m *BlockWalk) GetSlotMetaFromAnyDB(slot uint64) (*SlotMeta, error) {
	for _, h := range m.handles {
		got, err := h.DB.GetSlotMeta(slot)
		if err == nil && got != nil {
			return got, nil
		}
	}
	return nil, fmt.Errorf("meta for slot %d not found in any DB", slot)
}

func (m *BlockWalk) RootExistsInAnyDB(slot uint64) (string, error) {
	for _, h := range m.handles {
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

func (m *BlockWalk) BlockTime(slot uint64) (uint64, error) {
	h := m.handles[0]
	return h.DB.GetBlockTime(slot)
}

func (m *BlockWalk) BlockHeight(slot uint64) (*uint64, error) {
	h := m.handles[0]
	return h.DB.GetBlockHeight(slot)
}

func (m *BlockWalk) Rewards(slot uint64) ([]byte, error) {
	h := m.handles[0]
	return h.DB.GetRewards(slot)
}

// pop closes the current open DB.
func (m *BlockWalk) pop() error {
	if m.onBeforePop != nil {
		err := m.onBeforePop()
		if err != nil {
			return fmt.Errorf("onBeforePop: %w", err)
		}
	}
	if m.root == nil {
		klog.Infof("pop called with no open DB")
	}
	if m.root != nil {
		m.root.Close()
	}
	m.root = nil
	m.handles[0].DB.Close()
	m.handles = m.handles[1:]
	return nil
}

func (m *BlockWalk) Close() {
	if m.root != nil {
		m.root.Close()
		m.root = nil
	}
	for _, h := range m.handles {
		h.DB.Close()
	}
	m.handles = nil
}

type WalkHandle struct {
	DB    *DB
	Start uint64
	Stop  uint64 // inclusive
}

// sortWalkHandles detects bounds of each DB and sorts handles.
func sortWalkHandles(h []WalkHandle, shredRevision int, nextRevisionActivationSlot *uint64) error {
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
		h[i] = WalkHandle{
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

func cloneUint64Ptr(p *uint64) *uint64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// getLowestCompleteSlot finds the lowest slot in a RocksDB from which slots are complete onwards.
func getLowestCompletedSlot(d *DB, shredRevision int, nextRevisionActivationSlot *uint64) (uint64, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := d.DB.NewIteratorCF(opts, d.CfMeta)
	defer iter.Close()
	iter.SeekToFirst()

	// The Solana validator periodically prunes old slots to keep database space bounded.
	// Therefore, the first (few) slots might have valid meta entries but missing data shreds.
	// To work around this, we simply start at the lowest meta and iterate until we find a complete entry.

	const maxTries = 32

	activationSlot := cloneUint64Ptr(nextRevisionActivationSlot)
	for i := 0; iter.Valid() && i < maxTries; i++ {
		slot, ok := ParseSlotKey(iter.Key().Data())
		if !ok {
			return 0, fmt.Errorf(
				"getLowestCompletedSlot(%s): choked on invalid slot key: %x", d.DB.Name(), iter.Key().Data())
		}

		// If we have a shred revision, we need to check if the slot is in the range of the next revision activation.
		// If so, we need to increment the shred revision.
		if activationSlot != nil && slot >= *activationSlot {
			shredRevision++
			activationSlot = nil
		}

		// RocksDB row writes are atomic, therefore meta should never be broken.
		// If we fail to decode meta, bail as early as possible, as we cannot guarantee compatibility.
		meta, err := ParseBincode[SlotMeta](iter.Value().Data())
		if err != nil {
			return 0, fmt.Errorf(
				"getLowestCompletedSlot(%s): choked on invalid meta for slot %d", d.DB.Name(), slot)
		}

		if _, err = d.GetEntries(meta, shredRevision); err == nil {
			// Success!
			return slot, nil
		}

		iter.Next()
	}

	return 0, fmt.Errorf("failed to find a valid complete slot in DB: %s", d.DB.Name())
}
