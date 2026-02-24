//go:build !lite

package blockstore

import (
	"fmt"
	"sort"

	"github.com/linxGnu/grocksdb"
)

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
	var lastError error
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
		meta, ver, err := DecodeSlotMetaAuto(iter.Value().Data())
		if err != nil {
			return 0, fmt.Errorf(
				"getLowestCompletedSlot(%s): choked on invalid meta for slot %d", d.DB.Name(), slot)
		}
		if ver != SlotMetaV1 && ver != SlotMetaV2 {
			return 0, fmt.Errorf(
				"getLowestCompletedSlot(%s): choked on unsupported meta version %d for slot %d", d.DB.Name(), ver, slot)
		}

		if _, err = d.GetEntries(meta, shredRevision); err == nil {
			// Success!
			return slot, nil
		} else {
			lastError = fmt.Errorf("getLowestCompletedSlot: failed to get entries for slot %d: %w", slot, err)
		}

		iter.Next()
	}

	return 0, fmt.Errorf("failed to find a valid complete slot in DB: %s; last error: %w", d.DB.Name(), lastError)
}
