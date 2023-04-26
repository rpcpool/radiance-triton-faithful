package createcar

import "sort"

type SlotRangeSchedule [][]uint64

// SplitSlotsIntoRanges splits the given slots into ranges of the given length.
func SplitSlotsIntoRanges(maxRangeLen int, slots []uint64) SlotRangeSchedule {
	if maxRangeLen < 1 {
		panic("maxRangeLen must be greater than 0")
	}
	if len(slots) == 0 {
		return SlotRangeSchedule{slots}
	}

	// sort the slots
	sort.Slice(slots, func(i, j int) bool {
		return slots[i] < slots[j]
	})

	if len(slots) <= maxRangeLen {
		return SlotRangeSchedule{slots}
	}

	// group them into ranges
	var ranges [][]uint64
	for i := 0; i < len(slots); i += maxRangeLen {
		end := i + maxRangeLen
		if end > len(slots) {
			end = len(slots)
		}
		ranges = append(ranges, slots[i:end])
	}
	// return the ranges
	return ranges
}
