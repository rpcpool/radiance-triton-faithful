package blockstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraversalSchedule_PruneLowerThan(t *testing.T) {
	schedule := TraversalSchedule{}
	schedule.schedule = append(schedule.schedule,
		DBtoSlots{slots: []uint64{1, 2, 3, 4}},
		DBtoSlots{slots: []uint64{5, 7, 9}},
	)

	schedule.PruneLowerThan(7)
	assert.Equal(t, []uint64{7, 9}, schedule.Slots())
	assert.Equal(t, uint64(2), schedule.totalSlotsToProcess)
	assert.Empty(t, schedule.schedule[0].slots)

	// Combined with PruneHigherThan, as --start-at-slot and --stop-at-slot are.
	schedule.PruneHigherThan(7)
	assert.Equal(t, []uint64{7}, schedule.Slots())
	assert.Equal(t, uint64(1), schedule.totalSlotsToProcess)
}
