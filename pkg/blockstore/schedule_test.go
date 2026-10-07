package blockstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestScheduleEdges(t *testing.T) {
	const epochStart, epochStop = 1000, 1999
	tests := []struct {
		name               string
		haveStart, haveEnd uint64
		full               bool
		wantStart, wantEnd uint64
		wantErr            bool
	}{
		{name: "full epoch ignores DB edges", haveStart: 500, haveEnd: 2500, full: true, wantStart: 1000, wantEnd: 1999},
		{name: "partial clamps to the epoch", haveStart: 500, haveEnd: 2500, wantStart: 1000, wantEnd: 1999},
		{name: "partial DB starts mid-epoch", haveStart: 1500, haveEnd: 2500, wantStart: 1500, wantEnd: 1999},
		{name: "partial DB ends mid-epoch", haveStart: 500, haveEnd: 1200, wantStart: 1000, wantEnd: 1200},
		{name: "partial DB inside the epoch", haveStart: 1100, haveEnd: 1200, wantStart: 1100, wantEnd: 1200},
		{name: "no overlap", haveStart: 2000, haveEnd: 2500, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := scheduleEdges(epochStart, epochStop, tt.haveStart, tt.haveEnd, tt.full)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantStart, start)
			assert.Equal(t, tt.wantEnd, end)
		})
	}
}
