//go:build !lite

package blockstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiWalk_Len(t *testing.T) {
	{
		mw := BlockWalk{
			handles: []WalkHandle{
				{Start: 0, Stop: 16},
				{Start: 14, Stop: 31},
				{Start: 32, Stop: 34},
				{Start: 36, Stop: 100},
			},
		}
		assert.Equal(t, int(100), int(mw.NumSlotsAvailable()))
	}
	{
		mw := BlockWalk{
			handles: []WalkHandle{
				{Start: 0, Stop: 10},
				{Start: 5, Stop: 9},
			},
		}
		assert.Equal(t, int(11), int(mw.NumSlotsAvailable()))
	}
	{
		nums := []WalkHandle{
			se(0, 10),
			se(5, 9),
		}
		require.Equal(t, int(11), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(0, 10),
			se(5, 9),
		}
		require.Equal(t, int(11), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(0, 10),
			se(5, 11),
		}
		require.Equal(t, int(12), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(4, 10),
			se(0, 11),
		}
		require.Equal(t, int(12), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(4, 10),
			se(3, 11),
		}
		require.Equal(t, int(9), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(4, 10),
			se(3, 11),
			se(0, 20),
		}
		require.Equal(t, int(21), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(4, 10),
			se(0, 20),
			se(3, 11),
		}
		require.Equal(t, int(21), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(0, 16),
			se(15, 31),
		}
		require.Equal(t, int(32), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(0, 16),
			se(17, 31),
		}
		require.Equal(t, int(32), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			se(0, 14),
			se(16, 31),
		}
		require.Equal(t, int(31), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			{Start: 0, Stop: 16},
			{Start: 14, Stop: 31},
			{Start: 32, Stop: 34},
			{Start: 36, Stop: 100},
		}
		require.Equal(t, int(100), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			{Start: 0, Stop: 16},
			{Start: 14, Stop: 31},
			{Start: 32, Stop: 34},
			{Start: 36, Stop: 100},
		}
		require.Equal(t, int(100), int(calcContiguousSum(nums)))
	}
	{
		nums := []WalkHandle{
			{Start: 0, Stop: 1},
			{Start: 1, Stop: 2},
			{Start: 3, Stop: 4},
		}
		require.Equal(t, int(5), int(calcContiguousSum(nums)))
	}
}

func se(start, stop uint64) WalkHandle {
	return WalkHandle{Start: start, Stop: stop}
}
