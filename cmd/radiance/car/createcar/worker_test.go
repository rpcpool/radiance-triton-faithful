package createcar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalcEpochLimits(t *testing.T) {
	{
		epochStart, epochStop := CalcEpochLimits(0)
		require.Equal(t, uint64(0), epochStart)
		require.Equal(t, uint64(431_999), epochStop)
	}
	{
		epochStart, epochStop := CalcEpochLimits(1)
		require.Equal(t, uint64(432_000), epochStart)
		require.Equal(t, uint64(863_999), epochStop)
	}
	{
		epochStart, epochStop := CalcEpochLimits(333)
		require.Equal(t, uint64(143_856_000), epochStart)
		require.Equal(t, uint64(144_287_999), epochStop)
	}
	{
		epochStart, epochStop := CalcEpochLimits(447)
		require.Equal(t, uint64(193_104_000), epochStart)
		require.Equal(t, uint64(193_535_999), epochStop)
	}
}

func TestUint64RangesHavePartialOverlapIncludingEdges(t *testing.T) {
	{
		r1 := [2]uint64{0, 10}
		r2 := [2]uint64{5, 15}
		require.True(t, Uint64RangesHavePartialOverlapIncludingEdges(r1, r2))
	}
	{
		r1 := [2]uint64{0, 10}
		r2 := [2]uint64{10, 15}
		require.True(t, Uint64RangesHavePartialOverlapIncludingEdges(r1, r2))
	}
	{
		r1 := [2]uint64{0, 10}
		r2 := [2]uint64{11, 15}
		require.False(t, Uint64RangesHavePartialOverlapIncludingEdges(r1, r2))
	}
	{
		r1 := [2]uint64{0, 10}
		r2 := [2]uint64{0, 10}
		require.True(t, Uint64RangesHavePartialOverlapIncludingEdges(r1, r2))
	}
	{
		r1 := [2]uint64{10, 20}
		r2 := [2]uint64{0, 10}
		require.True(t, Uint64RangesHavePartialOverlapIncludingEdges(r1, r2))
	}
}
