package slotedges

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

func TestEpochSchedule_MainnetMatchesFixedLength(t *testing.T) {
	s := MainnetEpochSchedule
	for _, slot := range []uint64{0, 1, 431999, 432000, 432001, 123456789, 444625260, 1<<40 - 7} {
		assert.Equal(t, slot/EpochLen, s.EpochForSlot(slot), "slot %d", slot)
	}
	for epoch := uint64(0); epoch <= 2000; epoch++ {
		start, stop := s.EpochLimits(epoch)
		require.Equal(t, epoch*EpochLen, start, "epoch %d", epoch)
		require.Equal(t, epoch*EpochLen+EpochLen-1, stop, "epoch %d", epoch)
	}
}

func TestEpochSchedule_Testnet(t *testing.T) {
	s := TestnetEpochSchedule

	// Epoch 1042 (Alpenglow activation) per testnet getEpochInfo.
	assert.Equal(t, uint64(444620256), s.FirstSlotInEpoch(1042))
	assert.Equal(t, uint64(1042), s.EpochForSlot(444620256))
	assert.Equal(t, uint64(1041), s.EpochForSlot(444620255))
	assert.Equal(t, uint64(1042), s.EpochForSlot(444625260))

	// End of warmup.
	assert.Equal(t, uint64(524256), s.FirstSlotInEpoch(14))
	assert.Equal(t, uint64(13), s.EpochForSlot(524255))
	assert.Equal(t, uint64(14), s.EpochForSlot(524256))
	assert.Equal(t, uint64(262112), s.FirstSlotInEpoch(13))
	assert.Equal(t, uint64(262144), s.SlotsInEpoch(13))
	assert.Equal(t, uint64(EpochLen), s.SlotsInEpoch(14))

	// Start of warmup: 32, 64, 128, ... slots.
	assert.Equal(t, uint64(0), s.EpochForSlot(0))
	assert.Equal(t, uint64(0), s.EpochForSlot(31))
	assert.Equal(t, uint64(1), s.EpochForSlot(32))
	start, stop := s.EpochLimits(0)
	assert.Equal(t, [2]uint64{0, 31}, [2]uint64{start, stop})
	start, stop = s.EpochLimits(1)
	assert.Equal(t, [2]uint64{32, 95}, [2]uint64{start, stop})

	for epoch := uint64(0); epoch <= 1100; epoch++ {
		first := s.FirstSlotInEpoch(epoch)
		require.Equal(t, epoch, s.EpochForSlot(first), "epoch %d", epoch)
		_, stop := s.EpochLimits(epoch)
		require.Equal(t, epoch, s.EpochForSlot(stop), "epoch %d", epoch)
		require.Equal(t, epoch+1, s.EpochForSlot(stop+1), "epoch %d", epoch)
	}
}

func TestSetEpochSchedule(t *testing.T) {
	defer SetEpochSchedule(CurrentEpochSchedule())

	assert.Equal(t, MainnetEpochSchedule, CurrentEpochSchedule())
	assert.Equal(t, uint64(1029), CalcEpochForSlot(444625260))

	SetEpochSchedule(TestnetEpochSchedule)
	assert.Equal(t, uint64(1042), CalcEpochForSlot(444625260))
	start, stop := CalcEpochLimits(1042)
	assert.Equal(t, [2]uint64{444620256, 444620256 + EpochLen - 1}, [2]uint64{start, stop})
}

func TestEpochScheduleForCluster(t *testing.T) {
	for cluster, want := range map[string]EpochSchedule{
		"mainnet": MainnetEpochSchedule, "mainnet-beta": MainnetEpochSchedule,
		"devnet": DevnetEpochSchedule, "testnet": TestnetEpochSchedule,
	} {
		got, err := EpochScheduleForCluster(cluster)
		require.NoError(t, err)
		assert.Equal(t, want, got, cluster)
	}
	_, err := EpochScheduleForCluster("localnet")
	assert.Error(t, err)
}
