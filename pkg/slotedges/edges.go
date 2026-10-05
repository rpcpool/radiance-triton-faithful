package slotedges

import (
	"fmt"
	"math/bits"
	"sync/atomic"
)

// Uint64RangesHavePartialOverlapIncludingEdges returns true if the two ranges have any overlap.
func Uint64RangesHavePartialOverlapIncludingEdges(r1 [2]uint64, r2 [2]uint64) bool {
	if r1[0] < r2[0] {
		return r1[1] >= r2[0]
	} else {
		return r2[1] >= r1[0]
	}
}

// EpochLen is the number of slots in a normal (post-warmup) epoch on all public clusters.
const EpochLen = 432000

// minimumSlotsPerEpoch is the length of the first warmup epoch (agave MINIMUM_SLOTS_PER_EPOCH).
const minimumSlotsPerEpoch = 32

// EpochSchedule mirrors agave's solana-epoch-schedule. Clusters with warmup
// (testnet) start with epochs of 32, 64, 128, ... slots until FirstNormalEpoch.
type EpochSchedule struct {
	SlotsPerEpoch    uint64
	FirstNormalEpoch uint64
	FirstNormalSlot  uint64
}

// Epoch schedules of the public clusters, from getEpochSchedule.
var (
	MainnetEpochSchedule = EpochSchedule{SlotsPerEpoch: EpochLen}
	DevnetEpochSchedule  = EpochSchedule{SlotsPerEpoch: EpochLen}
	TestnetEpochSchedule = EpochSchedule{SlotsPerEpoch: EpochLen, FirstNormalEpoch: 14, FirstNormalSlot: 524256}
)

// EpochScheduleForCluster returns the epoch schedule of a public cluster.
func EpochScheduleForCluster(cluster string) (EpochSchedule, error) {
	switch cluster {
	case "mainnet", "mainnet-beta":
		return MainnetEpochSchedule, nil
	case "devnet":
		return DevnetEpochSchedule, nil
	case "testnet":
		return TestnetEpochSchedule, nil
	default:
		return EpochSchedule{}, fmt.Errorf("unknown cluster %q (want mainnet, devnet or testnet)", cluster)
	}
}

func (s EpochSchedule) String() string {
	return fmt.Sprintf("slots_per_epoch=%d first_normal_epoch=%d first_normal_slot=%d",
		s.SlotsPerEpoch, s.FirstNormalEpoch, s.FirstNormalSlot)
}

// FirstSlotInEpoch returns the first slot of the epoch.
func (s EpochSchedule) FirstSlotInEpoch(epoch uint64) uint64 {
	if epoch <= s.FirstNormalEpoch {
		return (1<<epoch - 1) * minimumSlotsPerEpoch
	}
	return (epoch-s.FirstNormalEpoch)*s.SlotsPerEpoch + s.FirstNormalSlot
}

// SlotsInEpoch returns the number of slots in the epoch.
func (s EpochSchedule) SlotsInEpoch(epoch uint64) uint64 {
	if epoch < s.FirstNormalEpoch {
		return minimumSlotsPerEpoch << epoch
	}
	return s.SlotsPerEpoch
}

// EpochForSlot returns the epoch that contains the slot.
func (s EpochSchedule) EpochForSlot(slot uint64) uint64 {
	if slot < s.FirstNormalSlot {
		// Warmup epoch e spans [(2^e-1)*32, (2^(e+1)-1)*32).
		return uint64(bits.Len64(slot/minimumSlotsPerEpoch+1)) - 1
	}
	return s.FirstNormalEpoch + (slot-s.FirstNormalSlot)/s.SlotsPerEpoch
}

// EpochLimits returns the first and last slot of the epoch (inclusive).
func (s EpochSchedule) EpochLimits(epoch uint64) (uint64, uint64) {
	start := s.FirstSlotInEpoch(epoch)
	return start, start + s.SlotsInEpoch(epoch) - 1
}

var current atomic.Pointer[EpochSchedule]

func init() {
	SetEpochSchedule(MainnetEpochSchedule)
}

// SetEpochSchedule sets the process-wide epoch schedule used by the package-level
// functions. Call it at startup, before any work that depends on epoch boundaries.
func SetEpochSchedule(s EpochSchedule) {
	current.Store(&s)
}

// CurrentEpochSchedule returns the process-wide epoch schedule (mainnet by default).
func CurrentEpochSchedule() EpochSchedule {
	return *current.Load()
}

// CalcEpochLimits returns the first and last slot of the epoch under the current schedule.
func CalcEpochLimits(epoch uint64) (uint64, uint64) {
	return CurrentEpochSchedule().EpochLimits(epoch)
}

// CalcEpochForSlot returns the epoch for the given slot under the current schedule.
func CalcEpochForSlot(slot uint64) uint64 {
	return CurrentEpochSchedule().EpochForSlot(slot)
}

func GetEpochFromSlot(slot uint64) uint64 {
	return CalcEpochForSlot(slot)
}
