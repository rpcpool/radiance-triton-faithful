package blockstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	bin "github.com/gagliardetto/binary"
	"go.firedancer.io/radiance/pkg/shred"
)

// MakeShredKey creates the RocksDB key for CfDataShred or CfCodeShred.
func MakeShredKey(slot, index uint64) (key [16]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	binary.BigEndian.PutUint64(key[8:16], index)
	return
}

// ParseShredKey decodes the RocksDB keys in CfDataShred or CfCodeShred.
func ParseShredKey(key []byte) (slot uint64, index uint64, ok bool) {
	ok = len(key) == 16
	if !ok {
		return
	}
	slot = binary.BigEndian.Uint64(key[0:8])
	index = binary.BigEndian.Uint64(key[8:16])
	return
}

type entryRange struct {
	startIdx, endIdx uint32 // inclusive
}

// entryRanges returns shred ranges for each *batch* (Vec<Entry> per batch).
func (s *SlotMeta) entryRanges() ([]entryRange, error) {
	if !s.IsFull() {
		return nil, nil
	}

	consumed := uint32(s.Consumed)
	if consumed == 0 {
		return nil, nil
	}

	// 1) Copy + filter to valid shred indexes we actually have [0, consumed)
	ends := make([]uint32, 0, len(s.EntryEndIndexes))
	for _, e := range s.EntryEndIndexes {
		if e < consumed {
			ends = append(ends, e)
		}
	}
	if len(ends) == 0 {
		// Full slot but no boundaries is suspicious; treat as error to avoid truncation.
		return nil, fmt.Errorf("slot %d: full but has no valid EntryEndIndexes (< consumed=%d)", s.Slot, consumed)
	}

	// 2) Sort boundaries (do not assume DB order)
	sort.Slice(ends, func(i, j int) bool { return ends[i] < ends[j] })

	// 3) Build ranges, enforcing monotonic, non-overlapping coverage.
	ranges := make([]entryRange, 0, len(ends))
	var begin uint32 = 0
	var prev uint32 = 0

	for i, end := range ends {
		if i > 0 && end <= prev {
			return nil, fmt.Errorf("slot %d: EntryEndIndexes not strictly increasing after sort? prev=%d end=%d", s.Slot, prev, end)
		}
		if end < begin {
			return nil, fmt.Errorf("slot %d: invalid range: begin=%d end=%d", s.Slot, begin, end)
		}
		ranges = append(ranges, entryRange{startIdx: begin, endIdx: end})
		begin = end + 1
		prev = end
	}

	// Optional: if you want to ensure the last boundary reaches the end of the slot’s
	// contiguous region, you can enforce:
	//
	// lastEnd := ends[len(ends)-1]
	// if lastEnd != consumed-1 {
	//     // Not necessarily fatal in all cases, but for "full slot" it's a great sanity check.
	//     return nil, fmt.Errorf("slot %d: last EntryEndIndex=%d does not match consumed-1=%d", s.Slot, lastEnd, consumed-1)
	// }

	return ranges, nil
}

type ordered interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 | uintptr |
		float32 | float64 |
		string
}

func sliceSortedByRange[T ordered](list []T, start T, stop T) []T {
	for len(list) > 0 && list[0] < start {
		list = list[1:]
	}
	for len(list) > 0 && list[len(list)-1] >= stop {
		list = list[:len(list)-1]
	}
	return list
}

type Entries struct {
	Entries []shred.Entry
	Raw     []byte
	Shreds  []shred.Shred
}

func (e *Entries) Slot() uint64 {
	if len(e.Shreds) == 0 {
		return math.MaxUint64
	}
	return e.Shreds[0].Slot
}

func isAllZero(b []byte) bool {
	// faster than looping in Go for large buffers
	return len(bytes.TrimRight(b, "\x00")) == 0
}

func trimLeadingZeros(b []byte) []byte {
	i := 0
	for i < len(b) && b[i] == 0 {
		i++
	}
	return b[i:]
}

func DataShredsToEntries(meta *SlotMeta, shreds []shred.Shred) ([]Entries, error) {
	var out []Entries
	var buf []byte

	// Which shred index begins the current undecoded buffer (coarse bookkeeping)
	startShred := 0

	for i := 0; i < len(shreds); i++ {
		// Append this shred's payload bytes
		buf = append(buf, shred.Concat([]shred.Shred{shreds[i]})...)

		for {
			// If buffer is only zeros, defer decision until we've appended more shreds.
			// (Or if this is the end, we'll accept it as padding.)
			buf = trimLeadingZeros(buf)
			if len(buf) == 0 {
				startShred = i + 1
				break
			}

			var dec bin.Decoder
			dec.SetEncoding(bin.EncodingBin)
			dec.Reset(buf)

			var sub SubEntries
			err := sub.UnmarshalWithDecoder(&dec)
			if err != nil {
				// Most common cause here is "need more bytes": stop inner loop,
				// append more shred bytes, try again.
				break
			}

			consumed := dec.Position()
			raw := buf[:consumed]

			// Associate with shreds [startShred..i]; if you need exact mapping when
			// a Vec<Entry> ends mid-shred, you'll need byte->shred accounting.
			parts := shreds[startShred : i+1]

			out = append(out, Entries{
				Entries: sub.Entries,
				Raw:     raw,
				Shreds:  parts,
			})

			buf = buf[consumed:]
			// next blob might still be in same shred if consumed < len(prev buf)
			// keep startShred as-is until we fully drain/shave buffer to empty
		}
	}

	// After processing all shreds: remaining bytes are OK iff they're all zero padding.
	buf = trimLeadingZeros(buf)
	if len(buf) != 0 && !isAllZero(buf) {
		return nil, fmt.Errorf("slot %d: trailing undecoded bytes=%d (non-zero)", meta.Slot, len(buf))
	}
	return out, nil
}

type SubEntries struct {
	Entries []shred.Entry
}

func (se *SubEntries) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	// read the number of entries:
	numEntries, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read number of entries: %w", err)
	}
	if numEntries > uint64(decoder.Remaining()) {
		return fmt.Errorf("not enough bytes to read %d entries", numEntries)
	}
	// read the entries:
	se.Entries = make([]shred.Entry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		if err = se.Entries[i].UnmarshalWithDecoder(decoder); err != nil {
			return fmt.Errorf("failed to read entry %d: %w", i, err)
		}
	}
	return
}

type ShredIndexV2 struct {
	Bits      []byte // length 4096 after padding
	NumShreds uint64
}

func DecodeShredIndexV2(b []byte) (ShredIndexV2, int, error) {
	r := newReader(b)

	// Vec<u8> length
	n, err := r.u64()
	if err != nil {
		return ShredIndexV2{}, 0, err
	}
	raw, err := r.bytes(int(n))
	if err != nil {
		return ShredIndexV2{}, 0, err
	}

	// num_shreds (usize) => treat as u64 for decoding
	num, err := r.u64()
	if err != nil {
		return ShredIndexV2{}, 0, err
	}

	// allow trailing? Rust uses reject_trailing_bytes for the collision tests.
	// For robustness in the wild, you *can* ignore trailing if you want, but
	// it’s better to reject for version detection.
	consumed := r.i

	buf := make([]byte, MaxDataShredsPerSlot/8) // 4096
	copy(buf, raw)

	return ShredIndexV2{Bits: buf, NumShreds: num}, consumed, nil
}
