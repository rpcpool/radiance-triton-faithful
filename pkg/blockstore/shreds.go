package blockstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

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

type bufSeg struct {
	shredIdx int
	n        int
}

func isAllZero(b []byte) bool { return len(bytes.Trim(b, "\x00")) == 0 }

func dropFront(segs *[]bufSeg, k int) {
	if k <= 0 {
		return
	}
	s := *segs
	for k > 0 && len(s) > 0 {
		if k >= s[0].n {
			k -= s[0].n
			s = s[1:]
			continue
		}
		s[0].n -= k
		k = 0
	}
	*segs = s
}

func segRangeForBytes(segs []bufSeg, k int) (start, end int) {
	if len(segs) == 0 || k <= 0 {
		return -1, -1
	}
	start = segs[0].shredIdx
	end = segs[0].shredIdx
	rem := k
	for _, sg := range segs {
		if rem <= 0 {
			break
		}
		end = sg.shredIdx
		rem -= sg.n
	}
	return start, end
}

// DataShredsToEntries reconstructs Vec<Entry> batches robustly.
// Fixes the "zero consumption" panic by never trusting dec.Position() when Unmarshal succeeded;
// we compute consumed bytes ourselves and ignore empty/zero-length decodes.
func DataShredsToEntries(meta *SlotMeta, shredsIn []shred.Shred) ([]Entries, error) {
	if !meta.IsFull() {
		return nil, nil
	}
	if len(shredsIn) == 0 {
		return nil, nil
	}

	consumed := int(meta.Consumed)
	if consumed <= 0 || consumed > len(shredsIn) {
		consumed = len(shredsIn)
	}
	shreds := shredsIn[:consumed]

	var (
		out  []Entries
		buf  []byte
		segs []bufSeg
	)

	var dec bin.Decoder
	dec.SetEncoding(bin.EncodingBin)

	// Attempt to decode as many Vec<Entry> as possible from the front of buf.
	// If boundary==true, then any remaining bytes must be zero padding only.
	decodeMany := func(boundary bool, boundaryShredIdx int) error {
		for {
			if len(buf) == 0 {
				return nil
			}

			// Snapshot start offset (Decoder doesn't guarantee Position semantics)
			startOff := len(buf)

			dec.Reset(buf)
			var se SubEntries
			err := se.UnmarshalWithDecoder(&dec)
			if err != nil {
				// If misaligned, wait for more bytes unless boundary forces a decision.
				if !boundary {
					return nil
				}
				if isAllZero(buf) {
					buf = nil
					segs = nil
					return nil
				}
				return fmt.Errorf("slot %d: cannot decode Vec<Entry> at shreds ~[%d-%d]: %w",
					meta.Slot, segs[0].shredIdx, boundaryShredIdx, err)
			}

			// Compute consumedBytes without trusting dec.Position().
			// The gagliardetto/binary decoder tracks remaining; that is reliable.
			consumedBytes := startOff - dec.Remaining()
			if consumedBytes <= 0 {
				// Treat as "nothing decoded": if at boundary, require padding-only; otherwise wait.
				if boundary {
					if isAllZero(buf) {
						buf = nil
						segs = nil
						return nil
					}
					// If this triggers, it's because the decoder succeeded without consuming,
					// which should not happen; dump a short prefix for debugging.
					prefix := buf
					if len(prefix) > 64 {
						prefix = prefix[:64]
					}
					return fmt.Errorf("slot %d: decoded Vec<Entry> with zero consumption at shred %d; buf_prefix=%v",
						meta.Slot, boundaryShredIdx, prefix)
				}
				return nil
			}

			startShred, endShred := segRangeForBytes(segs, consumedBytes)
			if startShred < 0 || endShred < 0 || endShred >= len(shreds) {
				return fmt.Errorf("slot %d: shred mapping failed (start=%d end=%d) consumedBytes=%d",
					meta.Slot, startShred, endShred, consumedBytes)
			}

			raw := buf[:consumedBytes]
			out = append(out, Entries{
				Entries: se.Entries,
				Raw:     raw,
				Shreds:  shreds[startShred : endShred+1],
			})

			// Drop decoded bytes
			buf = buf[consumedBytes:]
			dropFront(&segs, consumedBytes)

			if boundary {
				if len(buf) == 0 || isAllZero(buf) {
					buf = nil
					segs = nil
					return nil
				}

				// If remaining isn't zero padding, it might be another Vec<Entry> start inside same shred.
				// Loop and decode again.
			}
		}
	}

	for i, s := range shreds {
		if len(s.Payload) > 0 {
			buf = append(buf, s.Payload...)
			segs = append(segs, bufSeg{shredIdx: i, n: len(s.Payload)})
		}

		if s.DataHeader.DataComplete() || s.DataHeader.LastInSlot() {
			if err := decodeMany(true, i); err != nil {
				return nil, err
			}
		} else {
			if err := decodeMany(false, i); err != nil {
				return nil, err
			}
		}
	}

	// End: allow only zero padding.
	if len(buf) != 0 && !isAllZero(buf) {
		// A last-resort: sometimes buf starts with junk; if it's obviously not a Vec prefix, ignore only if all padding.
		if strings.Trim(string(buf), "\x00") != "" {
			return nil, fmt.Errorf("slot %d: trailing undecoded non-zero bytes=%d", meta.Slot, len(buf))
		}
	}

	return out, nil
}

var ErrVecMisaligned = errors.New("vec<entry> misaligned/garbage prefix")

// hard guardrails to avoid treating random bytes as a Vec length
const (
	MaxEntriesPerBatch = 1_000_000 // absurdly high but finite; tune if you want
)

type SubEntries struct {
	Entries []shred.Entry
}

func (se *SubEntries) UnmarshalWithDecoder(decoder *bin.Decoder) (err error) {
	numEntries, err := decoder.ReadUint64(bin.LE)
	if err != nil {
		return fmt.Errorf("failed to read number of entries: %w", err)
	}

	// If numEntries is insane relative to remaining bytes, this is almost certainly
	// not aligned to a Vec<Entry> start (you are at padding/junk).
	rem := uint64(decoder.Remaining())
	if numEntries > rem || numEntries > MaxEntriesPerBatch {
		return fmt.Errorf("%w: numEntries=%d remaining=%d", ErrVecMisaligned, numEntries, rem)
	}

	se.Entries = make([]shred.Entry, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		if err = se.Entries[i].UnmarshalWithDecoder(decoder); err != nil {
			return fmt.Errorf("failed to read entry %d: %w", i, err)
		}
	}
	return nil
}

// Helpers for the resync scanner (fast header probe without allocating a Decoder)
func peekU64LE(b []byte) (uint64, bool) {
	if len(b) < 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(b[:8]), true
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
