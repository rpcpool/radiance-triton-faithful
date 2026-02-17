package blockstore

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	// From agave: MAX_DATA_SHREDS_PER_SLOT = 32_768
	MaxDataShredsPerSlot = 32_768

	// BitVec<MAX_DATA_SHREDS_PER_SLOT> uses Word=u8, so NUM_WORDS = 32768/8 = 4096
	completedIndexesBitVecBytes = MaxDataShredsPerSlot / 8 // 4096
)

// SlotMetaVersion tells you which on-disk schema was detected.
type SlotMetaVersion int

const (
	SlotMetaUnknown SlotMetaVersion = iota
	SlotMetaV1                      // legacy: is_connected: bool, completed_data_indexes: BTreeSet<u32>
	SlotMetaV2                      // current: connected_flags: u8, completed_data_indexes: BitVec<32768>
)

func (v SlotMetaVersion) String() string {
	switch v {
	case SlotMetaV1:
		return "v1"
	case SlotMetaV2:
		return "v2"
	default:
		return "unknown"
	}
}

// SlotMeta is your consumer-facing structure.
// We normalize both formats to the old fields you were using.
type SlotMeta struct {
	Slot                uint64
	Consumed            uint64
	Received            uint64
	FirstShredTimestamp uint64
	LastIndex           uint64 // None encoded as math.MaxUint64
	ParentSlot          uint64 // None encoded as math.MaxUint64
	NextSlots           []uint64

	// Normalized view:
	IsConnected     bool
	ConnectedFlags  uint8 // only meaningful for v2 (but we set it for v1 too)
	EntryEndIndexes []uint32
}

// DecodeSlotMetaAuto detects v1 vs v2 and decodes accordingly.
// It returns a normalized SlotMeta + the detected version.
func DecodeSlotMetaAuto(b []byte) (*SlotMeta, SlotMetaVersion, error) {
	// First decode common prefix shared by both versions.
	r := newReader(b)
	out := &SlotMeta{}

	var err error
	if out.Slot, err = r.u64(); err != nil {
		return nil, SlotMetaUnknown, err
	}
	if out.Consumed, err = r.u64(); err != nil {
		return nil, SlotMetaUnknown, err
	}
	if out.Received, err = r.u64(); err != nil {
		return nil, SlotMetaUnknown, err
	}
	if out.FirstShredTimestamp, err = r.u64(); err != nil {
		return nil, SlotMetaUnknown, err
	}
	// In Rust, last_index/parent_slot are Option<...> but serialized as u64::MAX sentinel.
	if out.LastIndex, err = r.u64(); err != nil {
		return nil, SlotMetaUnknown, err
	}
	if out.ParentSlot, err = r.u64(); err != nil {
		return nil, SlotMetaUnknown, err
	}

	// Vec<Slot>: bincode encodes length as u64 (LE) followed by elems.
	nNext, err := r.u64()
	if err != nil {
		return nil, SlotMetaUnknown, err
	}
	if nNext > 1_000_000 { // sanity guard
		return nil, SlotMetaUnknown, fmt.Errorf("slotmeta: insane next_slots len %d", nNext)
	}
	out.NextSlots = make([]uint64, int(nNext))
	for i := range out.NextSlots {
		out.NextSlots[i], err = r.u64()
		if err != nil {
			return nil, SlotMetaUnknown, err
		}
	}

	// Now we are at the fork:
	//  v1: is_connected: bool(u8) ; completed_data_indexes: BTreeSet<u32> (len:u64 + u32 items)
	//  v2: connected_flags: u8     ; completed_data_indexes: BitVec bytes     (len:u64 + bytes)
	//
	// This is ambiguous because both start with a single byte, then a u64 length.
	// But: v2 BitVec length is *bytes* and almost always 4096.
	//      v1 length is *count of u32s* and is usually much smaller than 4096.
	//
	// We use a robust heuristic:
	//   - Read the next byte (x), peek the following u64 (n)
	//   - If n == 4096 -> v2
	//   - Else try v1 if remaining bytes can exactly fit n*u32 and consume the buffer cleanly
	//   - Otherwise fall back to v2.
	x, err := r.u8()
	if err != nil {
		return nil, SlotMetaUnknown, err
	}

	n, err := r.peekU64()
	if err != nil {
		return nil, SlotMetaUnknown, err
	}

	// Fast-path v2: exact BitVec byte length
	if n == completedIndexesBitVecBytes {
		return decodeSlotMetaV2(out, x, r)
	}

	// Try v1 if it "fits well" (exact consumption, plausible lengths).
	if meta1, ok := tryDecodeSlotMetaV1(out, x, r.clone()); ok {
		return meta1, SlotMetaV1, nil
	}

	// Otherwise v2.
	return decodeSlotMetaV2(out, x, r)
}

// --- v1/v2 decoders ---

func tryDecodeSlotMetaV1(base *SlotMeta, isConnectedByte uint8, r *reader) (*SlotMeta, bool) {
	// v1: bool is encoded as a single byte 0/1 by bincode.
	if isConnectedByte != 0 && isConnectedByte != 1 {
		return nil, false
	}
	// now length
	n, err := r.u64()
	if err != nil {
		return nil, false
	}
	if n > MaxDataShredsPerSlot {
		return nil, false
	}

	need := int(n) * 4
	if need < 0 || r.remaining() != need {
		// "exact consumption" requirement makes this quite reliable.
		return nil, false
	}

	out := *base // copy
	out.IsConnected = isConnectedByte == 1
	// v2 uses CONNECTED bit (LSB) for compatibility; emulate that here.
	if out.IsConnected {
		out.ConnectedFlags = 0x01
	} else {
		out.ConnectedFlags = 0x00
	}

	out.EntryEndIndexes = make([]uint32, int(n))
	for i := range out.EntryEndIndexes {
		v, err := r.u32()
		if err != nil {
			return nil, false
		}
		out.EntryEndIndexes[i] = v
	}

	if r.remaining() != 0 {
		return nil, false
	}
	return &out, true
}

func decodeSlotMetaV2(base *SlotMeta, connectedFlags uint8, r *reader) (*SlotMeta, SlotMetaVersion, error) {
	// v2: connected_flags: u8
	out := *base // copy
	out.ConnectedFlags = connectedFlags
	out.IsConnected = (connectedFlags & 0x01) != 0 // CONNECTED bit is LSB for back-compat.

	// completed_data_indexes: BitVec<32768> serialized via serde_bytes:
	//   u64 byte_len + byte_len bytes.
	byteLen, err := r.u64()
	if err != nil {
		return nil, SlotMetaUnknown, err
	}
	// guard rails
	if byteLen > 1<<20 { // 1MB is far beyond expected 4096
		return nil, SlotMetaUnknown, fmt.Errorf("slotmeta v2: insane bitvec byte_len=%d", byteLen)
	}
	raw, err := r.bytes(int(byteLen))
	if err != nil {
		return nil, SlotMetaUnknown, err
	}
	if r.remaining() != 0 {
		// print warning but be permissive and ignore trailing bytes, as Rust's Deserialize does.
		fmt.Printf("slotmeta v2: warning: trailing bytes=%d\n", r.remaining())
		// return nil, SlotMetaUnknown, fmt.Errorf("slotmeta v2: trailing bytes=%d", r.remaining())
	}

	// Rust custom Deserialize allows shorter buffers and zero-fills to NUM_WORDS.
	// Do the same in Go.
	buf := make([]byte, completedIndexesBitVecBytes)
	copy(buf, raw)

	out.EntryEndIndexes = bitvecOnesToU32(buf)
	return &out, SlotMetaV2, nil
}

func bitvecOnesToU32(buf []byte) []uint32 {
	// buf length should be 4096. Each bit position i corresponds to shred index i.
	// We return increasing order.
	out := make([]uint32, 0, 256)
	for byteIdx, w := range buf {
		if w == 0 {
			continue
		}
		base := byteIdx * 8
		// little-endian bit numbering within byte:
		// Rust sets (1 << bit_idx) where bit_idx = idx & 7.
		for bit := 0; bit < 8; bit++ {
			if (w & (1 << uint(bit))) != 0 {
				idx := base + bit
				if idx < MaxDataShredsPerSlot {
					out = append(out, uint32(idx))
				}
			}
		}
	}
	return out
}

// --- utilities ---

type reader struct {
	b []byte
	i int
}

func newReader(b []byte) *reader { return &reader{b: b, i: 0} }
func (r *reader) clone() *reader { rr := *r; return &rr }
func (r *reader) remaining() int { return len(r.b) - r.i }

func (r *reader) need(n int) error {
	if n < 0 || r.i+n > len(r.b) {
		return ioEOF("unexpected EOF")
	}
	return nil
}

func (r *reader) u8() (uint8, error) {
	if err := r.need(1); err != nil {
		return 0, err
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}

func (r *reader) u32() (uint32, error) {
	if err := r.need(4); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(r.b[r.i : r.i+4])
	r.i += 4
	return v, nil
}

func (r *reader) u64() (uint64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(r.b[r.i : r.i+8])
	r.i += 8
	return v, nil
}

func (r *reader) peekU64() (uint64, error) {
	if err := r.need(8); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(r.b[r.i : r.i+8]), nil
}

func (r *reader) bytes(n int) ([]byte, error) {
	if err := r.need(n); err != nil {
		return nil, err
	}
	v := r.b[r.i : r.i+n]
	r.i += n
	return v, nil
}

type ioEOF string

func (e ioEOF) Error() string { return string(e) }

func (s *SlotMeta) IsFull() bool {
	if s.LastIndex == math.MaxUint64 {
		return false
	}
	return s.Consumed == s.LastIndex+1
}

// MakeSlotKey creates the RocksDB key for CfMeta, CfRoot.
func MakeSlotKey(slot uint64) (key [8]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	return
}

// ParseSlotKey decodes the RocksDB keys in CfMeta, CfRoot.
func ParseSlotKey(key []byte) (slot uint64, ok bool) {
	ok = len(key) == 8
	if !ok {
		return
	}
	slot = binary.BigEndian.Uint64(key)
	return
}
