package blockstore

import (
	"encoding/binary"
	"testing"

	"github.com/rpcpool/yellowstone-faithful/blockmarker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.firedancer.io/radiance/pkg/shred"
)

// marker serializes a BlockComponent::BlockMarker: u64 zero entry count followed
// by a VersionedBlockMarker (u16 version | u8 variant | u16 len | payload).
func marker(variant blockmarker.Variant, payload []byte) []byte {
	b := make([]byte, 8, 8+5+len(payload))
	b = binary.LittleEndian.AppendUint16(b, 1)
	b = append(b, byte(variant))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(payload)))
	return append(b, payload...)
}

// footerPayload serializes VersionedBlockFooter::V1 with no certificates.
func footerPayload(bankHash [32]byte, timeNanos uint64, userAgent string) []byte {
	b := []byte{1}
	b = append(b, bankHash[:]...)
	b = binary.LittleEndian.AppendUint64(b, timeNanos)
	b = append(b, byte(len(userAgent)))
	b = append(b, userAgent...)
	return append(b, 0, 0, 0) // block_final_cert, skip_reward_cert, notar_reward_cert: None
}

// entryBatch serializes a Vec<Entry> with one tick entry.
func entryBatch(numHashes uint64, hash byte) []byte {
	b := binary.LittleEndian.AppendUint64(nil, 1)
	b = binary.LittleEndian.AppendUint64(b, numHashes)
	for i := 0; i < 32; i++ {
		b = append(b, hash)
	}
	return binary.LittleEndian.AppendUint64(b, 0)
}

func dataShred(index uint32, flags uint8, payload []byte) shred.Shred {
	var s shred.Shred
	s.CommonHeader.Index = index
	s.DataHeader.Flags = flags
	s.Payload = payload
	return s
}

func TestDataShredsToEntries_BlockMarkers(t *testing.T) {
	footer := marker(blockmarker.VariantFooter, footerPayload([32]byte{9}, 42, "ua"))
	shreds := []shred.Shred{
		dataShred(0, shred.FlagDataCompletePattern, marker(blockmarker.VariantHeader, make([]byte, 41))),
		dataShred(1, shred.FlagDataCompletePattern, entryBatch(100, 0xaa)),
		dataShred(2, shred.FlagLastInSlotPattern, append(footer, make([]byte, 32)...)),
	}
	meta := &SlotMeta{Consumed: 3, Received: 3, LastIndex: 2, EntryEndIndexes: []uint32{0, 1, 2}}

	entries, err := DataShredsToEntries(meta, shreds)
	require.NoError(t, err)
	require.Len(t, entries, 3)

	require.NotNil(t, entries[0].Marker)
	assert.Equal(t, blockmarker.VariantHeader, entries[0].Marker.Variant)
	assert.Empty(t, entries[0].Entries)

	assert.Nil(t, entries[1].Marker)
	require.Len(t, entries[1].Entries, 1)
	assert.Equal(t, uint64(100), entries[1].Entries[0].NumHashes)

	require.NotNil(t, entries[2].Marker)
	assert.Equal(t, footer[8:], entries[2].Marker.Raw)
	assert.Equal(t, shreds[2:3], entries[2].Shreds)

	markers := []*blockmarker.Marker{entries[0].Marker, entries[2].Marker}
	got, err := BlockFooterMarker(markers)
	require.NoError(t, err)
	assert.Same(t, entries[2].Marker, got)

	_, err = BlockFooterMarker([]*blockmarker.Marker{got, got})
	assert.ErrorIs(t, err, blockmarker.ErrInvalid)
}

func TestDataShredsToEntries_UpdateParentSkipsPrefix(t *testing.T) {
	shreds := []shred.Shred{
		dataShred(0, shred.FlagDataCompletePattern, entryBatch(1, 0x01)), // built on the abandoned parent
		dataShred(1, shred.FlagDataCompletePattern, marker(blockmarker.VariantUpdateParent, make([]byte, 41))),
		dataShred(2, shred.FlagLastInSlotPattern, entryBatch(2, 0x02)),
	}
	meta := &SlotMeta{
		Consumed:          3,
		Received:          3,
		LastIndex:         2,
		EntryEndIndexes:   []uint32{0, 1, 2},
		ReplayFecSetIndex: 1,
	}

	entries, err := DataShredsToEntries(meta, shreds)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.NotNil(t, entries[0].Marker)
	assert.Equal(t, blockmarker.VariantUpdateParent, entries[0].Marker.Variant)
	require.Len(t, entries[1].Entries, 1)
	assert.Equal(t, uint64(2), entries[1].Entries[0].NumHashes)
	assert.Equal(t, shreds[2:3], entries[1].Shreds)

	assert.Equal(t, []uint32{1, 2}, meta.ReplayEntryEndIndexes())
}

func TestDecodeSlotMetaAuto_V3(t *testing.T) {
	v2 := func() []byte {
		var b []byte
		for _, v := range []uint64{77, 3, 3, 1000, 2, 76} { // slot, consumed, received, ts, last_index, parent_slot
			b = binary.LittleEndian.AppendUint64(b, v)
		}
		b = binary.LittleEndian.AppendUint64(b, 0) // next_slots
		b = append(b, 1)                           // connected_flags
		b = binary.LittleEndian.AppendUint64(b, completedIndexesBitVecBytes)
		bits := make([]byte, completedIndexesBitVecBytes)
		bits[0] = 0b101 // data complete at shreds 0 and 2
		return append(b, bits...)
	}

	meta, ver, err := DecodeSlotMetaAuto(v2())
	require.NoError(t, err)
	assert.Equal(t, SlotMetaV2, ver)
	assert.Zero(t, meta.ReplayFecSetIndex)
	assert.Equal(t, meta.EntryEndIndexes, meta.ReplayEntryEndIndexes())

	parentBlockID := [32]byte{0xde, 0xad}
	b := append(v2(), parentBlockID[:]...)
	b = binary.LittleEndian.AppendUint32(b, 1)
	meta, ver, err = DecodeSlotMetaAuto(b)
	require.NoError(t, err)
	assert.Equal(t, SlotMetaV3, ver)
	assert.Equal(t, uint64(76), meta.ParentSlot)
	assert.Equal(t, parentBlockID, meta.ParentBlockID)
	assert.Equal(t, uint32(1), meta.ReplayFecSetIndex)
	assert.Equal(t, []uint32{0, 2}, meta.EntryEndIndexes)
	assert.Equal(t, []uint32{2}, meta.ReplayEntryEndIndexes())
}
