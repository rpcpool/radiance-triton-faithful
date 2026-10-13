package blockstore

import (
	"encoding/binary"
	"testing"

	"github.com/gagliardetto/solana-go"
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

	batches, markers, err := SplitMarkers(meta, entries)
	require.NoError(t, err)
	assert.Equal(t, []*blockmarker.Marker{entries[0].Marker, entries[2].Marker}, markers)
	assert.Len(t, batches, 3)
}

func TestDataShredsToEntries_UpdateParent(t *testing.T) {
	header := marker(blockmarker.VariantHeader, make([]byte, 41))
	shreds := []shred.Shred{
		dataShred(0, shred.FlagDataCompletePattern, header),
		dataShred(1, shred.FlagDataCompletePattern, entryBatch(1, 0x01)), // built on the abandoned parent
		dataShred(2, shred.FlagDataCompletePattern, marker(blockmarker.VariantUpdateParent, make([]byte, 41))),
		dataShred(3, shred.FlagDataCompletePattern, entryBatch(2, 0x02)),
		dataShred(4, shred.FlagDataCompletePattern, marker(blockmarker.VariantFooter, footerPayload([32]byte{9}, 42, "ua"))),
		dataShred(5, shred.FlagLastInSlotPattern, entryBatch(1, 0x03)),
	}
	meta := &SlotMeta{
		Consumed:          6,
		Received:          6,
		LastIndex:         5,
		EntryEndIndexes:   []uint32{0, 1, 2, 3, 4, 5},
		ReplayFecSetIndex: 2,
	}

	entries, err := DataShredsToEntries(meta, shreds)
	require.NoError(t, err)
	require.Len(t, entries, 5)
	// The header comes from before the replay index; the abandoned batch is dropped.
	assert.True(t, entries[0].BeforeReplay)
	assert.Equal(t, header[8:], entries[0].Marker.Raw)
	assert.Equal(t, shreds[0:1], entries[0].Shreds)
	assert.False(t, entries[1].BeforeReplay)
	assert.Equal(t, blockmarker.VariantUpdateParent, entries[1].Marker.Variant)
	assert.Equal(t, shreds[3:4], entries[2].Shreds)

	batches, markers, err := SplitMarkers(meta, entries)
	require.NoError(t, err)
	var variants []blockmarker.Variant
	for _, m := range markers {
		variants = append(variants, m.Variant)
	}
	assert.Equal(t, []blockmarker.Variant{blockmarker.VariantHeader, blockmarker.VariantUpdateParent, blockmarker.VariantFooter}, variants)
	// Batches line up with the replayed data-complete indexes.
	assert.Equal(t, []uint32{2, 3, 4, 5}, meta.ReplayEntryEndIndexes())
	require.Len(t, batches, 4)
	assert.Empty(t, batches[0])
	assert.Equal(t, uint64(2), batches[1][0].NumHashes)
	assert.Empty(t, batches[2])
	assert.Equal(t, uint64(1), batches[3][0].NumHashes)
}

func TestDataShredsToEntries_UnfinishedPrefix(t *testing.T) {
	batch := entryBatch(1, 0x01)
	shreds := []shred.Shred{
		dataShred(0, shred.FlagDataCompletePattern, marker(blockmarker.VariantHeader, make([]byte, 41))),
		dataShred(1, 0, batch[:10]), // abandoned mid-batch
		dataShred(2, shred.FlagDataCompletePattern, marker(blockmarker.VariantUpdateParent, make([]byte, 41))),
		dataShred(3, shred.FlagLastInSlotPattern, entryBatch(2, 0x02)),
	}
	meta := &SlotMeta{Consumed: 4, Received: 4, LastIndex: 3, EntryEndIndexes: []uint32{0, 2, 3}, ReplayFecSetIndex: 2}

	entries, err := DataShredsToEntries(meta, shreds)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.True(t, entries[0].BeforeReplay)
	assert.Equal(t, uint64(2), entries[2].Entries[0].NumHashes)
}

func TestValidateMarkerLayout(t *testing.T) {
	m := func(v blockmarker.Variant) Entries { return Entries{Marker: &blockmarker.Marker{Variant: v}} }
	batch := Entries{Entries: []shred.Entry{{NumHashes: 1}}}
	txBatch := Entries{Entries: []shred.Entry{{Txns: make([]solana.Transaction, 1)}}}
	var (
		header  = m(blockmarker.VariantHeader)
		footer  = m(blockmarker.VariantFooter)
		update  = m(blockmarker.VariantUpdateParent)
		genesis = m(blockmarker.VariantGenesisCertificate)
	)

	valid := map[string][]Entries{
		"minimal":        {header, footer},
		"full":           {header, genesis, batch, update, batch, footer, batch},
		"update parent":  {header, batch, update, batch, footer, batch},
		"genesis cert":   {header, genesis, footer, batch},
		"no final ticks": {header, batch, footer},
	}
	for name, entries := range valid {
		assert.NoError(t, ValidateMarkerLayout(entries), name)
	}

	invalid := map[string][]Entries{
		"no header":                     {batch, footer, batch},
		"two headers":                   {header, header, footer},
		"header not first":              {batch, header, footer},
		"no footer":                     {header, batch},
		"two footers":                   {header, footer, footer},
		"two update parents":            {header, update, batch, update, footer},
		"two genesis certs":             {header, genesis, genesis, footer},
		"genesis cert not after header": {header, batch, genesis, footer},
		"marker after footer":           {header, footer, update},
		"two batches after footer":      {header, footer, batch, batch},
		"transactions after footer":     {header, footer, txBatch},
	}
	for name, entries := range invalid {
		assert.ErrorIs(t, ValidateMarkerLayout(entries), blockmarker.ErrInvalid, name)
	}
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
