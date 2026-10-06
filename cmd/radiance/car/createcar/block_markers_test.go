package createcar

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/rpcpool/yellowstone-faithful/blockmarker"
	"github.com/rpcpool/yellowstone-faithful/ipld/ipldbindcode"
	"github.com/rpcpool/yellowstone-faithful/iplddecoders"
	"github.com/stretchr/testify/require"
	radianceblockstore "go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/shred"
)

// testMarker serializes a block marker as found in the shred stream: u64 zero
// entry count, then u16 version | u8 variant | u16 len | payload.
func testMarker(variant blockmarker.Variant, payload []byte) []byte {
	b := make([]byte, 8, 8+5+len(payload))
	b = binary.LittleEndian.AppendUint16(b, 1)
	b = append(b, byte(variant))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(payload)))
	return append(b, payload...)
}

// testBatch serializes a Vec<Entry> with one tick entry.
func testBatch(numHashes uint64, hash byte) []byte {
	b := binary.LittleEndian.AppendUint64(nil, 1)
	b = binary.LittleEndian.AppendUint64(b, numHashes)
	b = append(b, bytes.Repeat([]byte{hash}, 32)...)
	return binary.LittleEndian.AppendUint64(b, 0)
}

func testShred(index uint32, flags uint8, payload []byte) shred.Shred {
	var s shred.Shred
	s.CommonHeader.Index = index
	s.DataHeader.Flags = flags
	s.Payload = payload
	return s
}

var (
	testHeader       = testMarker(blockmarker.VariantHeader, append([]byte{1}, make([]byte, 40)...))
	testUpdateParent = testMarker(blockmarker.VariantUpdateParent, append([]byte{1}, bytes.Repeat([]byte{7}, 40)...))
	testBlockID      = bytes.Repeat([]byte{0x42}, 32)
	testFooter       = testMarker(blockmarker.VariantFooter, append(append([]byte{1}, make([]byte, 40)...), 2, 'u', 'a', 0, 0, 0))
)

// buildTestBlock runs the CAR path for one slot (deshred, split markers,
// construct) and returns the decoded block and its raw CBOR.
func buildTestBlock(t *testing.T, meta *radianceblockstore.SlotMeta, shreds []shred.Shred, blockID []byte) (*ipldbindcode.Block, []byte) {
	t.Helper()
	mapping, err := radianceblockstore.DataShredsToEntries(meta, shreds)
	require.NoError(t, err)
	entries, markers, err := radianceblockstore.SplitMarkers(meta, mapping)
	require.NoError(t, err)

	height := uint64(90)
	ms := newMemoryBlockstore(meta.Slot, meta.ParentSlot)
	link, err := constructBlock(ms, meta, 1700000000, &height, entries, nil, nil, markers, blockID)
	require.NoError(t, err)
	raw, ok := ms.getBlock(link.(cidlink.Link).Cid)
	require.True(t, ok)
	block, err := iplddecoders.DecodeBlock(raw.Data)
	require.NoError(t, err)
	return block, raw.Data
}

// slotMetaLen returns the number of elements in the encoded SlotMeta tuple.
func slotMetaLen(t *testing.T, raw []byte) int64 {
	t.Helper()
	nb := basicnode.Prototype.Any.NewBuilder()
	require.NoError(t, dagcbor.Decode(nb, bytes.NewReader(raw)))
	meta, err := nb.Build().LookupByIndex(4)
	require.NoError(t, err)
	return meta.Length()
}

func shredEndIdxs(block *ipldbindcode.Block) []int {
	var out []int
	for _, s := range block.Shredding {
		out = append(out, s.ShredEndIdx)
	}
	return out
}

func TestConstructBlock_PreAlpenglow(t *testing.T) {
	meta := &radianceblockstore.SlotMeta{
		Slot: 100, ParentSlot: 99, Consumed: 3, Received: 3, LastIndex: 2,
		EntryEndIndexes: []uint32{0, 2},
	}
	batch := testBatch(2, 0xbb)
	block, raw := buildTestBlock(t, meta, []shred.Shred{
		testShred(0, shred.FlagDataCompletePattern, testBatch(1, 0xaa)),
		testShred(1, 0, batch[:20]),
		testShred(2, shred.FlagLastInSlotPattern, batch[20:]),
	}, nil)

	require.EqualValues(t, 3, slotMetaLen(t, raw))
	_, ok := block.GetBlockMarkers()
	require.False(t, ok)
	_, ok = block.GetBlockFooter()
	require.False(t, ok)
	_, ok = block.GetBlockID()
	require.False(t, ok)
	h, ok := block.GetBlockHeight()
	require.True(t, ok)
	require.Equal(t, uint64(90), h)
	require.Equal(t, []int{0, 2}, shredEndIdxs(block))
}

func TestConstructBlock_HeaderAndFooter(t *testing.T) {
	meta := &radianceblockstore.SlotMeta{
		Slot: 100, ParentSlot: 99, Consumed: 4, Received: 4, LastIndex: 3,
		EntryEndIndexes: []uint32{0, 1, 2, 3},
	}
	shreds := []shred.Shred{
		testShred(0, shred.FlagDataCompletePattern, testHeader),
		testShred(1, shred.FlagDataCompletePattern, testBatch(1, 0xaa)),
		testShred(2, shred.FlagDataCompletePattern, testFooter),
		testShred(3, shred.FlagLastInSlotPattern, append(testBatch(1, 0xcc), make([]byte, 16)...)),
	}
	block, raw := buildTestBlock(t, meta, shreds, testBlockID)

	require.EqualValues(t, 5, slotMetaLen(t, raw))
	markers, ok := block.GetBlockMarkers()
	require.True(t, ok)
	require.Equal(t, [][]byte{testHeader[8:], testFooter[8:]}, markers)
	footer, ok := block.GetBlockFooter()
	require.True(t, ok)
	require.Equal(t, testFooter[8:], footer)
	id, ok := block.GetBlockID()
	require.True(t, ok)
	require.Equal(t, testBlockID, id)
	// Marker batches stay in the batch list, so entries keep their data-complete index.
	require.Equal(t, []int{1, 3}, shredEndIdxs(block))

	// Without a block id the tuple stops at block_markers.
	block, raw = buildTestBlock(t, meta, shreds, nil)
	require.EqualValues(t, 4, slotMetaLen(t, raw))
	markers, ok = block.GetBlockMarkers()
	require.True(t, ok)
	require.Equal(t, [][]byte{testHeader[8:], testFooter[8:]}, markers)
	_, ok = block.GetBlockFooter()
	require.True(t, ok)
	_, ok = block.GetBlockID()
	require.False(t, ok)
}

func TestConstructBlock_UpdateParent(t *testing.T) {
	meta := &radianceblockstore.SlotMeta{
		Slot: 600, ParentSlot: 598, Consumed: 7, Received: 7, LastIndex: 6,
		EntryEndIndexes:   []uint32{0, 1, 2, 3, 4, 5, 6},
		ReplayFecSetIndex: 3,
	}
	block, _ := buildTestBlock(t, meta, []shred.Shred{
		testShred(0, shred.FlagDataCompletePattern, testHeader),
		testShred(1, shred.FlagDataCompletePattern, testBatch(3, 0x01)), // abandoned
		testShred(2, shred.FlagDataCompletePattern, testBatch(4, 0x02)), // abandoned
		testShred(3, shred.FlagDataCompletePattern, testUpdateParent),
		testShred(4, shred.FlagDataCompletePattern, testBatch(5, 0x03)),
		testShred(5, shred.FlagDataCompletePattern, testFooter),
		testShred(6, shred.FlagLastInSlotPattern, testBatch(1, 0x04)),
	}, testBlockID)

	markers, ok := block.GetBlockMarkers()
	require.True(t, ok)
	require.Equal(t, [][]byte{testHeader[8:], testUpdateParent[8:], testFooter[8:]}, markers)
	id, ok := block.GetBlockID()
	require.True(t, ok)
	require.Equal(t, testBlockID, id)
	// Only the replayed entries are archived, mapped to the same shreds as before.
	require.Len(t, block.Entries, 2)
	require.Equal(t, []int{4, 6}, shredEndIdxs(block))
}
