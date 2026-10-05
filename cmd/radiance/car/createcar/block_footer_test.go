package createcar

import (
	"testing"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/rpcpool/yellowstone-faithful/iplddecoders"
	"github.com/stretchr/testify/require"
	radianceblockstore "go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/shred"
)

func TestConstructBlock_BlockFooter(t *testing.T) {
	slotMeta := &radianceblockstore.SlotMeta{
		Slot:       100,
		ParentSlot: 99,
		// batch 0: entries; batch 1: footer marker
		EntryEndIndexes: []uint32{3, 5},
	}
	entries := [][]shred.Entry{
		{{NumHashes: 1}, {NumHashes: 2}},
		{},
	}
	height := uint64(90)

	build := func(footer *radianceblockstore.BlockMarker) []byte {
		ms := newMemoryBlockstore(slotMeta.Slot, slotMeta.ParentSlot)
		link, err := constructBlock(ms, slotMeta, 1700000000, &height, entries, nil, nil, footer)
		require.NoError(t, err)
		block, ok := ms.getBlock(link.(cidlink.Link).Cid)
		require.True(t, ok)
		return block.Data
	}

	t.Run("without footer", func(t *testing.T) {
		block, err := iplddecoders.DecodeBlock(build(nil))
		require.NoError(t, err)
		_, ok := block.GetBlockFooter()
		require.False(t, ok)
		got, ok := block.GetBlockHeight()
		require.True(t, ok)
		require.Equal(t, height, got)
		// shredding maps the last entry of batch 0 to its data-complete shred
		require.Len(t, block.Shredding, 2)
		require.Equal(t, 3, block.Shredding[1].ShredEndIdx)
	})

	t.Run("with footer", func(t *testing.T) {
		raw := []byte{1, 0, 0, 3, 0, 1, 2, 3}
		block, err := iplddecoders.DecodeBlock(build(&radianceblockstore.BlockMarker{
			Variant: radianceblockstore.BlockMarkerFooter,
			Raw:     raw,
		}))
		require.NoError(t, err)
		got, ok := block.GetBlockFooter()
		require.True(t, ok)
		require.Equal(t, raw, got)
		h, ok := block.GetBlockHeight()
		require.True(t, ok)
		require.Equal(t, height, h)
	})
}
