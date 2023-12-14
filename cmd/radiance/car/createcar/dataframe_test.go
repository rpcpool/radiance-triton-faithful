package createcar

import (
	"fmt"
	"testing"

	bin "github.com/gagliardetto/binary"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/rpcpool/yellowstone-faithful/iplddecoders"
	"github.com/stretchr/testify/require"
)

func TestCreateAndStoreFrames(t *testing.T) {
	{
		data := []byte("hello world")

		store := newMemoryBlockstore(123, 122)

		first, err := CreateAndStoreFrames(store.Store, data, 5)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.NotNil(t, *first)
		require.Equal(t, 1, len(**first.Next))
		require.Equal(t, 0, **first.Index)
		require.Equal(t, int(iplddecoders.KindDataFrame), first.Kind)
		require.NotNil(t, first.Hash)
		require.NotNil(t, first.Index)
		require.NotNil(t, first.Total)
		require.NotNil(t, first.Data)
		require.Equal(t, 5, len(first.Data))
		require.Equal(t, []byte("hello"), first.Data)

		linkToSecond := **first.Next
		require.Equal(t, 1, len(linkToSecond))

		second, ok := store.getBlock(linkToSecond[0].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, second)
		{
			decoded, err := iplddecoders.DecodeDataFrame(second.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte(" world"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(second.Data))
		}
	}
	{
		data := []byte("abcdefghijklmnopqrstuvwxyz")

		store := newMemoryBlockstore(123, 122)

		first, err := CreateAndStoreFramesWithFrameSize(store.Store, data, 5, 5)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.NotNil(t, *first)
		require.Equal(t, 5, len(**first.Next))
		require.Equal(t, 0, **first.Index)
		require.Equal(t, int(iplddecoders.KindDataFrame), first.Kind)
		require.NotNil(t, first.Hash)
		require.NotNil(t, first.Index)
		require.NotNil(t, first.Total)
		require.NotNil(t, first.Data)
		require.Equal(t, 5, len(first.Data))
		require.Equal(t, []byte("abcde"), first.Data)

		linkToSecond := **first.Next
		require.Equal(t, 5, len(linkToSecond))

		second, ok := store.getBlock(linkToSecond[0].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, second)
		{
			decoded, err := iplddecoders.DecodeDataFrame(second.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("fghij"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(second.Data))
		}

		third, ok := store.getBlock(linkToSecond[1].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, third)
		{
			decoded, err := iplddecoders.DecodeDataFrame(third.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("klmno"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(third.Data))
		}

		fourth, ok := store.getBlock(linkToSecond[2].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, fourth)
		{
			decoded, err := iplddecoders.DecodeDataFrame(fourth.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("pqrst"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(fourth.Data))
		}

		fifth, ok := store.getBlock(linkToSecond[3].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, fifth)
		{
			decoded, err := iplddecoders.DecodeDataFrame(fifth.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("uvwxy"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(fifth.Data))
		}

		sixth, ok := store.getBlock(linkToSecond[4].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, sixth)
		{
			decoded, err := iplddecoders.DecodeDataFrame(sixth.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("z"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(sixth.Data))
		}
	}
	{
		dataGreek := []byte("abcdefghijklmnopqrstuvwxyzzyxwvutsrqponmlkjihgfedcba")

		store := newMemoryBlockstore(123, 122)

		first, err := CreateAndStoreFramesWithFrameSize(store.Store, dataGreek, 5, 5)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.NotNil(t, *first)
		require.Equal(t, 5, len(**first.Next))
		require.Equal(t, 0, **first.Index)
		require.Equal(t, int(iplddecoders.KindDataFrame), first.Kind)
		require.NotNil(t, first.Hash)
		require.NotNil(t, first.Index)
		require.NotNil(t, first.Total)
		require.NotNil(t, first.Data)
		require.Equal(t, 5, len(first.Data))
		require.Equal(t, []byte("abcde"), first.Data)

		linksToNext := **first.Next
		require.Equal(t, 5, len(linksToNext))

		second, ok := store.getBlock(linksToNext[0].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, second)
		{
			decoded, err := iplddecoders.DecodeDataFrame(second.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("fghij"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(second.Data))
			require.Equal(t, 0, len(**decoded.Next))
		}

		third, ok := store.getBlock(linksToNext[1].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, third)
		{
			decoded, err := iplddecoders.DecodeDataFrame(third.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("klmno"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(third.Data))
			require.Equal(t, 0, len(**decoded.Next))
		}

		fourth, ok := store.getBlock(linksToNext[2].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, fourth)
		{
			decoded, err := iplddecoders.DecodeDataFrame(fourth.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("pqrst"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(fourth.Data))
			require.Equal(t, 0, len(**decoded.Next))
		}

		fifth, ok := store.getBlock(linksToNext[3].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, fifth)
		{
			decoded, err := iplddecoders.DecodeDataFrame(fifth.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("uvwxy"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(fifth.Data))
			require.Equal(t, 0, len(**decoded.Next))
		}

		sixth, ok := store.getBlock(linksToNext[4].(cidlink.Link).Cid)
		require.True(t, ok)
		require.NotNil(t, sixth)
		{
			decoded, err := iplddecoders.DecodeDataFrame(sixth.Data)
			require.NoError(t, err)
			require.NotNil(t, decoded)
			require.Equal(t, []byte("zzyxw"), decoded.Data)
			fmt.Println(bin.FormatByteSlice(sixth.Data))
			require.Equal(t, 5, len(**decoded.Next))

			linksToNext = **decoded.Next
			require.Equal(t, 5, len(linksToNext))

			second, ok := store.getBlock(linksToNext[0].(cidlink.Link).Cid)
			require.True(t, ok)
			require.NotNil(t, second)
			{
				decoded, err := iplddecoders.DecodeDataFrame(second.Data)
				require.NoError(t, err)
				require.NotNil(t, decoded)
				require.Equal(t, []byte("vutsr"), decoded.Data)
				fmt.Println(bin.FormatByteSlice(second.Data))
				require.Equal(t, 0, len(**decoded.Next))
			}

			third, ok := store.getBlock(linksToNext[1].(cidlink.Link).Cid)
			require.True(t, ok)
			require.NotNil(t, third)
			{
				decoded, err := iplddecoders.DecodeDataFrame(third.Data)
				require.NoError(t, err)
				require.NotNil(t, decoded)
				require.Equal(t, []byte("qponm"), decoded.Data)
				fmt.Println(bin.FormatByteSlice(third.Data))
				require.Equal(t, 0, len(**decoded.Next))
			}

			fourth, ok := store.getBlock(linksToNext[2].(cidlink.Link).Cid)
			require.True(t, ok)
			require.NotNil(t, fourth)
			{
				decoded, err := iplddecoders.DecodeDataFrame(fourth.Data)
				require.NoError(t, err)
				require.NotNil(t, decoded)
				require.Equal(t, []byte("lkjih"), decoded.Data)
				fmt.Println(bin.FormatByteSlice(fourth.Data))
				require.Equal(t, 0, len(**decoded.Next))
			}

			fifth, ok := store.getBlock(linksToNext[3].(cidlink.Link).Cid)
			require.True(t, ok)
			require.NotNil(t, fifth)
			{
				decoded, err := iplddecoders.DecodeDataFrame(fifth.Data)
				require.NoError(t, err)
				require.NotNil(t, decoded)
				require.Equal(t, []byte("gfedc"), decoded.Data)
				fmt.Println(bin.FormatByteSlice(fifth.Data))
				require.Equal(t, 0, len(**decoded.Next))
			}

			sixth, ok := store.getBlock(linksToNext[4].(cidlink.Link).Cid)
			require.True(t, ok)
			require.NotNil(t, sixth)
			{
				decoded, err := iplddecoders.DecodeDataFrame(sixth.Data)
				require.NoError(t, err)
				require.NotNil(t, decoded)
				require.Equal(t, []byte("ba"), decoded.Data)
				fmt.Println(bin.FormatByteSlice(sixth.Data))
				require.Equal(t, 0, len(**decoded.Next))
			}
		}
	}
}

func TestCreateRawDataFrames(t *testing.T) {
	data := []byte("hello world")

	frames, err := CreateRawDataFrames(data, 5, 5)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, 3, len(frames))
	require.Equal(t, "hello", string(frames[0]))
	require.Equal(t, " worl", string(frames[1]))
	require.Equal(t, "d", string(frames[2]))

	frames, err = CreateRawDataFrames(data, 5, 4)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 3, len(frames))
	require.Equal(t, "hello", string(frames[0]))
	require.Equal(t, " wor", string(frames[1]))
	require.Equal(t, "ld", string(frames[2]))

	frames, err = CreateRawDataFrames(data, 5, 3)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 3, len(frames))
	require.Equal(t, "hello", string(frames[0]))
	require.Equal(t, " wo", string(frames[1]))
	require.Equal(t, "rld", string(frames[2]))
}

func TestsplitSlice(t *testing.T) {
	elements := []int{1, 2, 3, 4, 5, 6, 7, 8}

	{
		slices := splitSlice(elements, 3)
		require.Equal(t, 3, len(slices))
		require.Equal(t, []int{1, 2, 3}, slices[0])
		require.Equal(t, []int{4, 5, 6}, slices[1])
		require.Equal(t, []int{7, 8}, slices[2])
	}
	{
		slices := splitSlice(elements, 4)
		require.Equal(t, 2, len(slices))
		require.Equal(t, []int{1, 2, 3, 4}, slices[0])
		require.Equal(t, []int{5, 6, 7, 8}, slices[1])
	}
}
