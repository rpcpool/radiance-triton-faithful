package createcar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
