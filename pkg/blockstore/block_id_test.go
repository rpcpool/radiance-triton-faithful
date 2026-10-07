package blockstore

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeDoubleMerkleMetaKey(t *testing.T) {
	key := MakeDoubleMerkleMetaKey(0x0102030405060708)
	assert.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, key[:8])
	assert.Equal(t, make([]byte, 32), key[8:], "Original block location")
}

func TestParseDoubleMerkleRoot(t *testing.T) {
	root := bytes.Repeat([]byte{0xab}, 32)
	value := append([]byte{}, root...)
	value = binary.LittleEndian.AppendUint32(value, 3) // fec_set_count
	value = binary.LittleEndian.AppendUint64(value, 4) // proofs len
	value = append(value, 1, 2, 3, 4)                  // proofs
	got, err := ParseDoubleMerkleRoot(value)
	require.NoError(t, err)
	assert.Equal(t, root, got[:])

	_, err = ParseDoubleMerkleRoot(root[:31])
	assert.Error(t, err)
}
