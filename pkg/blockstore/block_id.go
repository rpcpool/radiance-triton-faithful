package blockstore

import (
	"encoding/binary"
	"fmt"
)

// CfDoubleMerkleMetaName is Agave's (4.3+) column of DoubleMerkleMeta, whose
// double_merkle_root is the Alpenglow block id. Older ledgers don't have it.
const CfDoubleMerkleMetaName = "double_merkle_meta"

// MakeDoubleMerkleMetaKey returns the CfDoubleMerkleMeta key of a slot's
// Original block location: u64 big-endian slot ++ 32 zero bytes.
func MakeDoubleMerkleMetaKey(slot uint64) (key [40]byte) {
	binary.BigEndian.PutUint64(key[0:8], slot)
	return
}

// ParseDoubleMerkleRoot returns the block id from a wincode DoubleMerkleMeta
// { double_merkle_root: Hash, fec_set_count: u32, proofs: Vec<u8> }.
// Like Agave's get_double_merkle_root, it reads only the leading hash.
func ParseDoubleMerkleRoot(value []byte) ([32]byte, error) {
	var root [32]byte
	if len(value) < len(root) {
		return root, fmt.Errorf("double_merkle_meta: value is %d bytes, want at least %d", len(value), len(root))
	}
	copy(root[:], value)
	return root, nil
}
