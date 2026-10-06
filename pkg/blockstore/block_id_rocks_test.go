//go:build !lite

package blockstore

import (
	"bytes"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeLedger creates a RocksDB with the column families open() requires, plus extra.
func makeLedger(t *testing.T, extra ...string) (string, map[string]*grocksdb.ColumnFamilyHandle, *grocksdb.DB) {
	t.Helper()
	path := t.TempDir()
	names := append([]string{CfDefault, CfMeta, CfRoot, CfDataShred, CfCodeShred, CfTxStatus}, extra...)
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)
	cfOpts := make([]*grocksdb.Options, len(names))
	for i := range cfOpts {
		cfOpts[i] = grocksdb.NewDefaultOptions()
	}
	db, handles, err := grocksdb.OpenDbColumnFamilies(opts, path, names, cfOpts)
	require.NoError(t, err)
	byName := make(map[string]*grocksdb.ColumnFamilyHandle, len(names))
	for i, n := range names {
		byName[n] = handles[i]
	}
	return path, byName, db
}

func TestGetBlockID(t *testing.T) {
	path, cfs, w := makeLedger(t, CfDoubleMerkleMetaName)
	root := bytes.Repeat([]byte{0xcd}, 32)
	key := MakeDoubleMerkleMetaKey(42)
	value := append(append([]byte{}, root...), 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) // fec_set_count, empty proofs
	wo := grocksdb.NewDefaultWriteOptions()
	require.NoError(t, w.PutCF(wo, cfs[CfDoubleMerkleMetaName], key[:], value))
	w.Close()

	db, err := OpenReadOnly(path)
	require.NoError(t, err)
	defer db.Close()
	require.NotNil(t, db.CfDoubleMerkleMeta)

	got, err := db.GetBlockID(42)
	require.NoError(t, err)
	assert.Equal(t, root, got[:])

	_, err = db.GetBlockID(43)
	assert.ErrorContains(t, err, "no double_merkle_meta entry")
}

func TestGetBlockID_PreAlpenglowLedger(t *testing.T) {
	path, _, w := makeLedger(t)
	w.Close()

	db, err := OpenReadOnly(path)
	require.NoError(t, err, "a ledger without double_merkle_meta must still open")
	defer db.Close()
	assert.Nil(t, db.CfDoubleMerkleMeta)

	_, err = db.GetBlockID(42)
	assert.ErrorContains(t, err, "ledger has no double_merkle_meta column")
}
