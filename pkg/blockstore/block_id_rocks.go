//go:build !lite

package blockstore

import "fmt"

// GetBlockID returns the Alpenglow block id (double merkle root) of slot.
func (d *DB) GetBlockID(slot uint64) ([32]byte, error) {
	if d.CfDoubleMerkleMeta == nil {
		return [32]byte{}, fmt.Errorf("slot %d: ledger has no %s column", slot, CfDoubleMerkleMetaName)
	}
	key := MakeDoubleMerkleMetaKey(slot)
	opts := getReadOptions()
	defer opts.Destroy()
	got, err := d.DB.GetCF(opts, d.CfDoubleMerkleMeta, key[:])
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to get %s: %w", CfDoubleMerkleMetaName, err)
	}
	defer got.Free()
	if !got.Exists() {
		return [32]byte{}, fmt.Errorf("slot %d: no %s entry", slot, CfDoubleMerkleMetaName)
	}
	return ParseDoubleMerkleRoot(got.Data())
}
