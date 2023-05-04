//go:build !lite

package blockstore

import (
	"encoding/hex"
	"fmt"

	"github.com/linxGnu/grocksdb"
)

func GetBincode[T any](db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, key []byte) (*T, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	res, err := db.GetCF(opts, cf, key)
	if err != nil {
		return nil, err
	}
	if !res.Exists() {
		return nil, ErrNotFound
	}
	defer res.Free()
	return ParseBincode[T](res.Data())
}

func MultiGetBincode[T any](db *grocksdb.DB, cf *grocksdb.ColumnFamilyHandle, key ...[]byte) ([]*T, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	rows, err := db.MultiGetCF(opts, cf, key...)
	if err != nil {
		return nil, err
	}
	defer rows.Destroy()

	vals := make([]*T, len(rows))
	for i, row := range rows {
		val, err := ParseBincode[T](row.Data())
		if err != nil {
			fmt.Printf("cannot decode %s: %s", hex.EncodeToString(key[i]), err)
			return nil, err
		}
		vals[i] = val
	}

	return vals, nil
}
