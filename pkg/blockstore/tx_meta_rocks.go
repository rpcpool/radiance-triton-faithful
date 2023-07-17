//go:build !lite

package blockstore

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/linxGnu/grocksdb"
)

func FormatTxMetadataKey(slot uint64, sig solana.Signature) []byte {
	key := make([]byte, 80)
	// the first 8 bytes are empty; fill them with zeroes
	// copy(key[:8], []byte{0, 0, 0, 0, 0, 0, 0, 0})
	// then comes the signature
	copy(key[8:], sig[:])
	// then comes the slot
	binary.BigEndian.PutUint64(key[72:], slot)
	return key
}

func ParseTxMetadataKey(key []byte) (slot uint64, sig solana.Signature) {
	sig = solana.Signature{}
	copy(sig[:], key[8:72])
	slot = binary.BigEndian.Uint64(key[72:])
	return
}

var readOptionsPool = sync.Pool{
	New: func() interface{} {
		opts := grocksdb.NewDefaultReadOptions()
		opts.SetVerifyChecksums(false)
		opts.SetFillCache(false)
		return opts
	},
}

func getReadOptions() *grocksdb.ReadOptions {
	return readOptionsPool.Get().(*grocksdb.ReadOptions)
}

func putReadOptions(opts *grocksdb.ReadOptions) {
	readOptionsPool.Put(opts)
}

type TransactionStatusMetaWithRaw struct {
	// ParsedLatest *confirmed_block.TransactionStatusMeta
	// ParsedLegacy *parse_legacy_transaction_status_meta.TransactionStatusMeta
	Raw []byte
}

func (d *DB) GetTransactionMetas(keys ...[]byte) ([]*TransactionStatusMetaWithRaw, error) {
	opts := getReadOptions()
	defer putReadOptions(opts)
	got, err := d.DB.MultiGetCF(opts, d.CfTxStatus, keys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx meta: %w", err)
	}
	defer got.Destroy()
	result := make([]*TransactionStatusMetaWithRaw, len(keys))
	for i := range keys {
		// if got[i] == nil || got[i].Size() == 0 {
		// 	continue
		// }
		// TODO: what if got[i] is empty?

		metaBytes := got[i].Data()
		obj := &TransactionStatusMetaWithRaw{
			Raw: cloneBytes(metaBytes),
		}
		result[i] = obj

		runtime.SetFinalizer(obj, func(obj *TransactionStatusMetaWithRaw) {
			obj.Raw = nil
		})
	}
	return result, nil
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

func encodeSlotAsKey(slot uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key[0:8], slot)
	return key
}

func (d *DB) GetBlockTime(slot uint64) (uint64, error) {
	if d.CfBlockTime == nil {
		return 0, nil
	}
	key := encodeSlotAsKey(slot)
	opts := getReadOptions()
	defer putReadOptions(opts)
	got, err := d.DB.GetCF(opts, d.CfBlockTime, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get blockTime: %w", err)
	}
	defer got.Free()
	if got == nil || got.Size() == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(got.Data()[:8]), nil
}

func (d *DB) GetBlockHeight(slot uint64) (uint64, error) {
	if d.CfBlockHeight == nil {
		return 0, nil
	}
	key := encodeSlotAsKey(slot)
	opts := getReadOptions()
	defer putReadOptions(opts)
	got, err := d.DB.GetCF(opts, d.CfBlockHeight, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get blockHeight: %w", err)
	}
	defer got.Free()
	if got == nil || got.Size() == 0 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(got.Data()[:8]), nil
}

func (d *DB) GetRewards(slot uint64) ([]byte, error) {
	if d.CfRewards == nil {
		return make([]byte, 0), nil
	}
	opts := getReadOptions()
	defer putReadOptions(opts)

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, slot)

	got, err := d.DB.GetCF(opts, d.CfRewards, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get rewards: %w", err)
	}
	defer got.Free()
	if got == nil || got.Size() == 0 {
		return make([]byte, 0), nil
	}
	return cloneBytes(got.Data()), nil
}

func signatureFromKey(key []byte) solana.Signature {
	return solana.SignatureFromBytes(key[8:72])
}
