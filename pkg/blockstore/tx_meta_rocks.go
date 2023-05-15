//go:build !lite

package blockstore

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	"github.com/gagliardetto/solana-go"
	"github.com/golang/protobuf/proto"
	"github.com/linxGnu/grocksdb"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
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
	Parsed *confirmed_block.TransactionStatusMeta
	Raw    []byte
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
	for i, key := range keys {
		// if got[i] == nil || got[i].Size() == 0 {
		// 	continue
		// }
		// TODO: what if got[i] is empty?
		metaBytes := got[i].Data()
		txMeta, err := ParseTransactionStatusMeta(metaBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tx meta for %s: %w", signatureFromKey(key), err)
		}
		obj := &TransactionStatusMetaWithRaw{
			Parsed: txMeta,
			Raw:    cloneBytes(metaBytes),
		}
		result[i] = obj

		runtime.SetFinalizer(obj, func(obj *TransactionStatusMetaWithRaw) {
			obj.Parsed = nil
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

func (d *DB) GetBlockTime(key []byte) (uint64, error) {
	if d.CfBlockTime == nil {
		return 0, nil
	}
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

func signatureFromKey(key []byte) solana.Signature {
	return solana.SignatureFromBytes(key[8:72])
}

func ParseTransactionStatusMeta(buf []byte) (*confirmed_block.TransactionStatusMeta, error) {
	var status confirmed_block.TransactionStatusMeta
	err := proto.Unmarshal(buf, &status)
	if err != nil {
		return nil, err
	}
	return &status, nil
}
