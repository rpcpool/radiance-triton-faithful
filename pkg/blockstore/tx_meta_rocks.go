//go:build !lite

package blockstore

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/golang/protobuf/proto"
	"github.com/linxGnu/grocksdb"
	metalatest "go.firedancer.io/radiance/cmd/radiance/car/createcar/parse_legacy_transaction_status_meta/v-latest"
	metaoldest "go.firedancer.io/radiance/cmd/radiance/car/createcar/parse_legacy_transaction_status_meta/v-oldest"
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
	for i, key := range keys {
		// if got[i] == nil || got[i].Size() == 0 {
		// 	continue
		// }
		// TODO: what if got[i] is empty?

		metaBytes := got[i].Data()
		txMeta, err := ParseAnyTransactionStatusMeta(metaBytes)
		if err != nil {
			debugSlot, debugSig := ParseTxMetadataKey(key)
			return nil, fmt.Errorf(
				"failed to parse tx meta for %s in slot %d: %w\n%s",
				debugSig,
				debugSlot,
				err,
				bin.FormatByteSlice(metaBytes),
			)
		}
		obj := &TransactionStatusMetaWithRaw{
			Raw: cloneBytes(metaBytes),
		}
		switch txMeta := txMeta.(type) {
		case *confirmed_block.TransactionStatusMeta:
		case *metalatest.TransactionStatusMeta:
		case *metaoldest.TransactionStatusMeta:
			// spew.Dump(debugSlot, debugSig.String(), txMeta)
		default:
			_ = txMeta
			panic("unreachable")
		}
		result[i] = obj

		runtime.SetFinalizer(obj, func(obj *TransactionStatusMetaWithRaw) {
			// obj.ParsedLatest = nil
			// obj.ParsedLegacy = nil
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

func (d *DB) GetRewards(slot uint64) (*confirmed_block.Rewards, error) {
	if d.CfRewards == nil {
		return nil, nil
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
		return nil, nil
	}
	return ParseRewards(got.Data())
}

func ParseRewards(buf []byte) (*confirmed_block.Rewards, error) {
	var rewards confirmed_block.Rewards
	err := proto.Unmarshal(buf, &rewards)
	if err != nil {
		return nil, err
	}
	return &rewards, nil
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

// From https://github.com/solana-labs/solana/blob/ce598c5c98e7384c104fe7f5121e32c2c5a2d2eb/transaction-status/src/lib.rs#L140-L147
func ParseLegacyTransactionStatusMeta(buf []byte) (*metalatest.TransactionStatusMeta, error) {
	legacyStatus, err := metalatest.BincodeDeserializeTransactionStatusMeta(buf)
	if err != nil {
		return nil, err
	}
	return &legacyStatus, nil
}

// From https://github.com/solana-labs/solana/blob/b7b4aa5d4d34ebf3fd338a64f4f2a5257b047bb4/transaction-status/src/lib.rs#L22-L27
func ParseLegacyTransactionStatusMetaOldest(buf []byte) (*metaoldest.TransactionStatusMeta, error) {
	legacyStatus, err := metaoldest.BincodeDeserializeTransactionStatusMeta(buf)
	if err != nil {
		return nil, err
	}
	return &legacyStatus, nil
}

func ParseAnyTransactionStatusMeta(buf []byte) (any, error) {
	// try to parse as protobuf (latest format)
	status, err := ParseTransactionStatusMeta(buf)
	if err == nil {
		return status, nil
	}
	// try to parse as legacy serde format (last serde format used by solana)
	status2, err := ParseLegacyTransactionStatusMeta(buf)
	if err == nil {
		return status2, nil
	}
	// try to parse as legacy serde format (probably the oldest serde format used by solana)
	status1, err := ParseLegacyTransactionStatusMetaOldest(buf)
	if err == nil {
		return status1, nil
	}
	return nil, fmt.Errorf("failed to parse tx meta: %w", err)
}
