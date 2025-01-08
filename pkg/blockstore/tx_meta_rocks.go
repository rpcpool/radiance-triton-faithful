//go:build !lite

package blockstore

import (
	"encoding/binary"
	"fmt"
	"runtime"

	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/linxGnu/grocksdb"
	"k8s.io/klog/v2"
)

func MakeTxMetadataKey(
	isNew bool,
	slot uint64,
	sig solana.Signature,
) []byte {
	if isNew {
		return MakeNewTxMetadataKey(slot, sig)
	}
	return MakeOldTxMetadataKey(slot, sig)
}

func MakeOldTxMetadataKey(slot uint64, sig solana.Signature) []byte {
	key := make([]byte, 80)
	// the first 8 bytes are empty; fill them with zeroes
	// copy(key[:8], []byte{0, 0, 0, 0, 0, 0, 0, 0})
	// then comes the signature
	copy(key[8:], sig[:])
	// then comes the slot
	binary.BigEndian.PutUint64(key[72:], slot)
	return key
}

// Somewhere around epoch 630, the key format changed.
func MakeNewTxMetadataKey(slot uint64, sig solana.Signature) []byte {
	// key is signature (64 bytes) + slot (8 bytes)
	key := make([]byte, 72)
	copy(key, sig[:])
	binary.BigEndian.PutUint64(key[64:], slot)
	return key
}

func ParseOldTxMetadataKey(key []byte) (slot uint64, sig solana.Signature) {
	if len(key) != 80 {
		panic(fmt.Sprintf("invalid old tx metadata key length: %d; expected 80; key: %s", len(key), bin.FormatByteSlice(key)))
	}
	sig = solana.Signature{}
	copy(sig[:], key[8:72])
	slot = binary.BigEndian.Uint64(key[72:])
	return
}

const (
	LengthOldTxMetadataKey = 80
	LengthNewTxMetadataKey = 72
	// SlotBoundaryTxMetadataKeyFormatChange is the slot where the tx metadata key format changed.
	SlotBoundaryTxMetadataKeyFormatChange = 273686799
)

func ParseTxMetadataKey(key []byte) (slot uint64, sig solana.Signature) {
	ln := len(key)
	if ln == 80 {
		return ParseOldTxMetadataKey(key)
	}
	if ln == 72 {
		return ParseNewTxMetadataKey(key)
	}
	panic(fmt.Sprintf("invalid tx metadata key length: %d; expected 80 or 72; key: %s", len(key), bin.FormatByteSlice(key)))
}

func ParseNewTxMetadataKey(key []byte) (slot uint64, sig solana.Signature) {
	// check if the key is in the new format (signature + slot)
	if len(key) != 72 {
		panic(fmt.Sprintf("invalid new tx metadata key length: %d; expected 72; key: %s", len(key), bin.FormatByteSlice(key)))
	}
	sig = solana.Signature{}
	copy(sig[:], key[:64])
	slot = binary.BigEndian.Uint64(key[64:])
	return
}

func extractSignatureFromKey(key []byte) solana.Signature {
	_, sig := ParseTxMetadataKey(key)
	return sig
}

var optRocksDBVerifyChecksums = false

func getReadOptions() *grocksdb.ReadOptions {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(optRocksDBVerifyChecksums)
	opts.SetFillCache(false)
	return opts
}

type TransactionStatusMetaWithRaw struct {
	// ParsedLatest *confirmed_block.TransactionStatusMeta
	// ParsedLegacy *parse_legacy_transaction_status_meta.TransactionStatusMeta
	Raw []byte
}

func (d *DB) GetTransactionMetas(
	allowNotFound bool,
	onNotFound func(key []byte),
	keys ...[]byte,
) ([]*TransactionStatusMetaWithRaw, error) {
	opts := getReadOptions()
	defer opts.Destroy()
	got, err := d.DB.MultiGetCF(opts, d.CfTxStatus, keys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx meta: %w", err)
	}
	defer got.Destroy()
	result := make([]*TransactionStatusMetaWithRaw, len(keys))
	for i := range keys {
		if got[i] == nil || got[i].Size() == 0 {
			if !allowNotFound {
				return nil, fmt.Errorf("failed to get tx meta: key not found %v", keys[i])
			}
		}

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

func (d *DB) GetTransactionMetasWithAlternativeSources(
	allowNotFound bool,
	slot uint64,
	onNotFound func(key []byte),
	alternativeSourceFuncs []func(slot uint64, sig solana.Signature) ([]byte, error),
	keys ...[]byte,
) ([]*TransactionStatusMetaWithRaw, error) {
	opts := getReadOptions()
	defer opts.Destroy()
	got, err := d.DB.MultiGetCF(opts, d.CfTxStatus, keys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx meta: %w", err)
	}
	defer got.Destroy()
	result := make([]*TransactionStatusMetaWithRaw, len(keys))
	for i := range keys {
		isNotFound := got[i] == nil || got[i].Size() == 0
		if isNotFound && onNotFound != nil {
			onNotFound(keys[i])
		}
		if isNotFound && len(alternativeSourceFuncs) > 0 {
			for _, fillerFunc := range alternativeSourceFuncs {
				sig := extractSignatureFromKey(keys[i])
				filler, err := fillerFunc(slot, sig)
				if filler != nil && err == nil {
					obj := &TransactionStatusMetaWithRaw{
						Raw: cloneBytes(filler),
					}
					result[i] = obj
					runtime.SetFinalizer(obj, func(obj *TransactionStatusMetaWithRaw) {
						obj.Raw = nil
					})
					break
				} else {
					if err != nil {
						klog.Errorf("failed to get tx meta from alternative source: %v", err)
						return nil, fmt.Errorf("failed to get tx meta from alternative source: %w", err)
					}
				}
			}
		}
		if !allowNotFound && isNotFound && result[i] == nil {
			// was not found in DB, and neither in alternative sources;
			// given that not found is not allowed, return an error
			return nil, fmt.Errorf("failed to get tx meta: key not found %v, signature %s", keys[i], extractSignatureFromKey(keys[i]))
		}

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
	defer opts.Destroy()
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

func (d *DB) GetBlockHeight(slot uint64) (*uint64, error) {
	if d.CfBlockHeight == nil {
		return nil, nil
	}
	key := encodeSlotAsKey(slot)
	opts := getReadOptions()
	defer opts.Destroy()
	got, err := d.DB.GetCF(opts, d.CfBlockHeight, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get blockHeight: %w", err)
	}
	defer got.Free()
	if got == nil || got.Size() == 0 {
		return nil, nil
	}
	value := binary.LittleEndian.Uint64(got.Data()[:8])
	return &value, nil
}

func (d *DB) GetRewards(slot uint64) ([]byte, error) {
	if d.CfRewards == nil {
		return make([]byte, 0), nil
	}
	opts := getReadOptions()
	defer opts.Destroy()

	key := encodeSlotAsKey(slot)
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
