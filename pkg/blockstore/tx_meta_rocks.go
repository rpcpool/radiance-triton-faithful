//go:build !lite

package blockstore

import (
	"encoding/binary"
	"fmt"

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

func (d *DB) GetTransactionMetas(keys ...[]byte) ([]*confirmed_block.TransactionStatusMeta, error) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	got, err := d.DB.MultiGetCF(opts, d.CfTxStatus, keys...)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx meta: %w", err)
	}
	defer got.Destroy()
	result := make([]*confirmed_block.TransactionStatusMeta, len(keys))
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
		result[i] = txMeta
	}
	return result, nil
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
