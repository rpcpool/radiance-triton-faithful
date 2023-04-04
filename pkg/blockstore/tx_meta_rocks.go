//go:build !lite

package blockstore

import (
	"fmt"

	"github.com/gagliardetto/solana-go"
	"github.com/golang/protobuf/proto"
	"github.com/linxGnu/grocksdb"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
)

func (d *DB) GetTransactionMetas(keys ...[]byte) ([]*confirmed_block.TransactionStatusMeta, error) {
	got, err := d.DB.MultiGetCF(grocksdb.NewDefaultReadOptions(), d.CfTxStatus, keys...)
	if err != nil {
		panic(err)
	}
	defer got.Destroy()
	result := make([]*confirmed_block.TransactionStatusMeta, len(keys))
	for i, key := range keys {
		if got[i] == nil || got[i].Size() == 0 {
			continue
		}
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
