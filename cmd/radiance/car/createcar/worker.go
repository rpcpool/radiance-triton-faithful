// Package cargen transforms blockstores into CAR files.
package createcar

import (
	"github.com/gagliardetto/solana-go"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/shred"
)

func transactionMetaKeysFromEntries(slot uint64, entries [][]shred.Entry) ([][]byte, error) {
	ln := 0
	for _, batch := range entries {
		for _, entry := range batch {
			ln += len(entry.Txns)
		}
	}
	keys := make([][]byte, ln)
	index := 0
	for _, batch := range entries {
		for _, entry := range batch {
			for _, tx := range entry.Txns {
				firstSig := tx.Signatures[0]
				keys[index] = blockstore.FormatTxMetadataKey(slot, firstSig)
				index++
			}
		}
	}
	return keys, nil
}

func transactionSignaturesFromEntries(slot uint64, entries [][]shred.Entry) ([]solana.Signature, error) {
	ln := 0
	for _, batch := range entries {
		for _, entry := range batch {
			ln += len(entry.Txns)
		}
	}
	keys := make([]solana.Signature, ln)
	index := 0
	for _, batch := range entries {
		for _, entry := range batch {
			for _, tx := range entry.Txns {
				firstSig := tx.Signatures[0]
				keys[index] = firstSig
				index++
			}
		}
	}
	return keys, nil
}
