package iplddecoders

import (
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/ipld/ipldbindcode"
)

func DecodeEpoch(epochRaw []byte) (*ipldbindcode.Epoch, error) {
	var epoch ipldbindcode.Epoch
	got, err := ipld.Unmarshal(epochRaw, dagcbor.Decode, &epoch, ipldbindcode.Prototypes.Epoch.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Epoch node: %w", err)
	}
	_ = got
	return &epoch, nil
}

func DecodeSubset(subsetRaw []byte) (*ipldbindcode.Subset, error) {
	var subset ipldbindcode.Subset
	got, err := ipld.Unmarshal(subsetRaw, dagcbor.Decode, &subset, ipldbindcode.Prototypes.Subset.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Subset node: %w", err)
	}
	_ = got
	return &subset, nil
}

func DecodeBlock(blockRaw []byte) (*ipldbindcode.Block, error) {
	var block ipldbindcode.Block
	got, err := ipld.Unmarshal(blockRaw, dagcbor.Decode, &block, ipldbindcode.Prototypes.Block.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Block node: %w", err)
	}
	_ = got
	return &block, nil
}

func DecodeEntry(entryRaw []byte) (*ipldbindcode.Entry, error) {
	var entry ipldbindcode.Entry
	got, err := ipld.Unmarshal(entryRaw, dagcbor.Decode, &entry, ipldbindcode.Prototypes.Entry.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Entry node: %w", err)
	}
	_ = got
	return &entry, nil
}

func DecodeTransaction(transactionRaw []byte) (*ipldbindcode.Transaction, error) {
	var transaction ipldbindcode.Transaction
	got, err := ipld.Unmarshal(transactionRaw, dagcbor.Decode, &transaction, ipldbindcode.Prototypes.Transaction.Type())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Transaction node: %w", err)
	}
	_ = got
	return &transaction, nil
}
