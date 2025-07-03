package ledger_bigtable

import (
	"bytes"
	"fmt"

	"cloud.google.com/go/bigtable"
	"google.golang.org/protobuf/proto"

	binc "github.com/rpcpool/yellowstone-faithful/parse_legacy_transaction_status_meta"
	"github.com/rpcpool/yellowstone-faithful/third_party/solana_proto/confirmed_block"
)

const (
	compressionHeaderSize = 4
)

func SlotKey(slot uint64) string {
	return fmt.Sprintf("%016x", slot)
}

type ConfirmedBlockWrapper struct {
	// Serde bincode
	Bin *binc.StoredConfirmedBlock

	// Protobuf
	Proto *confirmed_block.ConfirmedBlock
}

func (cbw *ConfirmedBlockWrapper) IsEmpty() bool {
	return cbw.Bin == nil && cbw.Proto == nil
}

type BigTableDataEncoding string

const (
	BigTableDataEncodingProto   BigTableDataEncoding = "x:proto"
	BigTableDataEncodingBincode BigTableDataEncoding = "x:bin"
)

const MaxEncodingSize = len(BigTableDataEncodingProto)

func GetEncodingFromBytes(b []byte) (BigTableDataEncoding, int, error) {
	if bytes.HasPrefix(b, []byte(BigTableDataEncodingProto)) {
		return BigTableDataEncodingProto, len(BigTableDataEncodingProto), nil
	}
	if bytes.HasPrefix(b, []byte(BigTableDataEncodingBincode)) {
		return BigTableDataEncodingBincode, len(BigTableDataEncodingBincode), nil
	}
	return "", -1, fmt.Errorf("unknown encoding prefix in bytes: %s", b[:MaxEncodingSize])
}

func GetRawUncompressedBlock(row bigtable.Row) ([]byte, BigTableDataEncoding, error) {
	x := row["x"]
	for _, item := range x {
		if item.Column == "x:proto" || item.Column == "x:bin" {
			encoding := BigTableDataEncoding(item.Column)
			if len(item.Value) == 0 {
				return nil, encoding, fmt.Errorf("no value found for column %s", item.Column)
			}
			b, err := bigtableCompression(item.Value[0]).Uncompress(item.Value[compressionHeaderSize:])
			if err != nil {
				return nil, encoding, fmt.Errorf("failed to uncompress block: %w", err)
			}
			if len(b) == 0 {
				return nil, encoding, fmt.Errorf("uncompressed block is empty for column %s", item.Column)
			}
			return b, encoding, nil
		}
	}
	return nil, "", fmt.Errorf("no proto or bin column found in row")
}

func ParseProtoBlock(buf []byte) (*confirmed_block.ConfirmedBlock, error) {
	var block confirmed_block.ConfirmedBlock
	if err := proto.Unmarshal(buf, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal proto block: %w", err)
	}
	return &block, nil
}

func ParseBincodeBlock(buf []byte) (*binc.StoredConfirmedBlock, error) {
	block, err := binc.BincodeDeserializeStoredConfirmedBlock(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize bincode block: %w", err)
	}
	return block, nil
}

func ParseRow(row bigtable.Row) (*ConfirmedBlockWrapper, error) {
	var block confirmed_block.ConfirmedBlock
	x := row["x"]
	for _, item := range x {
		if item.Column == "x:proto" {
			b, err := bigtableCompression(item.Value[0]).Uncompress(item.Value[compressionHeaderSize:])
			if err != nil {
				return nil, fmt.Errorf("failed to uncompress block: %w", err)
			}

			if err := proto.Unmarshal(b, &block); err != nil {
				return nil, fmt.Errorf("failed to unmarshal block: %w", err)
			}
			return &ConfirmedBlockWrapper{
				Proto: &block,
			}, nil
		}
		if item.Column == "x:bin" {
			b, err := bigtableCompression(item.Value[0]).Uncompress(item.Value[compressionHeaderSize:])
			if err != nil {
				return nil, fmt.Errorf("failed to uncompress block: %w", err)
			}
			block, err := binc.BincodeDeserializeStoredConfirmedBlock(b)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize bincode block: %w", err)
			}

			return &ConfirmedBlockWrapper{
				Bin: block,
			}, nil
		}
	}

	return nil, fmt.Errorf("no proto message in row") // might be bincode?
}
