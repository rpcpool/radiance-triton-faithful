package shred

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

type Shred struct {
	CommonHeader
	// CodeHeader
	DataHeader
	Payload           []byte
	MerklePath        [][20]byte
	MerkleRoot        [32]byte // Only for chained shreds
	ResignedSignature [64]byte // Only for resigned shreds
}

const (
	LegacyCodeID    = uint8(0b0101_1010)
	LegacyDataID    = uint8(0b1010_0101)
	MerkleTypeMask  = uint8(0xF0)
	MerkleDepthMask = uint8(0x0F)
	MerkleCodeID    = uint8(0x40)
	MerkleDataID    = uint8(0x80)
)

const (
	FlagDataTickMask   = uint8(0b0011_1111)
	FlagDataEndOfBatch = uint8(0b0100_0000)
	FlagDataEndOfBlock = uint8(0b1100_0000)
)

const (
	RevisionV1 = 1
	RevisionV2 = 2
)

const (
	LegacyDataV1HeaderSize  = 86
	LegacyDataV2HeaderSize  = 88
	LegacyDataV1PayloadSize = 1057 // TODO where does this number come from?
)

// NewShredFromSerialized creates a shred object from the given buffer.
//
// The original slice may be deallocated after this function returns.
func NewShredFromSerialized(shred []byte, revision int) (s Shred) {
	if len(shred) < 88 {
		return
	}
	variant := shred[64]
	switch {
	case variant == LegacyCodeID:
		// s.loadCode()
		panic("todo legacy code shred")
	case variant == LegacyDataID:
		var payloadOff, payloadSize int
		switch revision {
		case 1:
			s.DataHeader.ParentOffset = binary.LittleEndian.Uint16(shred[0x53:0x55])
			s.DataHeader.Flags = shred[0x55]
			s.DataHeader.Size = LegacyDataV1HeaderSize + LegacyDataV1PayloadSize
			payloadOff = LegacyDataV1HeaderSize
			payloadSize = LegacyDataV1PayloadSize
		case 2:
			s.DataHeader.ParentOffset = binary.LittleEndian.Uint16(shred[0x53:0x55])
			s.DataHeader.Flags = shred[0x55]
			s.DataHeader.Size = binary.LittleEndian.Uint16(shred[0x56:0x58])
			payloadOff = LegacyDataV2HeaderSize
			payloadSize = int(s.DataHeader.Size) - LegacyDataV2HeaderSize
		default:
			panic(fmt.Sprintf("unsupported shred revision %d", revision))
		}
		if payloadSize < 0 {
			return
		}
		if len(shred) < int(s.DataHeader.Size) {
			return
		}
		s.Payload = make([]byte, payloadSize)
		copy(s.Payload, shred[payloadOff:payloadOff+payloadSize])
	case variant&MerkleTypeMask == MerkleCodeID:
		panic("todo merkle code shred")
		// return MerkleCodeFromPayload(shred)
	case variant&MerkleTypeMask == MerkleDataID:
		// 1. Extract metadata from variant byte
		merkleDepth := int(variant & MerkleDepthMask)
		chained := (variant & 0x20) != 0  // bit 5 indicates chained
		resigned := (variant & 0x10) != 0 // bit 4 indicates resigned

		// 2. Calculate component sizes
		merkleProofSize := merkleDepth * 20
		merkleRootSize := 0
		if chained {
			merkleRootSize = 32 // Size of SHA256 hash
		}
		signatureSize := 0
		if resigned {
			signatureSize = 64 // Size of ED25519 signature
		}

		// 3. Parse headers
		s.DataHeader.ParentOffset = binary.LittleEndian.Uint16(shred[0x53:0x55])
		s.DataHeader.Flags = shred[0x55]
		s.DataHeader.Size = binary.LittleEndian.Uint16(shred[0x56:0x58])

		// 4. Validate total size
		payloadOff := LegacyDataV2HeaderSize
		totalSize := int(s.DataHeader.Size) + merkleProofSize + signatureSize
		if len(shred) < totalSize {
			return // Invalid shred length
		}

		// 5. Extract payload components
		dataSize := int(s.DataHeader.Size) - LegacyDataV2HeaderSize - merkleRootSize - signatureSize
		s.Payload = make([]byte, dataSize)
		copy(s.Payload, shred[payloadOff:payloadOff+dataSize])

		// 6. Extract Merkle root (if chained)
		offset := payloadOff + dataSize
		if chained {
			// s.MerkleRoot = make([]byte, 32)
			// copy(s.MerkleRoot, shred[offset:offset+32])
			offset += 32
		}

		// 7. Extract Merkle proofs
		s.MerklePath = make([][20]byte, merkleDepth)
		for i := 0; i < merkleDepth; i++ {
			copy(s.MerklePath[i][:], shred[offset:offset+20])
			offset += 20
		}

		// 8. Handle resigned signature
		if resigned {
			// copy(s.ResignedSignature[:], shred[offset:offset+64])
			offset += 64
		}
	default:
		return
	}
	copy(s.Signature[:], shred[0x00:0x40])
	s.Variant = variant
	s.Slot = binary.LittleEndian.Uint64(shred[0x41:0x49])
	s.Index = binary.LittleEndian.Uint32(shred[0x49:0x4d])
	s.Version = binary.LittleEndian.Uint16(shred[0x4d:0x4f])
	s.FECSetIndex = binary.LittleEndian.Uint32(shred[0x4f:0x53])
	return
}

func (s Shred) MarshalYAML() (any, error) {
	merklePath := make([]string, len(s.MerklePath))
	for i, x := range s.MerklePath {
		merklePath[i] = hex.EncodeToString(x[:])
	}
	return struct {
		CommonHeader
		DataHeader
		Payload    string
		MerklePath []string `json:",omitempty"`
	}{
		CommonHeader: s.CommonHeader,
		DataHeader:   s.DataHeader,
		Payload:      base64.StdEncoding.EncodeToString(nil),
		MerklePath:   merklePath,
	}, nil
}

type CommonHeader struct {
	Signature   solana.Signature
	Variant     uint8
	Slot        uint64
	Index       uint32
	Version     uint16
	FECSetIndex uint32
}

func (c *CommonHeader) Ok() bool {
	return c.IsData() || c.IsCode()
}

func (c *CommonHeader) IsData() bool {
	return c.Variant == LegacyDataID || (c.Variant&MerkleTypeMask) == MerkleDataID
}

func (c *CommonHeader) IsCode() bool {
	return c.Variant == LegacyCodeID || (c.Variant&MerkleTypeMask) == MerkleCodeID
}

type DataHeader struct {
	ParentOffset uint16
	Flags        uint8
	Size         uint16
}

func (d *DataHeader) EndOfBlock() bool {
	return d.Flags&FlagDataEndOfBlock != 0
}

func (s *DataHeader) EndOfBatch() bool {
	return s.Flags&FlagDataEndOfBatch == 1
}

func (s *DataHeader) Tick() uint8 {
	return s.Flags & FlagDataTickMask
}
