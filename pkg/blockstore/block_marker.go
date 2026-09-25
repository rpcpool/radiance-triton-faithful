package blockstore

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Alpenglow block markers, from agave entry/src/block_component.rs.
//
// A block component is either a Vec<Entry> or a block marker. A marker is encoded
// as an empty entry batch (u64 zero entry count) followed by a VersionedBlockMarker:
//
//	u16 version (1) | u8 variant | u16 len | payload (len bytes)
//
// The payload is the wincode-serialized versioned footer/header/etc.

type BlockMarkerVariant uint8

const (
	BlockMarkerFooter             BlockMarkerVariant = 0
	BlockMarkerHeader             BlockMarkerVariant = 1
	BlockMarkerUpdateParent       BlockMarkerVariant = 2
	BlockMarkerGenesisCertificate BlockMarkerVariant = 3
)

func (v BlockMarkerVariant) String() string {
	switch v {
	case BlockMarkerFooter:
		return "BlockFooter"
	case BlockMarkerHeader:
		return "BlockHeader"
	case BlockMarkerUpdateParent:
		return "UpdateParent"
	case BlockMarkerGenesisCertificate:
		return "GenesisCertificate"
	default:
		return fmt.Sprintf("BlockMarkerVariant(%d)", uint8(v))
	}
}

const (
	blockMarkerVersionV1  = 1
	blockMarkerHeaderSize = 2 + 1 + 2 // version + variant + len
)

var ErrInvalidBlockMarker = errors.New("invalid block marker")

// BlockMarker is a VersionedBlockMarker as found in the shred stream.
type BlockMarker struct {
	Variant BlockMarkerVariant
	// Payload is the length-prefixed inner value (versioned footer/header/...).
	Payload []byte
	// Raw is the full VersionedBlockMarker serialization, without the leading
	// u64 zero entry count. This is what gets archived.
	Raw []byte
}

// ParseBlockMarker parses a VersionedBlockMarker from the front of b (after the
// u64 zero entry count) and returns it along with the number of bytes consumed.
func ParseBlockMarker(b []byte) (*BlockMarker, int, error) {
	if len(b) < blockMarkerHeaderSize {
		return nil, 0, fmt.Errorf("%w: short header (%d bytes)", ErrInvalidBlockMarker, len(b))
	}
	if version := binary.LittleEndian.Uint16(b[0:2]); version != blockMarkerVersionV1 {
		return nil, 0, fmt.Errorf("%w: unsupported version %d", ErrInvalidBlockMarker, version)
	}
	variant := BlockMarkerVariant(b[2])
	if variant > BlockMarkerGenesisCertificate {
		return nil, 0, fmt.Errorf("%w: unknown variant %d", ErrInvalidBlockMarker, variant)
	}
	size := blockMarkerHeaderSize + int(binary.LittleEndian.Uint16(b[3:5]))
	if len(b) < size {
		return nil, 0, fmt.Errorf("%w: %s payload needs %d bytes, have %d",
			ErrInvalidBlockMarker, variant, size-blockMarkerHeaderSize, len(b)-blockMarkerHeaderSize)
	}
	return &BlockMarker{
		Variant: variant,
		Payload: b[blockMarkerHeaderSize:size],
		Raw:     b[:size],
	}, size, nil
}

// BlockFooterV1 holds the fixed fields of an Alpenglow block footer. The
// certificates that follow them are kept only in the raw marker bytes.
type BlockFooterV1 struct {
	BankHash               [32]byte
	BlockProducerTimeNanos uint64
	BlockUserAgent         []byte
}

// Footer decodes the leading fields of a BlockFooter marker.
func (m *BlockMarker) Footer() (*BlockFooterV1, error) {
	if m.Variant != BlockMarkerFooter {
		return nil, fmt.Errorf("%w: %s is not a footer", ErrInvalidBlockMarker, m.Variant)
	}
	p := m.Payload
	// u8 VersionedBlockFooter tag | bank_hash | u64 time | u8 len + user agent
	if len(p) < 1+32+8+1 {
		return nil, fmt.Errorf("%w: short footer (%d bytes)", ErrInvalidBlockMarker, len(p))
	}
	if p[0] != 1 {
		return nil, fmt.Errorf("%w: unsupported footer version %d", ErrInvalidBlockMarker, p[0])
	}
	var f BlockFooterV1
	copy(f.BankHash[:], p[1:33])
	f.BlockProducerTimeNanos = binary.LittleEndian.Uint64(p[33:41])
	uaLen := int(p[41])
	if len(p) < 42+uaLen {
		return nil, fmt.Errorf("%w: short footer user agent", ErrInvalidBlockMarker)
	}
	f.BlockUserAgent = p[42 : 42+uaLen]
	return &f, nil
}
