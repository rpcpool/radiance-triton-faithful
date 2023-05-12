// Package car implements the CARv1 file format.
package car

import (
	"bytes"
	"fmt"
	"hash"
	"sync"

	"github.com/minio/sha256-simd"

	// "crypto/sha256"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/multiformats/go-varint"
)

var (
	// DummyCID is the "zero-length "identity" multihash with "raw" codec".
	//
	// This is the best-practices placeholder value to refer to a non-existent or unknown object.
	DummyCID = cid.MustParse("bafkqaaa")
	// CBOR_SHA256_DUMMY_CID is the CID of the CBOR multicodec with SHA2-256 multihash.
	//
	// This is the default CID used when CAR creation starts (before we know the actual root CID
	// which is the Epoch CID).
	CBOR_SHA256_DUMMY_CID = cid.MustParse("bafyreics5uul5lbtxslcigtoa5fkba7qgwu7cyb7ih7z6fzsh4lgfgraau")
)

// Block is a length-cid-data tuple.
// These make up most of CARv1.
//
// See https://ipld.io/specs/transport/car/carv1/#data
type Block struct {
	Length int
	Cid    cid.Cid
	Data   []byte
}

// NewBlockFromRaw creates a new CIDv1 with the given multicodec contentType on the fly.
func NewBlockFromRaw(data []byte, contentType uint64) Block {
	// cidBuilder := cid.V1Builder{
	// 	Codec:  contentType,
	// 	MhType: uint64(multicodec.Sha2_256),
	// }
	// id, err := cidBuilder.Sum(data)
	id, err := sum(data, contentType)
	if err != nil {
		// Something is wrong with go-cid if this fails.
		panic("failed to construct CID: " + err.Error())
	}
	return Block{
		Length: id.ByteLen() + len(data),
		Cid:    id,
		Data:   data,
	}
}

func sum(data []byte, contentType uint64) (cid.Cid, error) {
	mhLen := -1
	MhType := uint64(multicodec.Sha2_256)
	hash, err := sumMultihash(data, MhType, mhLen)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(contentType, hash), nil
}

var sha256Pool = sync.Pool{
	New: func() interface{} {
		return sha256.New()
	},
}

func sha256PoolGet() hash.Hash {
	return sha256Pool.Get().(hash.Hash)
}

func sha256PoolPut(h hash.Hash) {
	h.Reset()
	sha256Pool.Put(h)
}

func sumMultihash(data []byte, code uint64, length int) (multihash.Multihash, error) {
	// Get the algorithm.
	hasher := sha256PoolGet()
	defer sha256PoolPut(hasher)

	// Feed data in.
	hasher.Write(data)

	return encodeHash(hasher, code, length)
}

// NOTE: copied from github.com/multiformats/go-multihash@v0.2.1/sum.go
func encodeHash(hasher hash.Hash, code uint64, length int) (multihash.Multihash, error) {
	// Compute final hash.
	//  A new slice is allocated.  FUTURE: see other comment below about allocation, and review together with this line to try to improve.
	sum := hasher.Sum(nil)

	// Deal with any truncation.
	//  Unless it's an identity multihash.  Those have different rules.
	if length < 0 {
		length = hasher.Size()
	}
	if len(sum) < length {
		return nil, multihash.ErrLenTooLarge
	}
	if length >= 0 {
		if code == multihash.IDENTITY {
			if length != len(sum) {
				return nil, fmt.Errorf("the length of the identity hash (%d) must be equal to the length of the data (%d)", length, len(sum))
			}
		}
		sum = sum[:length]
	}

	// Put the multihash metainfo bytes at the front of the buffer.
	//  FUTURE: try to improve allocations here.  Encode does several which are probably avoidable, but it's the shape of the Encode method arguments that forces this.
	return encodeMultihash(sum, code)
}

// encodeMultihash a hash digest along with the specified function code.
// Note: the length is derived from the length of the digest itself.
//
// The error return is legacy; it is always nil.
func encodeMultihash(buf []byte, code uint64) ([]byte, error) {
	// FUTURE: this function always causes heap allocs... but when used, this value is almost always going to be appended to another buffer (either as part of CID creation, or etc) -- should this whole function be rethought and alternatives offered?
	newBuf := make([]byte, varint.UvarintSize(code)+varint.UvarintSize(uint64(len(buf)))+len(buf))
	n := varint.PutUvarint(newBuf, code)
	n += varint.PutUvarint(newBuf[n:], uint64(len(buf)))

	copy(newBuf[n:], buf)
	return newBuf, nil
}

func NewBlockFromCBOR(node datamodel.Node, contentType uint64) (Block, error) {
	// TODO: This could be rewritten as zero-copy
	var buf bytes.Buffer
	if err := dagcbor.Encode(node, &buf); err != nil {
		return Block{}, err
	}
	return NewBlockFromRaw(buf.Bytes(), contentType), nil
}

// TotalLen returns the total length of the block, including the length prefix.
func (b Block) TotalLen() int {
	return leb128Len(uint64(b.Length)) + b.Length
}

// leb128Len is like len(leb128.FromUInt64(x)).
// But without an allocation, therefore should be preferred.
func leb128Len(x uint64) (n int) {
	n = 1
	for {
		x >>= 7
		if x == 0 {
			return
		}
		n++
	}
}
