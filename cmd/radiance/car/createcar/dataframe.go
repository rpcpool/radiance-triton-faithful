package createcar

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc64"
	"hash/fnv"
	"io"

	chunk "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/rpcpool/yellowstone-faithful/ipld/ipldbindcode"
	"github.com/rpcpool/yellowstone-faithful/iplddecoders"
)

var (
	ErrInvalidFirstFrameSizeLimit = errors.New("first frame size limit must be greater than 0")
	ErrInvalidFrameSizeLimit      = errors.New("frame size limit must be greater than 0")
)

func CreateRawDataFrames(
	data []byte,
	firstFrameSizeLimit int,
	frameSizeLimit int,
) ([][]byte, error) {
	if firstFrameSizeLimit < 1 {
		return nil, ErrInvalidFirstFrameSizeLimit
	}
	if frameSizeLimit < 1 {
		return nil, ErrInvalidFrameSizeLimit
	}

	frames := make([][]byte, 0)

	// the first frame is special, it has a different size limit
	if len(data) > firstFrameSizeLimit {
		frames = append(frames, data[:firstFrameSizeLimit])
		data = data[firstFrameSizeLimit:]
	} else {
		frames = append(frames, data)
		return frames, nil
	}

	reader := bytes.NewReader(data)

	// the rest of the frames have the same size limit
	splitter := chunk.NewSizeSplitter(reader, int64(frameSizeLimit))

	for {
		chunk, err := splitter.NextBytes()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		frames = append(frames, chunk)
	}

	return frames, nil
}

func CreateDataFrames(
	data []byte,
	firstFrameSizeLimit int,
	frameSizeLimit int,
) ([]*ipldbindcode.DataFrame, error) {
	ha := checksumCrc64(data)

	rawFrames, err := CreateRawDataFrames(data, firstFrameSizeLimit, frameSizeLimit)
	if err != nil {
		return nil, err
	}

	total := len(rawFrames)

	if total == 1 {
		bl := &ipldbindcode.DataFrame{
			Kind:  int(iplddecoders.KindDataFrame),
			Hash:  nil,
			Index: nil,
			Total: nil,
			Next:  nil,
			Data:  rawFrames[0],
		}
		return []*ipldbindcode.DataFrame{bl}, nil
	}

	frames := make([]*ipldbindcode.DataFrame, 0, len(rawFrames))
	for i, rawFrame := range rawFrames {
		bl := &ipldbindcode.DataFrame{
			Kind:  int(iplddecoders.KindDataFrame),
			Hash:  douplePointerInt(int(ha)),
			Index: douplePointerInt(i),
			Total: douplePointerInt(total),
			Data:  rawFrame,
		}
		frames = append(frames, bl)
	}

	return frames, nil
}

func douplePointerInt(i int) **int {
	return pointerToPointerInt(&i)
}

func pointerToPointerInt(i *int) **int {
	return &i
}

// checksumFnv is the legacy checksum function, used in the first version of the radiance
// car creator. Some old cars still use this function.
func checksumFnv(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// checksumCrc64 returns the hash of the provided buffer.
// It is used in the latest version of the radiance car creator.
func checksumCrc64(buf []byte) uint64 {
	return crc64.Checksum(buf, crc64.MakeTable(crc64.ISO))
}

func CreateAndStoreFrames(
	store func(node datamodel.Node) (datamodel.Link, error),
	data []byte,
	firstFrameSizeLimit int,
) (*ipldbindcode.DataFrame, error) {
	frameSizeLimit := MAX_BLOCK_SIZE - 300
	return CreateAndStoreFramesWithFrameSize(store, data, firstFrameSizeLimit, frameSizeLimit)
}

func CreateAndStoreFramesWithFrameSize(
	store func(node datamodel.Node) (datamodel.Link, error),
	data []byte,
	firstFrameSizeLimit int,
	frameSizeLimit int,
) (*ipldbindcode.DataFrame, error) {
	frames, err := CreateDataFrames(data, firstFrameSizeLimit, frameSizeLimit)
	if err != nil {
		return nil, err
	}

	// if there is only one frame, return it
	if len(frames) == 1 {
		return frames[0], nil
	}

	first := frames[0]
	// otherwise, link them together, backwards
	rest := frames[1:]
	reverse(rest)

	split := splitSlice(rest, NumNextLinks)

	previousLinks := make([]datamodel.Link, 0)
	for _, chunk := range split {
		links := make([]datamodel.Link, len(chunk))
		for j, frame := range chunk {
			reverse(previousLinks)
			frame.Next = douplePointerLinkSlice(previousLinks)
			dataFrameNode, err := frameToDatamodelNode(frame)
			if err != nil {
				return nil, fmt.Errorf("failed to build dataFrame node: %w", err)
			}
			// reset previous links
			previousLinks = make([]datamodel.Link, 0)

			link, err := store(dataFrameNode.(schema.TypedNode).Representation())
			if err != nil {
				return nil, err
			}
			links[j] = link
		}
		previousLinks = links
	}

	reverse(previousLinks)
	first.Next = douplePointerLinkSlice(previousLinks)

	return first, nil
}

func reverse[T any](x []T) {
	for i := len(x)/2 - 1; i >= 0; i-- {
		opp := len(x) - 1 - i
		x[i], x[opp] = x[opp], x[i]
	}
}

func douplePointerLinkSlice(l ipldbindcode.List__Link) **ipldbindcode.List__Link {
	return pointerToPointerLinkSlice(&l)
}

func pointerToPointerLinkSlice(l *ipldbindcode.List__Link) **ipldbindcode.List__Link {
	return &l
}

func frameToDatamodelNode(
	frame *ipldbindcode.DataFrame,
) (datamodel.Node, error) {
	return qp.BuildMap(ipldbindcode.Prototypes.DataFrame, -1, frameToDatamodelNodeAssembler(frame))
}

func frameToDatamodelNodeAssembler(
	frame *ipldbindcode.DataFrame,
) func(datamodel.MapAssembler) {
	return func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "kind", qp.Int(int64(iplddecoders.KindDataFrame)))
		if v, ok := frame.GetHash(); ok {
			qp.MapEntry(ma, "hash", qp.Int(int64(v)))
		}
		if v, ok := frame.GetIndex(); ok {
			qp.MapEntry(ma, "index", qp.Int(int64(v)))
		}
		if v, ok := frame.GetTotal(); ok {
			qp.MapEntry(ma, "total", qp.Int(int64(v)))
		}

		qp.MapEntry(ma, "data", qp.Bytes(frame.Data))

		if v, ok := frame.GetNext(); ok {
			qp.MapEntry(ma, "next",
				qp.List(-1, func(la datamodel.ListAssembler) {
					for _, link := range v {
						qp.ListEntry(la,
							qp.Link(link),
						)
					}
				}),
			)
		}
	}
}

const (
	NumNextLinks = 5 // how many links to store in each dataFrame
)

func splitSlice[T comparable](slice []T, size int) [][]T {
	var chunks [][]T
	for size < len(slice) {
		slice, chunks = slice[size:], append(chunks, slice[0:size:size])
	}
	return append(chunks, slice)
}
