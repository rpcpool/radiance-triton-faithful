package createcar

import (
	"bytes"
	"errors"
	"fmt"
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
	ha := hashBytes(data)

	rawFrames, err := CreateRawDataFrames(data, firstFrameSizeLimit, frameSizeLimit)
	if err != nil {
		return nil, err
	}

	total := len(rawFrames)

	frames := make([]*ipldbindcode.DataFrame, 0, len(rawFrames))
	for i, rawFrame := range rawFrames {
		bl := &ipldbindcode.DataFrame{
			Kind:  int(iplddecoders.KindDataFrame),
			Hash:  int(ha),
			Index: i,
			Total: total,
			Data:  rawFrame,
		}
		frames = append(frames, bl)
	}

	return frames, nil
}

func hashBytes(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

func CreateAndStoreFrames(
	store func(node datamodel.Node) (datamodel.Link, error),
	data []byte,
	firstFrameSizeLimit int,
) (*ipldbindcode.DataFrame, error) {
	frameSizeLimit := MaxObjectSize - 300
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
	revertSlice(rest)

	split := splitSlice(rest, NumNextLinks)

	previousLinks := make([]datamodel.Link, 0)
	for _, chunk := range split {
		links := make([]datamodel.Link, len(chunk))
		for j, frame := range chunk {
			reverseLinkSlice(previousLinks)
			frame.Next = previousLinks
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

	reverseLinkSlice(previousLinks)
	first.Next = previousLinks

	return first, nil
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
		qp.MapEntry(ma, "hash", qp.Int(int64(frame.Hash)))
		qp.MapEntry(ma, "index", qp.Int(int64(frame.Index)))
		qp.MapEntry(ma, "total", qp.Int(int64(frame.Total)))
		qp.MapEntry(ma, "data", qp.Bytes(frame.Data))
		qp.MapEntry(ma, "next",
			qp.List(-1, func(la datamodel.ListAssembler) {
				for _, link := range frame.Next {
					qp.ListEntry(la,
						qp.Link(link),
					)
				}
			}),
		)
	}
}

const (
	MaxObjectSize = 1 << 20 // 1 MiB
	NumNextLinks  = 5       // how many links to store in each dataFrame
)

func revertSlice(slice []*ipldbindcode.DataFrame) {
	for i := len(slice)/2 - 1; i >= 0; i-- {
		opp := len(slice) - 1 - i
		slice[i], slice[opp] = slice[opp], slice[i]
	}
}

func reverseLinkSlice(slice []datamodel.Link) {
	for i := len(slice)/2 - 1; i >= 0; i-- {
		opp := len(slice) - 1 - i
		slice[i], slice[opp] = slice[opp], slice[i]
	}
}

func splitSlice[T comparable](slice []T, size int) [][]T {
	var chunks [][]T
	for size < len(slice) {
		slice, chunks = slice[size:], append(chunks, slice[0:size:size])
	}
	return append(chunks, slice)
}
