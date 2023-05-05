package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-storethehash/store"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/indexcidtooffset"
	"k8s.io/klog/v2"
)

func readHeader(br io.Reader) (*carv1.CarHeader, error) {
	hb, err := util.LdRead(bufio.NewReader(br))
	if err != nil {
		return nil, err
	}

	var ch carv1.CarHeader
	if err := cbor.DecodeInto(hb, &ch); err != nil {
		return nil, fmt.Errorf("invalid header: %v", err)
	}

	return &ch, nil
}

type carReader struct {
	br     *bufio.Reader
	header *carv1.CarHeader
}

func newCarReader(r io.Reader) (*carReader, error) {
	br := bufio.NewReader(r)
	ch, err := readHeader(br)
	if err != nil {
		return nil, err
	}

	if ch.Version != 1 {
		return nil, fmt.Errorf("invalid car version: %d", ch.Version)
	}

	if len(ch.Roots) == 0 {
		return nil, fmt.Errorf("empty car, no roots")
	}

	return &carReader{
		br:     br,
		header: ch,
	}, nil
}

func readNodeInfo(br *bufio.Reader) (cid.Cid, uint64, error) {
	sectionLen, ll, err := readSectionLength(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	cidLen, c, err := cid.CidFromReader(br)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	// Seek to the next section by skipping the block.
	// The section length includes the CID, so subtract it.
	remainingSectionLen := int64(sectionLen) - int64(cidLen)

	_, err = io.CopyN(io.Discard, br, remainingSectionLen)
	if err != nil {
		return cid.Cid{}, 0, err
	}

	return c, sectionLen + ll, nil
}

type byteReaderWithCounter struct {
	io.ByteReader
	Offset uint64
}

func (b *byteReaderWithCounter) ReadByte() (byte, error) {
	c, err := b.ByteReader.ReadByte()
	if err == nil {
		b.Offset++
	}
	return c, err
}

func readSectionLength(r *bufio.Reader) (uint64, uint64, error) {
	if _, err := r.Peek(1); err != nil { // no more blocks, likely clean io.EOF
		return 0, 0, err
	}

	br := byteReaderWithCounter{r, 0}
	l, err := binary.ReadUvarint(&br)
	if err != nil {
		if err == io.EOF {
			return 0, 0, io.ErrUnexpectedEOF // don't silently pretend this is a clean EOF
		}
		return 0, 0, err
	}

	if l > uint64(util.MaxAllowedSectionSize) { // Don't OOM
		return 0, 0, errors.New("malformed car; header is bigger than util.MaxAllowedSectionSize")
	}

	return l, br.Offset, nil
}

func (cr *carReader) Next() (cid.Cid, uint64, error) {
	c, offset, err := readNodeInfo(cr.br)
	if err != nil {
		return c, 0, err
	}

	return c, offset, nil
}

func isDirEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdir(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func CreateIndex(ctx context.Context, carPath string, indexDir string) error {
	f, err := os.Open(carPath)
	if err != nil {
		return err
	}
	defer f.Close()

	rd, err := newCarReader(f)
	if err != nil {
		klog.Exitf("Failed to open CAR: %s", err)
	}

	// Check if the index already exists.
	// If it does, we can skip indexing.
	empty, err := isDirEmpty(indexDir)
	if err != nil {
		return err
	}
	if !empty {
		klog.Infof("Index already exists, skipping indexing")
		return nil
	}

	c2o, err := indexcidtooffset.OpenStore(ctx,
		filepath.Join(indexDir, "index"),
		filepath.Join(indexDir, "data"),
		store.IndexBitSize(31),
	)
	if err != nil {
		return fmt.Errorf("failed to open index store: %w", err)
	}
	defer c2o.Close()
	c2o.Start()
	totalOffset := uint64(0)
	{
		var buf bytes.Buffer
		if err = carv1.WriteHeader(rd.header, &buf); err != nil {
			return err
		}
		totalOffset = uint64(buf.Len())
	}
	for {
		c, sectionLength, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		err = c2o.Put(ctx, c, uint64(totalOffset))
		if err != nil {
			return fmt.Errorf("failed to put cid to offset: %w", err)
		}

		totalOffset += sectionLength
	}
	return nil
}
