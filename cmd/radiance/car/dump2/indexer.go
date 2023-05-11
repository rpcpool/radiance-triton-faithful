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

	bin "github.com/gagliardetto/binary"
	"github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	"github.com/ipld/go-storethehash/store"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/indexcidtooffset"
	"go.firedancer.io/radiance/pkg/compactindex"
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

func getFileSize(path string) (uint64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

func carCountItems(carPath string) (uint64, error) {
	f, err := os.Open(carPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	rd, err := newCarReader(f)
	if err != nil {
		return 0, fmt.Errorf("failed to open car file: %w", err)
	}

	var count uint64
	for {
		_, _, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		count++
	}

	return count, nil
}

func CreateCompactIndex(ctx context.Context, carPath string, indexDir string) error {
	f, err := os.Open(carPath)
	if err != nil {
		return err
	}
	defer f.Close()

	rd, err := newCarReader(f)
	if err != nil {
		return fmt.Errorf("failed to open car file: %w", err)
	}

	klog.Infof("Getting car file size")
	targetFileSize, err := getFileSize(carPath)
	if err != nil {
		return fmt.Errorf("failed to get car file size: %w", err)
	}

	klog.Infof("Counting items in car file...")
	numItems, err := carCountItems(carPath)
	if err != nil {
		return fmt.Errorf("failed to count items in car file: %w", err)
	}
	klog.Infof("Found %d items in car file", numItems)

	klog.Infof("Creating builder with %d items and target file size %d", numItems, targetFileSize)
	c2o, err := compactindex.NewBuilder(
		"",
		uint(numItems),
		(targetFileSize),
	)
	if err != nil {
		return fmt.Errorf("failed to open index store: %w", err)
	}
	defer c2o.Close()
	totalOffset := uint64(0)
	{
		var buf bytes.Buffer
		if err = carv1.WriteHeader(rd.header, &buf); err != nil {
			return err
		}
		totalOffset = uint64(buf.Len())
	}
	numItemsIndexed := uint64(0)
	klog.Infof("Indexing...")
	for {
		c, sectionLength, err := rd.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		klog.Infof("key: %s, offset: %d", bin.FormatByteSlice(c.Bytes()), totalOffset)

		err = c2o.Insert(c.Bytes(), uint64(totalOffset))
		if err != nil {
			return fmt.Errorf("failed to put cid to offset: %w", err)
		}

		totalOffset += sectionLength

		numItemsIndexed++
		if numItemsIndexed%100_000 == 0 {
			fmt.Print(".")
		}
	}

	targetFile, err := os.Create(filepath.Join(indexDir, filepath.Base(carPath)+".index"))
	if err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}
	defer targetFile.Close()

	klog.Infof("Sealing index...")
	if err = c2o.Seal(ctx, targetFile); err != nil {
		return fmt.Errorf("failed to seal index: %w", err)
	}
	klog.Infof("Index created")
	return nil
}

func CreateIndexStore(ctx context.Context, carPath string, indexDir string) error {
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
