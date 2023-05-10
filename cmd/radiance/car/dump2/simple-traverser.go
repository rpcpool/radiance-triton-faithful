package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car"
	carv2 "github.com/ipld/go-car/v2"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/indexcidtooffset"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/ipld/ipldbindcode"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/iplddecoders"
	"k8s.io/klog/v2"
)

func openCarReaderWithCidIndex(carPath string, indexDir string) (*carv2.Reader, *indexcidtooffset.Store, error) {
	// check if index file exists.
	if empty, err := isDirEmpty(indexDir); empty {
		klog.Infof("Index %s does not exist, creating...", indexDir)
		err := CreateCompactIndex(context.Background(), carPath, indexDir)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create index: %w", err)
		}
	} else if err != nil {
		return nil, nil, fmt.Errorf("failed to stat index file: %w", err)
	} else {
		klog.Infof("Index file %s exists", indexDir)
	}

	cr, err := carv2.OpenReader(carPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open CAR file: %w", err)
	}

	// Get root CIDs in the CARv1 file.
	roots, err := cr.Roots()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get roots: %w", err)
	}
	spew.Dump(roots)

	klog.Infof("Reading index from %s", indexDir)
	c2o, err := indexcidtooffset.OpenStore(context.Background(),
		filepath.Join(indexDir, "index"),
		filepath.Join(indexDir, "data"),
	)
	klog.Infof("Done reading index from %s", indexDir)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open index: %w", err)
	}
	c2o.Start()
	return cr, c2o, nil
}

type SimpleIterator struct {
	id *indexcidtooffset.Store
	cr *carv2.Reader
}

func NewSimpleIterator(carPath string, indexDir string) (*SimpleIterator, error) {
	cr, id, err := openCarReaderWithCidIndex(carPath, indexDir)
	if err != nil {
		return nil, err
	}
	return &SimpleIterator{
		id: id,
		cr: cr,
	}, nil
}

// Close closes the underlying blockstore.
func (t *SimpleIterator) Close() error {
	t.id.Close()
	return t.cr.Close()
}

func (t *SimpleIterator) Get(ctx context.Context, c cid.Cid) (*blocks.BasicBlock, error) {
	node, _, err := getRawNode(t.id.Get, t.cr, c)
	return node, err
}

func (t *SimpleIterator) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	_, size, err := getRawNode(t.id.Get, t.cr, c)
	return int(size), err
}

func (t *SimpleIterator) GetEpoch(ctx context.Context) (*ipldbindcode.Epoch, error) {
	roots, err := t.cr.Roots()
	if err != nil {
		return nil, fmt.Errorf("failed to get roots: %w", err)
	}
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root, got %d", len(roots))
	}
	epochRaw, err := t.Get(ctx, roots[0])
	if err != nil {
		return nil, fmt.Errorf("failed to get Epoch root: %w", err)
	}
	epoch, err := iplddecoders.DecodeEpoch(epochRaw.RawData())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Epoch root: %w", err)
	}
	return epoch, nil
}

func (t *SimpleIterator) FindSubsets(ctx context.Context, callback func(*ipldbindcode.Subset) error) error {
	dr, err := t.cr.DataReader()
	if err != nil {
		return fmt.Errorf("failed to get data reader: %w", err)
	}
	rd, err := car.NewCarReader(dr)
	if err != nil {
		return fmt.Errorf("failed to create car reader: %w", err)
	}
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		{
			if block.RawData()[1] != byte(iplddecoders.KindSubset) {
				continue
			}
			decoded, err := iplddecoders.DecodeSubset(block.RawData())
			if err != nil {
				continue
			}
			err = callback(decoded)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *SimpleIterator) FindBlocks(ctx context.Context, callback func(*ipldbindcode.Block) error) error {
	dr, err := t.cr.DataReader()
	if err != nil {
		return fmt.Errorf("failed to get data reader: %w", err)
	}
	rd, err := car.NewCarReader(dr)
	if err != nil {
		return fmt.Errorf("failed to create car reader: %w", err)
	}
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		{
			if block.RawData()[1] != byte(iplddecoders.KindBlock) {
				continue
			}
			decoded, err := iplddecoders.DecodeBlock(block.RawData())
			if err != nil {
				continue
			}
			err = callback(decoded)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *SimpleIterator) FindEntries(ctx context.Context, callback func(*ipldbindcode.Entry) error) error {
	dr, err := t.cr.DataReader()
	if err != nil {
		return fmt.Errorf("failed to get data reader: %w", err)
	}
	rd, err := car.NewCarReader(dr)
	if err != nil {
		return fmt.Errorf("failed to create car reader: %w", err)
	}
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		{
			if block.RawData()[1] != byte(iplddecoders.KindEntry) {
				continue
			}
			decoded, err := iplddecoders.DecodeEntry(block.RawData())
			if err != nil {
				continue
			}
			err = callback(decoded)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *SimpleIterator) FindTransactions(ctx context.Context, callback func(*ipldbindcode.Transaction) error) error {
	dr, err := t.cr.DataReader()
	if err != nil {
		return fmt.Errorf("failed to get data reader: %w", err)
	}
	rd, err := car.NewCarReader(dr)
	if err != nil {
		return fmt.Errorf("failed to create car reader: %w", err)
	}
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		{
			if block.RawData()[1] != byte(iplddecoders.KindTransaction) {
				continue
			}
			decoded, err := iplddecoders.DecodeTransaction(block.RawData())
			if err != nil {
				continue
			}
			err = callback(decoded)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
