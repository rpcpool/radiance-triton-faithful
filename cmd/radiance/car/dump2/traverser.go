package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/ipfs/boxo/ipld/car/v2/blockstore"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car/util"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/iplddecoders"
	"k8s.io/klog/v2"
)

type Traverser struct {
	id    index.Index
	cr    *carv2.Reader
	graph *CARDAG
}

func openCarReaderWithIndex(carPath string) (*carv2.Reader, index.Index, error) {
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

	indexPath := carPath + ".carindex"

	// check if index file exists.
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		klog.Infof("Index file %s does not exist, creating...", indexPath)
		// index file does not exist, create it.
		dr, err := cr.DataReader()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get data reader: %w", err)
		}
		idx, err := carv2.GenerateIndex(dr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate index: %w", err)
		}
		indexFile, err := os.Create(indexPath)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create index file: %w", err)
		}
		if _, err := index.WriteTo(idx, indexFile); err != nil {
			return nil, nil, fmt.Errorf("failed to write index: %w", err)
		}
		if err := indexFile.Close(); err != nil {
			return nil, nil, fmt.Errorf("failed to close index file: %w", err)
		}
	} else if err != nil {
		return nil, nil, fmt.Errorf("failed to stat index file: %w", err)
	} else {
		klog.Infof("Index file %s exists", indexPath)
	}

	klog.Infof("Reading index file %s", indexPath)
	indexFile, err := os.Open(indexPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open index file: %w", err)
	}

	id, err := index.ReadFrom(indexFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read index: %w", err)
	}
	return cr, id, nil
}

func NewTraverser(carPath string) (*Traverser, error) {
	cr, id, err := openCarReaderWithIndex(carPath)
	if err != nil {
		return nil, err
	}
	return &Traverser{
		id: id,
		cr: cr,
	}, nil
}

// Close closes the underlying blockstore.
func (t *Traverser) Close() error {
	return t.cr.Close()
}

func (t *Traverser) Get(ctx context.Context, c cid.Cid) (*blocks.BasicBlock, error) {
	node, _, err := getRawNode(
		func(ctx context.Context, c cid.Cid) (uint64, error) {
			var offset uint64
			t.id.GetAll(c, func(o uint64) bool {
				offset = o
				return false
			})
			return offset, nil
		},
		t.cr,
		c,
	)
	return node, err
}

func (t *Traverser) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	_, size, err := getRawNode(
		func(ctx context.Context, c cid.Cid) (uint64, error) {
			var offset uint64
			t.id.GetAll(c, func(o uint64) bool {
				offset = o
				return false
			})
			return offset, nil
		},
		t.cr,
		c,
	)
	return int(size), err
}

type RawNodeGetter interface {
	Get(ctx context.Context, c cid.Cid) (*blocks.BasicBlock, error)
	GetSize(ctx context.Context, c cid.Cid) (int, error)
}

func getRawNode(in func(ctx context.Context, c cid.Cid) (uint64, error), cr *carv2.Reader, c cid.Cid) (*blocks.BasicBlock, uint64, error) {
	offset, err := in(context.Background(), c)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get block: %w", err)
	}
	// get block from offset.
	dr, err := cr.DataReader()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get data reader: %w", err)
	}
	// read block from offset.
	dr.Seek(int64(offset), io.SeekStart)
	br := bufio.NewReader(dr)

	// sectionLen, err := varint.ReadUvarint(br)
	// if err != nil {
	// 	return nil, 0, err
	// }
	// // Read the CID.
	// cidLen, gotCid, err := cid.CidFromReader(br)
	// if err != nil {
	// 	return nil, 0, err
	// }
	// remainingSectionLen := int64(sectionLen) - int64(cidLen)
	// // Read the data.
	// data := make([]byte, remainingSectionLen)
	// if _, err := io.ReadFull(br, data); err != nil {
	// 	return nil, 0, err
	// }
	gotCid, data, err := util.ReadNode(br)
	if err != nil {
		return nil, 0, err
	}
	// verify that the CID we read matches the one we expected.
	if !gotCid.Equals(c) {
		return nil, 0, fmt.Errorf("CID mismatch: expected %s, got %s", c, gotCid)
	}
	bl, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, 0, err
	}
	return bl, uint64(len(data)), nil
}

// BuildGraph builds the graph of the CAR file.
func (t *Traverser) BuildGraph() error {
	graph, err := buildGraph(t, t.cr)
	if err != nil {
		return fmt.Errorf("failed to build graph: %w", err)
	}
	t.graph = graph
	return nil
}

func (t *Traverser) TraverseBlocks(callback func(BlockDAG) bool) error {
	if t.graph == nil {
		return fmt.Errorf("graph not built")
	}
	for _, rangeDAG := range t.graph.Epoch.Subsets {
		for _, blockDAG := range rangeDAG.Blocks {
			if !callback(blockDAG) {
				return nil
			}
		}
	}
	return nil
}

func buildGraph(
	in RawNodeGetter,
	cr *carv2.Reader,
) (*CARDAG, error) {
	roots, err := cr.Roots()
	if err != nil {
		return nil, fmt.Errorf("failed to get roots: %w", err)
	}
	fmt.Println("CAR roots:", roots)

	// there should only be one root.
	if len(roots) != 1 {
		return nil, fmt.Errorf("expected 1 root (the epoch), got %d", len(roots))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sizeDAG := NewCARDAG()

	// parse root as Epoch node.
	epochRoot := roots[0]
	sizeDAG.Epoch.CID = epochRoot
	epochBlock, err := in.Get(ctx, epochRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to get Epoch node: %w", err)
	}
	epoch, err := iplddecoders.DecodeEpoch(epochBlock.RawData())
	if err != nil {
		return nil, fmt.Errorf("failed to decode Epoch node: %w", err)
	}
	// sizeDAG.Epoch.Value = epoch

	// now get the Epoch node's child nodes.
	subsetLinks := epoch.Subsets

	sizeDAG.Epoch.Subsets = make([]SubsetDAG, len(subsetLinks))

	for subsetIndex, subsetLink := range subsetLinks {
		sizeDAG.Epoch.Subsets[subsetIndex].CID = subsetLink.(cidlink.Link).Cid

		subsetNode, err := in.Get(ctx, subsetLink.(cidlink.Link).Cid)
		if err != nil {
			return nil, fmt.Errorf("failed to get Subset node: %w", err)
		}
		subset, err := iplddecoders.DecodeSubset(subsetNode.RawData())
		if err != nil {
			return nil, fmt.Errorf("failed to decode Range node: %w", err)
		}
		// sizeDAG.Epoch.Subsets[subsetIndex].Value = subset

		// get the Range node's child nodes.
		blockLinks := subset.Blocks

		sizeDAG.Epoch.Subsets[subsetIndex].Blocks = make([]BlockDAG, len(blockLinks))

		for blockIndex, blockLink := range blockLinks {
			sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].CID = blockLink.(cidlink.Link).Cid

			slotBlock, err := in.Get(ctx, blockLink.(cidlink.Link).Cid)
			if err != nil {
				return nil, fmt.Errorf("failed to get Slot node: %w", err)
			}
			block, err := iplddecoders.DecodeBlock(slotBlock.RawData())
			if err != nil {
				return nil, fmt.Errorf("failed to decode Slot node: %w", err)
			}
			// sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Value = block

			entryLinks := block.Entries

			sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Entries = make([]EntryDAG, len(entryLinks))
			for entryIndex, entryLink := range entryLinks {
				sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Entries[entryIndex].CID = entryLink.(cidlink.Link).Cid

				fragmentBlock, err := in.Get(ctx, entryLink.(cidlink.Link).Cid)
				if err != nil {
					return nil, fmt.Errorf("failed to get Fragment node: %w", err)
				}
				fragment, err := iplddecoders.DecodeEntry(fragmentBlock.RawData())
				if err != nil {
					return nil, fmt.Errorf("failed to decode Fragment node: %w", err)
				}
				// sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Entries[entryIndex].Value = fragment

				transactionLinks := fragment.Transactions

				sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Entries[entryIndex].Transactions = make([]TransactionDAG, len(transactionLinks))

				for transactionIndex, transactionLink := range transactionLinks {
					sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Entries[entryIndex].Transactions[transactionIndex].CID = transactionLink.(cidlink.Link).Cid

					// transactionBlock, err := in.Get(ctx, transactionLink.(cidlink.Link).Cid)
					// if err != nil {
					// 	return nil, fmt.Errorf("failed to get Transaction node: %w", err)
					// }
					// transaction, err := iplddecoders.DecodeTransaction(transactionBlock.RawData())
					// if err != nil {
					// 	return nil, fmt.Errorf("failed to decode Transaction node: %w", err)
					// }
					// sizeDAG.Epoch.Subsets[subsetIndex].Blocks[blockIndex].Entries[entryIndex].Transactions[transactionIndex].Value = transaction
				}

			}
		}
	}

	return sizeDAG, nil
}

func NewCARDAG() *CARDAG {
	return &CARDAG{
		Epoch: EpochDAG{
			Subsets: []SubsetDAG{},
		},
	}
}

type CARDAG struct {
	Epoch EpochDAG
}

type EpochDAG struct {
	CID cid.Cid
	// Value *ipldbindcode.Epoch

	Subsets []SubsetDAG
}

type SubsetDAG struct {
	CID cid.Cid
	// Value *ipldbindcode.Subset

	Blocks []BlockDAG
}

type BlockDAG struct {
	CID cid.Cid
	// Value *ipldbindcode.Block

	Entries []EntryDAG
}

type EntryDAG struct {
	CID cid.Cid
	// Value *ipldbindcode.Entry

	Transactions []TransactionDAG
}

type TransactionDAG struct {
	CID cid.Cid
	// Value *ipldbindcode.Transaction
}

func (d *CARDAG) EachSize(robs *blockstore.ReadOnly, callback func(parents []cid.Cid, this cid.Cid, size int) error) error {
	return d.Each(func(parents []cid.Cid, this cid.Cid) error {
		size, err := robs.GetSize(context.TODO(), this)
		if err != nil {
			return fmt.Errorf("failed to get size of node %s: %w", this.String(), err)
		}
		return callback(parents, this, size)
	})
}

func (d *CARDAG) Each(callback func(parents []cid.Cid, this cid.Cid) error) error {
	return d.Epoch.each([]cid.Cid{}, callback)
}

func (d *EpochDAG) each(
	parents []cid.Cid,
	callback func(parents []cid.Cid, this cid.Cid) error,
) error {
	err := callback(parents, d.CID)
	if err != nil {
		return err
	}
	for _, rangeDAG := range d.Subsets {
		err := rangeDAG.each(append(parents, d.CID), callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *SubsetDAG) each(
	parents []cid.Cid,
	callback func(parents []cid.Cid, this cid.Cid) error,
) error {
	err := callback(parents, d.CID)
	if err != nil {
		return err
	}
	for _, slotDAG := range d.Blocks {
		err := slotDAG.each(append(parents, d.CID), callback)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *BlockDAG) each(
	parents []cid.Cid,
	callback func(parents []cid.Cid, this cid.Cid) error,
) error {
	err := callback(parents, d.CID)
	if err != nil {
		return err
	}
	for _, entry := range d.Entries {
		err := callback(append(parents, d.CID), entry.CID)
		if err != nil {
			return err
		}
	}
	return nil
}

// Each SubsetDAG has a list of Slots.
func (e *EpochDAG) NumSlots() int {
	numSlots := 0
	for _, rangeDAG := range e.Subsets {
		numSlots += len(rangeDAG.Blocks)
	}
	return numSlots
}

func (e *EpochDAG) IterateRanges(callback func(*SubsetDAG) bool) {
	for _, rangeDAG := range e.Subsets {
		if !callback(&rangeDAG) {
			return
		}
	}
}

// Iterate over all the slots in the EpochDAG.
func (e *EpochDAG) IterateBlocks(callback func(*BlockDAG) bool) {
	for _, rangeDAG := range e.Subsets {
		for _, block := range rangeDAG.Blocks {
			if !callback(&block) {
				return
			}
		}
	}
}

// Iterate over all the fragments in the EpochDAG.
func (e *EpochDAG) IterateEntries(callback func(*EntryDAG) bool) {
	for _, rangeDAG := range e.Subsets {
		for _, slotDAG := range rangeDAG.Blocks {
			for _, entry := range slotDAG.Entries {
				if !callback(&entry) {
					return
				}
			}
		}
	}
}

// Iterate over all the transactions in the EpochDAG.
func (e *EpochDAG) IterateTransactions(callback func(*TransactionDAG) bool) {
	for _, rangeDAG := range e.Subsets {
		for _, slotDAG := range rangeDAG.Blocks {
			for _, entry := range slotDAG.Entries {
				for _, transaction := range entry.Transactions {
					if !callback(&transaction) {
						return
					}
				}
			}
		}
	}
}
