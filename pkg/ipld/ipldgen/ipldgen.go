// Package ipldgen transforms Solana ledger data into IPLD DAGs.
package ipldgen

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/ipld/car"
	"go.firedancer.io/radiance/pkg/ipld/ipldsch"
	"go.firedancer.io/radiance/pkg/shred"
)

// CIDLen is the serialized size in bytes of a Raw/DagCbor CIDv1
const CIDLen = 36

// TargetBlockSize is the target size of variable-length IPLD blocks (e.g. link lists).
// Don't set this to the IPFS max block size, as we might overrun by a few kB.
const TargetBlockSize = 1 << 19

// lengthPrefixSize is the max practical size of an array length prefix.
const lengthPrefixSize = 4

// IPLD node identifier
const (
	KindTx = iota
	KindEntry
	KindBlock
)

type BlockAssembler struct {
	writer    car.OutStream
	slot      uint64
	entries   []cidlink.Link
	shredding []shredding
}

type shredding struct {
	entryEndIdx uint
	shredEndIdx uint
}

func NewBlockAssembler(writer car.OutStream, slot uint64) *BlockAssembler {
	return &BlockAssembler{
		writer: writer,
		slot:   slot,
	}
}

type EntryPos struct {
	Slot       uint64
	EntryIndex int
	Batch      int
	BatchIndex int
	LastShred  int
}

// WriteEntry appends a ledger entry to the CAR.
func (b *BlockAssembler) WriteEntry(entry shred.Entry, pos EntryPos, txMetas []*blockstore.TransactionStatusMetaWithRaw) error {
	txList, err := NewTxListAssembler(b.writer).Assemble(entry.Txns)
	if err != nil {
		return err
	}
	metaList, err := NewTxMetaListAssembler(b.writer).Assemble(txMetas)
	if err != nil {
		return err
	}
	builder := ipldsch.Type.Entry.NewBuilder()
	entryMap, err := builder.BeginMap(9)
	if err != nil {
		return err
	}

	if pos.LastShred > 0 {
		b.shredding = append(b.shredding, shredding{
			entryEndIdx: uint(pos.EntryIndex),
			shredEndIdx: uint(pos.LastShred),
		})
	}

	var nodeAsm datamodel.NodeAssembler

	nodeAsm, err = entryMap.AssembleEntry("kind")
	if err != nil {
		return err
	}
	if err = nodeAsm.AssignInt(int64(KindEntry)); err != nil {
		return err
	}

	nodeAsm, err = entryMap.AssembleEntry("numHashes")
	if err != nil {
		return err
	}
	if err = nodeAsm.AssignInt(int64(entry.NumHashes)); err != nil {
		return err
	}

	nodeAsm, err = entryMap.AssembleEntry("hash")
	if err != nil {
		return err
	}
	if err = nodeAsm.AssignBytes(entry.Hash[:]); err != nil {
		return err
	}

	nodeAsm, err = entryMap.AssembleEntry("txs")
	if err != nil {
		return err
	}
	if err = nodeAsm.AssignNode(txList); err != nil {
		return err
	}

	nodeAsm, err = entryMap.AssembleEntry("txMetas")
	if err != nil {
		return err
	}
	if err = nodeAsm.AssignNode(metaList); err != nil {
		return err
	}

	if err = entryMap.Finish(); err != nil {
		return err
	}
	node := builder.Build().(ipldsch.Entry).Representation()
	block, err := car.NewBlockFromCBOR(node, uint64(multicodec.DagCbor))
	if err != nil {
		return err
	}
	b.entries = append(b.entries, cidlink.Link{Cid: block.Cid})
	return b.writer.WriteBlock(block)
}

// Finish appends block metadata to the CAR and returns the root CID.
func (b *BlockAssembler) Finish() (link cidlink.Link, err error) {
	builder := ipldsch.Type.Block.NewBuilder()
	entryMap, err := builder.BeginMap(4)
	if err != nil {
		return link, err
	}

	var nodeAsm datamodel.NodeAssembler

	nodeAsm, err = entryMap.AssembleEntry("kind")
	if err != nil {
		return link, err
	}
	if err = nodeAsm.AssignInt(int64(KindBlock)); err != nil {
		return link, err
	}

	nodeAsm, err = entryMap.AssembleEntry("slot")
	if err != nil {
		return link, err
	}
	if err = nodeAsm.AssignInt(int64(b.slot)); err != nil {
		return link, err
	}

	nodeAsm, err = entryMap.AssembleEntry("shredding")
	if err != nil {
		return link, err
	}
	list, err := nodeAsm.BeginList(int64(len(b.shredding)))
	if err != nil {
		return link, err
	}
	for _, s := range b.shredding {
		tuple, err := list.AssembleValue().BeginMap(2)
		if err != nil {
			return link, err
		}
		entry, err := tuple.AssembleEntry("entryEndIdx")
		if err != nil {
			return link, err
		}
		if err = entry.AssignInt(int64(s.entryEndIdx)); err != nil {
			return link, err
		}
		entry, err = tuple.AssembleEntry("shredEndIdx")
		if err != nil {
			return link, err
		}
		if err = entry.AssignInt(int64(s.shredEndIdx)); err != nil {
			return link, err
		}
		if err = tuple.Finish(); err != nil {
			return link, err
		}
	}
	if err = list.Finish(); err != nil {
		return link, err
	}

	nodeAsm, err = entryMap.AssembleEntry("entries")
	if err != nil {
		return link, err
	}
	list, err = nodeAsm.BeginList(int64(len(b.entries)))
	if err != nil {
		return link, err
	}
	for _, entry := range b.entries {
		if err = list.AssembleValue().AssignLink(entry); err != nil {
			return link, err
		}
	}
	if err = list.Finish(); err != nil {
		return link, err
	}

	if err = entryMap.Finish(); err != nil {
		return link, err
	}
	node := builder.Build().(ipldsch.Block).Representation()
	block, err := car.NewBlockFromCBOR(node, uint64(multicodec.DagCbor))
	if err != nil {
		return link, err
	}
	if err = b.writer.WriteBlock(block); err != nil {
		return link, err
	}

	return cidlink.Link{Cid: block.Cid}, nil
}
