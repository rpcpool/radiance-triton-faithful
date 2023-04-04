package ipldgen

import (
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"go.firedancer.io/radiance/pkg/ipld/car"
	"go.firedancer.io/radiance/pkg/ipld/ipldsch"
)

// TxListAssembler produces a Merkle tree of transactions with wide branches.
type TxListAssembler struct {
	writer car.OutStream
	links  []cidlink.Link
}

func NewTxListAssembler(writer car.OutStream) TxListAssembler {
	return TxListAssembler{writer: writer}
}

// Assemble produces a transaction list DAG and returns the root node.
func (t TxListAssembler) Assemble(txs []solana.Transaction) (datamodel.Node, error) {
	for _, tx := range txs {
		if err := t.writeTx(tx); err != nil {
			return nil, err
		}
	}
	return t.finish()
}

// writeTx writes out SolanaTx to the CAR and appends CID to memory.
func (t *TxListAssembler) writeTx(tx solana.Transaction) error {
	buf, err := bin.MarshalBin(tx)
	if err != nil {
		panic("failed to marshal tx: " + err.Error())
	}
	leaf := car.NewBlockFromRaw(buf, uint64(multicodec.Raw))
	if err := t.writer.WriteBlock(leaf); err != nil {
		return err
	}
	t.links = append(t.links, cidlink.Link{Cid: leaf.Cid})
	return nil
}

// finish recursively writes out RadianceTx into a tree structure until the root fits.
func (t *TxListAssembler) finish() (datamodel.Node, error) {
	node, err := t.pack()
	if err != nil {
		return nil, err
	}
	// Terminator: Reached root, stop merklerizing.
	if len(t.links) == 0 {
		return node, nil
	}

	// Create left link.
	block, err := car.NewBlockFromCBOR(node, uint64(multicodec.DagCbor))
	if err != nil {
		return nil, err
	}
	var links []cidlink.Link
	links = append(links, cidlink.Link{Cid: block.Cid})
	if err := t.writer.WriteBlock(block); err != nil {
		return nil, err
	}
	// Create right links.
	for len(t.links) > 0 {
		node, err = t.pack()
		if err != nil {
			return nil, err
		}
		block, err = car.NewBlockFromCBOR(node, uint64(multicodec.DagCbor))
		if err != nil {
			return nil, err
		}
		if err = t.writer.WriteBlock(block); err != nil {
			return nil, err
		}
		links = append(links, cidlink.Link{Cid: block.Cid})
	}

	// Move up layer.
	t.links = links
	return t.finish()
}

// pack moves as many CIDs as possible into a node.
func (t *TxListAssembler) pack() (node datamodel.Node, err error) {
	builder := ipldsch.Type.TransactionList.NewBuilder()
	list, err := builder.BeginList(0)
	if err != nil {
		return nil, err
	}

	// Pack nodes until we fill TargetBlockSize.
	left := TargetBlockSize - lengthPrefixSize
	for ; len(t.links) > 0 && left >= CIDLen; left -= CIDLen {
		link := t.links[0]
		t.links = t.links[1:]
		if err := list.AssembleValue().AssignLink(link); err != nil {
			return nil, err
		}
	}

	if err := list.Finish(); err != nil {
		return nil, err
	}
	node = builder.Build()
	return node, nil
}
