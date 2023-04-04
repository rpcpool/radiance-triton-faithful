package ipldgen

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"go.firedancer.io/radiance/pkg/ipld/car"
	"go.firedancer.io/radiance/pkg/ipld/ipldsch"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"google.golang.org/protobuf/proto"
)

// TxMetaListAssembler produces a Merkle tree of transactionMeta with wide branches.
type TxMetaListAssembler struct {
	writer car.OutStream
	links  []cidlink.Link
}

func NewTxMetaListAssembler(writer car.OutStream) TxMetaListAssembler {
	return TxMetaListAssembler{writer: writer}
}

// Assemble produces a transaction list DAG and returns the root node.
func (t TxMetaListAssembler) Assemble(metas []*confirmed_block.TransactionStatusMeta) (datamodel.Node, error) {
	for _, meta := range metas {
		if err := t.writeTxMeta(meta); err != nil {
			return nil, err
		}
	}
	return t.finish()
}

// writeTxMeta writes out TransactionMeta to the CAR and appends CID to memory.
func (t *TxMetaListAssembler) writeTxMeta(meta *confirmed_block.TransactionStatusMeta) error {
	buf, err := proto.Marshal(meta)
	if err != nil {
		panic("failed to marshal meta: " + err.Error())
	}
	leaf := car.NewBlockFromRaw(buf, uint64(multicodec.Raw))
	if err := t.writer.WriteBlock(leaf); err != nil {
		return err
	}
	t.links = append(t.links, cidlink.Link{Cid: leaf.Cid})
	return nil
}

// finish recursively writes out RadianceTx into a tree structure until the root fits.
func (t *TxMetaListAssembler) finish() (datamodel.Node, error) {
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
func (t *TxMetaListAssembler) pack() (node datamodel.Node, err error) {
	builder := ipldsch.Type.TransactionMetaList.NewBuilder()
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
