package ipldbindcode

import (
	_ "embed"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
)

//go:embed ledger.ipldsch
var embeddedSchema []byte

var Prototypes schemaSlab

type schemaSlab struct {
	List__Link      schema.TypedPrototype
	Epoch           schema.TypedPrototype
	Subset          schema.TypedPrototype
	List__Shredding schema.TypedPrototype
	Block           schema.TypedPrototype
	Shredding       schema.TypedPrototype
	Entry           schema.TypedPrototype
	Transaction     schema.TypedPrototype
	Hash            schema.TypedPrototype
	Buffer          schema.TypedPrototype
}

func init() {
	ts, err := ipld.LoadSchemaBytes(embeddedSchema)
	if err != nil {
		panic(err)
	}

	Prototypes.List__Link = bindnode.Prototype(
		(*List__Link)(nil),
		ts.TypeByName("List__Link"),
	)

	Prototypes.Epoch = bindnode.Prototype(
		(*Epoch)(nil),
		ts.TypeByName("Epoch"),
	)

	Prototypes.Subset = bindnode.Prototype(
		(*Subset)(nil),
		ts.TypeByName("Subset"),
	)

	Prototypes.List__Shredding = bindnode.Prototype(
		(*List__Shredding)(nil),
		ts.TypeByName("List__Shredding"),
	)

	Prototypes.Block = bindnode.Prototype(
		(*Block)(nil),
		ts.TypeByName("Block"),
	)

	Prototypes.Shredding = bindnode.Prototype(
		(*Shredding)(nil),
		ts.TypeByName("Shredding"),
	)

	Prototypes.Entry = bindnode.Prototype(
		(*Entry)(nil),
		ts.TypeByName("Entry"),
	)

	Prototypes.Transaction = bindnode.Prototype(
		(*Transaction)(nil),
		ts.TypeByName("Transaction"),
	)

	Prototypes.Hash = bindnode.Prototype(
		(*Hash)(nil),
		ts.TypeByName("Hash"),
	)

	Prototypes.Buffer = bindnode.Prototype(
		(*Buffer)(nil),
		ts.TypeByName("Buffer"),
	)
}

type (
	Hash   []byte
	Buffer []byte
)
