package ipldsch

// Code generated by go-ipld-prime gengo.  DO NOT EDIT.

import (
	"github.com/ipld/go-ipld-prime/datamodel"
)

var _ datamodel.Node = nil // suppress errors when this dependency is not referenced
// Type is a struct embeding a NodePrototype/Type for every Node implementation in this package.
// One of its major uses is to start the construction of a value.
// You can use it like this:
//
//	ipldsch.Type.YourTypeName.NewBuilder().BeginMap() //...
//
// and:
//
//	ipldsch.Type.OtherTypeName.NewBuilder().AssignString("x") // ...
var Type typeSlab

type typeSlab struct {
	Block                     _Block__Prototype
	Block__Repr               _Block__ReprPrototype
	Bool                      _Bool__Prototype
	Bool__Repr                _Bool__ReprPrototype
	Bytes                     _Bytes__Prototype
	Bytes__Repr               _Bytes__ReprPrototype
	Entry                     _Entry__Prototype
	Entry__Repr               _Entry__ReprPrototype
	Float                     _Float__Prototype
	Float__Repr               _Float__ReprPrototype
	Hash                      _Hash__Prototype
	Hash__Repr                _Hash__ReprPrototype
	Int                       _Int__Prototype
	Int__Repr                 _Int__ReprPrototype
	Link                      _Link__Prototype
	Link__Repr                _Link__ReprPrototype
	List__Link                _List__Link__Prototype
	List__Link__Repr          _List__Link__ReprPrototype
	List__Shredding           _List__Shredding__Prototype
	List__Shredding__Repr     _List__Shredding__ReprPrototype
	Shredding                 _Shredding__Prototype
	Shredding__Repr           _Shredding__ReprPrototype
	String                    _String__Prototype
	String__Repr              _String__ReprPrototype
	Transaction               _Transaction__Prototype
	Transaction__Repr         _Transaction__ReprPrototype
	TransactionList           _TransactionList__Prototype
	TransactionList__Repr     _TransactionList__ReprPrototype
	TransactionMetaList       _TransactionMetaList__Prototype
	TransactionMetaList__Repr _TransactionMetaList__ReprPrototype
}

// --- type definitions follow ---

// Block matches the IPLD Schema type "Block".  It has struct type-kind, and may be interrogated like map kind.
type Block = *_Block
type _Block struct {
	kind      _Int
	slot      _Int
	entries   _List__Link
	shredding _List__Shredding
}

// Bool matches the IPLD Schema type "Bool".  It has bool kind.
type Bool = *_Bool
type _Bool struct{ x bool }

// Bytes matches the IPLD Schema type "Bytes".  It has bytes kind.
type Bytes = *_Bytes
type _Bytes struct{ x []byte }

// Entry matches the IPLD Schema type "Entry".  It has struct type-kind, and may be interrogated like map kind.
type Entry = *_Entry
type _Entry struct {
	kind      _Int
	numHashes _Int
	hash      _Hash
	txs       _TransactionList
	txMetas   _TransactionMetaList
}

// Float matches the IPLD Schema type "Float".  It has float kind.
type Float = *_Float
type _Float struct{ x float64 }

// Hash matches the IPLD Schema type "Hash".  It has bytes kind.
type Hash = *_Hash
type _Hash struct{ x []byte }

// Int matches the IPLD Schema type "Int".  It has int kind.
type Int = *_Int
type _Int struct{ x int64 }

// Link matches the IPLD Schema type "Link".  It has link kind.
type Link = *_Link
type _Link struct{ x datamodel.Link }

// List__Link matches the IPLD Schema type "List__Link".  It has list kind.
type List__Link = *_List__Link
type _List__Link struct {
	x []_Link
}

// List__Shredding matches the IPLD Schema type "List__Shredding".  It has list kind.
type List__Shredding = *_List__Shredding
type _List__Shredding struct {
	x []_Shredding
}

// Shredding matches the IPLD Schema type "Shredding".  It has struct type-kind, and may be interrogated like map kind.
type Shredding = *_Shredding
type _Shredding struct {
	entryEndIdx _Int
	shredEndIdx _Int
}

// String matches the IPLD Schema type "String".  It has string kind.
type String = *_String
type _String struct{ x string }

// Transaction matches the IPLD Schema type "Transaction".  It has bytes kind.
type Transaction = *_Transaction
type _Transaction struct{ x []byte }

// TransactionList matches the IPLD Schema type "TransactionList".  It has list kind.
type TransactionList = *_TransactionList
type _TransactionList struct {
	x []_Link
}

// TransactionMetaList matches the IPLD Schema type "TransactionMetaList".  It has list kind.
type TransactionMetaList = *_TransactionMetaList
type _TransactionMetaList struct {
	x []_Link
}
