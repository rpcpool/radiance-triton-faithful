package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/davecgh/go-spew/spew"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipld/go-car"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/multiformats/go-multicodec"
	"go.firedancer.io/radiance/pkg/ipld/ipldgen"
	"go.firedancer.io/radiance/pkg/ipld/ipldsch"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"k8s.io/klog/v2"
)

func main() {
	f, err := os.Open(os.Args[1])
	if err != nil {
		klog.Exit(err.Error())
	}
	defer f.Close()

	rd, err := car.NewCarReader(f)
	if err != nil {
		klog.Exitf("Failed to open CAR: %s", err)
	}

	for i := 0; i < 500; i++ {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			klog.Exitf("Failed to open CAR: %s", err)
		}
		fmt.Printf("CID=%s Multicodec=%#x", block.Cid(), block.Cid().Type())

		/* OLD format
		   if block.Cid().Type() == ipldgen.RadianceBlock {
		     fmt.Println("loaded block")
		     builder := ipldsch.Type.Block.NewBuilder()
		     err := dagcbor.Decode(builder,bytes.NewReader(block.RawData()))
		     if err != nil {
		       klog.Exitf("Failed to decode blcok: %s", err)
		     }
		     spew.Dump(builder.Build())

		   } else if block.Cid().Type() == ipldgen.RadianceTxList {
		     fmt.Println("loaded transaction list")
		   } else if block.Cid().Type() == ipldgen.SolanaTx {
		     fmt.Println("solana tx")
		     var tx solana.Transaction
		     if err := bin.UnmarshalBin(&tx, block.RawData()); err != nil {
		         klog.Errorf("Invalid CID %s: %s", block.Cid(), err)
		         os.Exit(3)
		     }
		     fmt.Println(tx.Signatures[0].String())
		     spew.Dump(tx)
		   } else if block.Cid().Type() == ipldgen.RadianceEntry {
		     fmt.Println("radiance entry")
		   }*/

		switch multicodec.Code(block.Cid().Type()) {
		case multicodec.Raw:
			spew.Dump(block)
			var tx solana.Transaction
			if err := bin.UnmarshalBin(&tx, block.RawData()); err != nil {
				klog.Errorf("Invalid CID %s: %s", block.Cid(), err)
				os.Exit(3)
			} else if len(tx.Signatures) == 0 {
				klog.Errorf("Invalid CID %s: tx has zero signatures", block.Cid())
				os.Exit(3)
			}
			fmt.Println(tx.Signatures[0].String())
		case multicodec.DagCbor:
			decodeOpt := dagcbor.DecodeOptions{
				AllowLinks: true,
			}
			builder := ipldsch.Type.Block.NewBuilder()
			if err := decodeOpt.Decode(builder, bytes.NewReader(block.RawData())); err != nil {
				klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
				os.Exit(3)
			}
			node := builder.Build()
			fmt.Printf("Type: %s\n", node.Kind())
			val, err := node.LookupByString("kind")
			if err != nil {
				klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
				os.Exit(3)
			}
			num, err := val.AsInt()
			if err != nil {
				klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
				os.Exit(3)
			}

			if int64(ipldgen.KindEntry) == num {
				fmt.Println("This is an entry")
			} else if int64(ipldgen.KindBlock) == num {
				fmt.Println("This is a block")
			}
			spew.Dump(node)

			// get the Meta child
			entries, err := node.LookupByString("entries")
			if err != nil {
				klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
				os.Exit(3)
			}
			for !entries.ListIterator().Done() {
				_, entry, err := entries.ListIterator().Next()
				if err != nil {
					klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
					os.Exit(3)
				}
				kind, err := entry.LookupByString("kind")
				if err != nil {
					klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
					os.Exit(3)
				}
				num, err := kind.AsInt()
				if err != nil {
					klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
					os.Exit(3)
				}
				if int64(ipldgen.KindEntry) == num {
					fmt.Println("This is an entry")
				} else if int64(ipldgen.KindBlock) == num {
					fmt.Println("This is a block")
				} else {
					fmt.Println("This is an unknown block")
				}
				spew.Dump(entry)
				{
					transactions, err := entry.LookupByString("txs")
					if err != nil {
						klog.Errorf("Couldn't parse CID %s: %s", transactions, err)
						os.Exit(3)
					}
					for !transactions.ListIterator().Done() {
						_, txNode, err := transactions.ListIterator().Next()
						if err != nil {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						// must be raw
						if txNode.Kind() != ipld.Kind_Bytes {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						txNodeBytes, err := txNode.AsBytes()
						if err != nil {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						var tx solana.Transaction
						if err := bin.UnmarshalBin(&tx, txNodeBytes); err != nil {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						fmt.Println(tx.Signatures[0].String())
					}
				}
				{
					txMetas, err := entry.LookupByString("txMetas")
					if err != nil {
						klog.Errorf("Couldn't parse CID %s: %s", txMetas, err)
						os.Exit(3)
					}
					for !txMetas.ListIterator().Done() {
						_, txMetaNode, err := txMetas.ListIterator().Next()
						if err != nil {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						// must be raw
						if txMetaNode.Kind() != ipld.Kind_Bytes {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						txMetaNodeBytes, err := txMetaNode.AsBytes()
						if err != nil {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						var txMeta *confirmed_block.TransactionStatusMeta
						if err := bin.UnmarshalBin(&txMeta, txMetaNodeBytes); err != nil {
							klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
							os.Exit(3)
						}
						fmt.Println(txMeta)
					}
				}
			}

			// switch on the type of the meta (multicodec.PB)
			switch multicodec.Code(entries.Kind()) {
			case multicodec.Raw:
				fmt.Println("This is a raw block")
			case multicodec.DagCbor:
				fmt.Println("This is a dag-cbor block")
			case multicodec.DagPb:
				fmt.Println("This is a dag-pb block")
			default:
				fmt.Println("This is an unknown block")
			}

		case multicodec.DagPb:
			fmt.Println("This is a dag-pb block")

			// decode the block
			decodeOpt := dagcbor.DecodeOptions{
				AllowLinks: true,
			}
			builder := basicnode.Prototype.Any.NewBuilder()
			if err := decodeOpt.Decode(builder, bytes.NewReader(block.RawData())); err != nil {
				klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
				os.Exit(3)
			}
			node := builder.Build()
			fmt.Printf("Type: %s\n", node.Kind())
			spew.Dump(node)

			// get the Meta child
			meta, err := node.LookupByString("txMetas")
			if err != nil {
				klog.Errorf("Couldn't parse CID %s: %s", block.Cid(), err)
				os.Exit(3)
			}
			spew.Dump(meta)

		default:
			spew.Dump(block.RawData())
		}
	}
}
