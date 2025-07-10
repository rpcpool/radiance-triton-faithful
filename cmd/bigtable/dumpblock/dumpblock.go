package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"

	"github.com/davecgh/go-spew/spew"
	"github.com/gagliardetto/solana-go"
	binc "github.com/rpcpool/yellowstone-faithful/parse_legacy_transaction_status_meta"
	solanatxmetaparsers "github.com/rpcpool/yellowstone-faithful/solana-tx-meta-parsers"
	"go.firedancer.io/radiance/pkg/ledger_bigtable"
)

var (
	flagBlock = flag.Uint64("block", 0, "Block number to dump")
	flagDump  = flag.Bool("dump", true, "Dump the block to stdout as prototxt")
)

func init() {
	flag.Parse()

	if *flagBlock == 0 {
		log.Fatal("Must specify block number")
	}
}

func main() {
	ctx := context.Background()

	btClient, err := ledger_bigtable.MainnetClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create bigtable client: %v", err)
	}

	table := btClient.Open(ledger_bigtable.BlocksTable)
	rowKey := ledger_bigtable.SlotKey(*flagBlock)

	row, err := table.ReadRow(ctx, rowKey)
	if err != nil {
		log.Fatalf("Could not read row: %v", err)
	}

	block, err := ledger_bigtable.ParseRow(row)
	if err != nil {
		log.Fatalf("Could not parse row for slot %d: %v", *flagBlock, err)
	}

	if block == nil {
		log.Fatalf("Block not found")
	}
	slot := *flagBlock

	if block.Proto != nil {
		rawBlock := block.Proto
		// b, err := prototext.MarshalOptions{
		// 	Multiline: true,
		// 	Indent:    "\t",
		// }.Marshal(rawBlock)
		// if err != nil {
		// 	log.Fatalf("Could not marshal block: %v", err)
		// }

		log.Printf("Fetched block %v with %d txs", *flagBlock, len(rawBlock.Transactions))
		if *flagDump {
			for i, tx := range rawBlock.Transactions {
				log.Printf("Transaction %d: %s", i, tx.Transaction)
				sig := solana.SignatureFromBytes(tx.Transaction.Signatures[0])
				meta := tx.Meta
				metaBuf, err := proto.Marshal(meta)
				if err != nil {
					panic(fmt.Errorf("failed to marshal transaction meta for block %d / tx %s: %w", slot, sig, err))
				}
				{
					// sanity check:
					meta, err := solanatxmetaparsers.ParseTransactionStatusMetaContainer(metaBuf)
					if err != nil {
						panic(fmt.Errorf("sanity check: proto block %d / tx %s: failed to parse transaction status meta: %w", slot, sig, err))
					}
					if meta == nil {
						panic(fmt.Errorf("sanity check: proto block %d / tx %s: transaction status meta is nil", slot, sig))
					}
					spew.Dump(meta)
				}
			}
		}
	}
	if block.Bin != nil {
		rawBlock := block.Bin
		log.Printf("Fetched block %v with %d txs", *flagBlock, len(rawBlock.Transactions))
		if *flagDump {
			// spew.Dump(rawBlock)
			for i, tx := range rawBlock.Transactions {
				log.Printf("Transaction %d: %s", i, tx.Transaction)
				if tx.Meta != nil {
					{
						spew.Dump(tx.Meta)

						{
							converted := &binc.TransactionStatusMeta{}
							if tx.Meta.Err == nil {
								converted.Status = &binc.Result__Ok{}
							} else {
								converted.Status = &binc.Result__Err{
									Value: *tx.Meta.Err,
								}
							}
							converted.Fee = tx.Meta.Fee
							converted.PostBalances = tx.Meta.PostBalances
							converted.PreBalances = tx.Meta.PreBalances
							marshaled, err := converted.BincodeSerializeStored()
							if err != nil {
								panic(err)
							}
							{
								re, err := binc.BincodeDeserializeTransactionStatusMeta(marshaled)
								if err != nil {
									panic(err)
								}
								spew.Dump(re)
							}
						}
					}
				}
			}
		}
	}
}
