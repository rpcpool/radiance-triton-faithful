package main

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"go.firedancer.io/radiance/pkg/blockstore"
	"k8s.io/klog/v2"
)

func main() {
	path := os.Args[1]
	klog.Infof("Creating traverser for %s", path)
	t, err := NewTraverser(path)
	if err != nil {
		panic(err)
	}
	defer t.Close()
	klog.Infof("Created traverser for %s", path)

	klog.Info("Building graph...")
	if err := t.BuildGraph(); err != nil {
		panic(err)
	}
	klog.Infof("Graph built successfully")

	klog.Info("Traversing blocks	...")
	t.TraverseBlocks(func(block BlockDAG) bool {
		klog.Infof("Traversing entries...")
		for _, entry := range block.Entries {
			for _, txNode := range entry.Transactions {
				// spew.Dump(txNode)
				{
					var tx solana.Transaction
					if err := bin.UnmarshalBin(&tx, txNode.Value.Data); err != nil {
						panic(err)
					} else if len(tx.Signatures) == 0 {
						panic("no signatures")
					}
					fmt.Println(tx.Signatures[0].String())
				}
				{
					status, err := blockstore.ParseTransactionStatusMeta(txNode.Value.Metadata)
					if err != nil {
						panic(err)
					}
					spew.Dump(status)
				}
			}
		}
		return true
	})
	klog.Infof("Slots traversed successfully")
}
