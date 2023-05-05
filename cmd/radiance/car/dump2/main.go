package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/davecgh/go-spew/spew"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car"
	"github.com/ipld/go-car/util"
	carv2 "github.com/ipld/go-car/v2"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/indexcidtooffset"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/ipld/ipldbindcode"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/iplddecoders"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/compactindex"
	"k8s.io/klog/v2"
)

func main() {
	carPath := os.Args[1]
	indexDir := os.ExpandEnv("/media/$HOME/solana-history/cidtooffset")
	{
		compactIndexDir := os.ExpandEnv("/media/$HOME/solana-history/compactindex")
		numItems := 1000000
		targetFileSize := 1024 * 1024 * 1024
		builder, err := compactindex.NewBuilder(
			"",
			uint(numItems),
			uint64(targetFileSize),
		)
		if err != nil {
			panic(err)
		}
		defer builder.Close()

		c := cid.MustParse("bafyreiahlvkcx76rc7m7dttjmak2fe4xk63ldsyh7xjofzln3mdsq6zqau")
		builder.Insert(c.Hash(), 123)

		// Create file for final index.
		targetFile, err := os.Create(filepath.Join(compactIndexDir, "compactindex.index"))
		if err != nil {
			panic(err)
		}
		defer targetFile.Close()
		err = builder.Seal(context.TODO(), targetFile)
		if err != nil {
			panic(err)
		}

		{
			_, err := targetFile.Seek(0, io.SeekStart)
			if err != nil {
				panic(err)
			}

			db, err := compactindex.Open(targetFile)
			if err != nil {
				panic(err)
			}

			// Run query benchmark.
			{
				bucket, err := db.LookupBucket(c.Hash())
				if err != nil {
					panic(err)
				}
				value, err := bucket.Lookup(c.Hash())
				if err != nil {
					panic(err)
				}
				spew.Dump(value)
			}
		}
		return
	}
	if false {
		c2o, err := indexcidtooffset.OpenStore(context.Background(),
			filepath.Join(indexDir, "index"),
			filepath.Join(indexDir, "data"),
		)
		if err != nil {
			panic(err)
		}
		defer c2o.Close()
		c2o.Start()

		startedAt := time.Now()
		defer func() {
			klog.Infof("Finished in %s", time.Since(startedAt))
		}()

		c := cid.MustParse("bafyreiahlvkcx76rc7m7dttjmak2fe4xk63ldsyh7xjofzln3mdsq6zqau")
		offset, err := c2o.Get(context.Background(), c)
		if err != nil {
			panic(err)
		}
		klog.Infof("offset: %d", offset)

		cr, err := carv2.OpenReader(carPath)
		if err != nil {
			panic(err)
		}

		// Get root CIDs in the CARv1 file.
		roots, err := cr.Roots()
		if err != nil {
			panic(err)
		}
		spew.Dump(roots)

		// get block from offset.
		dr, err := cr.DataReader()
		if err != nil {
			panic(err)
		}
		// read block from offset.
		dr.Seek(int64(offset), io.SeekStart)
		br := bufio.NewReader(dr)

		gotCid, data, err := util.ReadNode(br)
		if err != nil {
			panic(err)
		}
		// verify that the CID we read matches the one we expected.
		if !gotCid.Equals(c) {
			panic(err)
		}
		bl, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			panic(err)
		}
		spew.Dump(bl)
		spew.Dump(bl.RawData()[1])
		fmt.Println("success")
		return
	}
	if false {
		startedAt := time.Now()
		defer func() {
			klog.Infof("Finished in %s", time.Since(startedAt))
		}()
		klog.Infof("Creating index for %s", carPath)
		err := CreateIndex(
			context.TODO(),
			carPath,
			indexDir,
		)
		klog.Info("Index created")
		if err != nil {
			panic(err)
		}
		return
	}
	if true {
		simpleIter, err := NewSimpleIterator(carPath, indexDir)
		if err != nil {
			panic(err)
		}
		defer simpleIter.Close()

		startedAt := time.Now()
		numSolanaBlocks := 0
		numTransactions := 0

		defer func() {
			klog.Infof("Finished in %s", time.Since(startedAt))
			klog.Infof("Read %d Solana blocks", numSolanaBlocks)
			klog.Infof("Read %d transactions", numTransactions)
		}()

		epoch, err := simpleIter.GetEpoch(context.Background())
		if err != nil {
			panic(err)
		}
		spew.Dump(epoch)

		err = simpleIter.FindBlocks(context.Background(), func(block *ipldbindcode.Block) error {
			numSolanaBlocks++
			if numSolanaBlocks%100000 == 0 {
				fmt.Print(".")
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		took := time.Since(startedAt)
		klog.Infof("Finished iterating blocks in %s; found %d solana blocks", took, numSolanaBlocks)

		err = simpleIter.FindTransactions(context.Background(), func(tx *ipldbindcode.Transaction) error {
			numTransactions++
			if numTransactions%100000 == 0 {
				fmt.Print(".")
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
		took = time.Since(startedAt) - took
		klog.Infof("Finished iterating transactions in %s; found %d transactions", took, numTransactions)
		return
	}
	if false {
		f, err := os.Open(os.Args[1])
		if err != nil {
			klog.Exit(err.Error())
		}
		defer f.Close()

		rd, err := car.NewCarReader(f)
		if err != nil {
			klog.Exitf("Failed to open CAR: %s", err)
		}

		startedAt := time.Now()
		numBlocks := 0
		defer func() {
			klog.Infof("Finished in %s", time.Since(startedAt))
			klog.Infof("Read %d nodes", numBlocks)
		}()
		for {
			block, err := rd.Next()
			if errors.Is(err, io.EOF) {
				fmt.Println("EOF")
				break
			}
			numBlocks++
			if numBlocks%100000 == 0 {
				fmt.Print(".")
			}
			if true {
				if block.RawData()[1] != byte(iplddecoders.KindTransaction) {
					continue
				}
				decoded, err := iplddecoders.DecodeTransaction(block.RawData())
				if err != nil {
					continue
				}
				_ = decoded
				fmt.Printf("CID=%s Multicodec=%#x\n", block.Cid(), block.Cid().Type())
				// spew.Dump(decoded)
				// spew.Dump(bin.FormatByteSlice(block.RawData()))
			}
		}
		return
	}
	klog.Infof("Creating traverser for %s", carPath)
	t, err := NewTraverser(carPath)
	if err != nil {
		panic(err)
	}
	defer t.Close()
	klog.Infof("Created traverser for %s", carPath)
	{
		c := cid.MustParse("bafyreibexiyiqrmxhwq5twn4rvkahb7eybqeemmdemluui2p2l5ga4hwu4")
		t.id.GetAll(c, func(u uint64) bool {
			spew.Dump(u)
			return true
		})
		return
	}
	klog.Info("Building graph...")
	if err := t.BuildGraph(); err != nil {
		panic(err)
	}
	klog.Infof("Graph built successfully")

	klog.Info("Traversing blocks	...")
	numTransactions := 0
	defer func() {
		klog.Infof("Traversed %d transactions", numTransactions)
	}()

	t.TraverseBlocks(func(block BlockDAG) bool {
		klog.Infof("Traversing entries...")
		for _, entry := range block.Entries {
			for _, txDAGNode := range entry.Transactions {
				// spew.Dump(txNode)
				numTransactions++
				rawNode, err := t.Get(context.TODO(), txDAGNode.CID)
				if err != nil {
					panic(err)
				}
				parsedNode, err := iplddecoders.DecodeTransaction(rawNode.RawData())
				if err != nil {
					panic(err)
				}
				{
					var tx solana.Transaction
					if err := bin.UnmarshalBin(&tx, parsedNode.Data); err != nil {
						panic(err)
					} else if len(tx.Signatures) == 0 {
						panic("no signatures")
					}
					fmt.Println(tx.Signatures[0].String())
				}
				{
					status, err := blockstore.ParseTransactionStatusMeta(parsedNode.Metadata)
					if err != nil {
						panic(err)
					}
					_ = status
					// spew.Dump(status)
				}
			}
		}
		return true
	})
	klog.Infof("Slots traversed successfully")
}
