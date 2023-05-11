package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipld/go-car"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/iplddecoders"
	"go.firedancer.io/radiance/pkg/blockstore"
	"k8s.io/klog/v2"
)

func main() {
	carPath := os.Args[1]
	file, err := os.Open(carPath)
	if err != nil {
		klog.Exit(err.Error())
	}
	defer file.Close()

	rd, err := car.NewCarReader(file)
	if err != nil {
		klog.Exitf("Failed to open CAR: %s", err)
	}

	startedAt := time.Now()
	numNodes := 0
	defer func() {
		klog.Infof("Finished in %s", time.Since(startedAt))
		klog.Infof("Read %d nodes from CAR file", numNodes)
	}()
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			fmt.Println("EOF")
			break
		}
		numNodes++
		if numNodes%100000 == 0 {
			fmt.Print(".")
		}
		kind := iplddecoders.Kind(block.RawData()[1])
		fmt.Printf("\nCID=%s Multicodec=%#x Kind=%s\n", block.Cid(), block.Cid().Type(), kind)

		switch kind {
		case iplddecoders.KindTransaction:
			decoded, err := iplddecoders.DecodeTransaction(block.RawData())
			if err != nil {
				panic(err)
			}
			{
				{
					var tx solana.Transaction
					if err := bin.UnmarshalBin(&tx, decoded.Data); err != nil {
						panic(err)
					} else if len(tx.Signatures) == 0 {
						panic("no signatures")
					}
					fmt.Println("sig=", tx.Signatures[0].String())
				}
				spew.Dump(decoded)
				{
					status, err := blockstore.ParseTransactionStatusMeta(decoded.Metadata)
					if err != nil {
						panic(err)
					}
					_ = status
					spew.Dump(status)
				}
			}
		case iplddecoders.KindEntry:
			decoded, err := iplddecoders.DecodeEntry(block.RawData())
			if err != nil {
				panic(err)
			}
			spew.Dump(decoded)
		case iplddecoders.KindBlock:
			decoded, err := iplddecoders.DecodeBlock(block.RawData())
			if err != nil {
				panic(err)
			}
			spew.Dump(decoded)
		case iplddecoders.KindSubset:
			decoded, err := iplddecoders.DecodeSubset(block.RawData())
			if err != nil {
				panic(err)
			}
			spew.Dump(decoded)
		case iplddecoders.KindEpoch:
			decoded, err := iplddecoders.DecodeEpoch(block.RawData())
			if err != nil {
				panic(err)
			}
			spew.Dump(decoded)
		default:
			panic("unknown kind" + kind.String())
		}
	}
	klog.Infof("CAR file traversed successfully")
}
