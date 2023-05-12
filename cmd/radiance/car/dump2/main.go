package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipld/go-car"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/iplddecoders"
	"go.firedancer.io/radiance/pkg/blockstore"
	"k8s.io/klog/v2"
)

type intSlice []int

// has
func (s intSlice) has(v int) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}

func (s intSlice) empty() bool {
	return len(s) == 0
}

func main() {
	var flagPrintFilter string
	var printID bool
	var prettyPrintTransactions bool
	flag.StringVar(&flagPrintFilter, "print", "", "print only nodes of these kinds (comma-separated)")
	flag.BoolVar(&printID, "id", false, "print only the CID of the nodes")
	flag.BoolVar(&prettyPrintTransactions, "pretty", false, "pretty print transactions")
	flag.Parse()
	filter := make(intSlice, 0)
	// parse slice of ints from flagPrintFilter
	{
		if flagPrintFilter != "" {
			for _, v := range flagPrintFilter {
				if v == ',' {
					continue
				}
				parsed, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					panic(err)
				}
				filter = append(filter, int(parsed))
			}
		}
	}

	carPath := flag.Arg(0)
	var file fs.File
	var err error
	if carPath == "-" {
		file = os.Stdin
	} else {
		file, err = os.Open(carPath)
		if err != nil {
			klog.Exit(err.Error())
		}
		defer file.Close()
	}

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
	dotEvery := 100_000
	klog.Infof("A dot is printed every %d nodes", dotEvery)
	if filter.empty() {
		klog.Info("Will print all nodes")
	} else {
		klog.Info("Will print only nodes of these kinds: ")
		for _, v := range filter {
			klog.Infof("- %s", iplddecoders.Kind(v).String())
		}
	}
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			fmt.Println("EOF")
			break
		}
		numNodes++
		if numNodes%dotEvery == 0 {
			fmt.Print(".")
		}
		kind := iplddecoders.Kind(block.RawData()[1])
		if printID {
			fmt.Printf("\nCID=%s Multicodec=%#x Kind=%s\n", block.Cid(), block.Cid().Type(), kind)
		}

		switch kind {
		case iplddecoders.KindTransaction:
			decoded, err := iplddecoders.DecodeTransaction(block.RawData())
			if err != nil {
				panic(err)
			}
			{
				var tx solana.Transaction
				if err := bin.UnmarshalBin(&tx, decoded.Data); err != nil {
					panic(err)
				} else if len(tx.Signatures) == 0 {
					panic("no signatures")
				}
				if filter.has(int(iplddecoders.KindTransaction)) || filter.empty() {
					fmt.Println("sig=", tx.Signatures[0].String())
					spew.Dump(decoded)
					if prettyPrintTransactions {
						fmt.Println(tx.String())
					}
				}
				{
					status, err := blockstore.ParseTransactionStatusMeta(decoded.Metadata)
					if err != nil {
						panic(err)
					}
					if filter.has(int(iplddecoders.KindTransaction)) || filter.empty() {
						spew.Dump(status)
					}
				}
			}
		case iplddecoders.KindEntry:
			decoded, err := iplddecoders.DecodeEntry(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindEntry)) || filter.empty() {
				spew.Dump(decoded)
			}
		case iplddecoders.KindBlock:
			decoded, err := iplddecoders.DecodeBlock(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindBlock)) || filter.empty() {
				spew.Dump(decoded)
			}
		case iplddecoders.KindSubset:
			decoded, err := iplddecoders.DecodeSubset(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindSubset)) || filter.empty() {
				spew.Dump(decoded)
			}
		case iplddecoders.KindEpoch:
			decoded, err := iplddecoders.DecodeEpoch(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindEpoch)) || filter.empty() {
				spew.Dump(decoded)
			}
		default:
			panic("unknown kind" + kind.String())
		}
	}
	klog.Infof("CAR file traversed successfully")
}
