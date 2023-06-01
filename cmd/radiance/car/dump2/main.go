package main

import (
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	"github.com/klauspost/compress/zstd"
	"go.firedancer.io/radiance/cmd/radiance/car/createcar/ipld/ipldbindcode"
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
	var limit int
	flag.StringVar(&flagPrintFilter, "print", "", "print only nodes of these kinds (comma-separated)")
	flag.BoolVar(&printID, "id", false, "print only the CID of the nodes")
	flag.BoolVar(&prettyPrintTransactions, "pretty", false, "pretty print transactions")
	flag.IntVar(&limit, "limit", 0, "limit the number of nodes to print")
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
	// print roots:
	{
		roots := rd.Header.Roots
		klog.Infof("Roots: %d", len(roots))
		for i, root := range roots {
			if i == 0 && len(roots) == 1 {
				klog.Infof("- %s (Epoch CID)", root.String())
			} else {
				klog.Infof("- %s", root.String())
			}
		}
	}

	startedAt := time.Now()
	numNodesSeen := 0
	numNodesPrinted := 0
	defer func() {
		klog.Infof("Finished in %s", time.Since(startedAt))
		klog.Infof("Read %d nodes from CAR file", numNodesSeen)
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
	nodeKindCounts := make(map[iplddecoders.Kind]int64)
	nodeKindTotalSizes := make(map[iplddecoders.Kind]uint64)
	for {
		block, err := rd.Next()
		if errors.Is(err, io.EOF) {
			fmt.Println("EOF")
			break
		}
		numNodesSeen++
		if numNodesSeen%dotEvery == 0 {
			fmt.Print(".")
		}
		if limit > 0 && numNodesPrinted >= limit {
			break
		}
		kind := iplddecoders.Kind(block.RawData()[1])
		nodeKindCounts[kind]++
		nodeKindTotalSizes[kind] += uint64(len(block.RawData()))

		doPrint := filter.has(int(kind)) || filter.empty()
		if doPrint {
			fmt.Printf("\nCID=%s Multicodec=%#x Kind=%s\n", block.Cid(), block.Cid().Type(), kind)
		} else {
			continue
		}

		switch kind {
		case iplddecoders.KindTransaction:
			decoded, err := iplddecoders.DecodeTransaction(block.RawData())
			if err != nil {
				panic(err)
			}
			if decoded.Data.Total == 1 {
				completeData := decoded.Data.Data
				var tx solana.Transaction
				if err := bin.UnmarshalBin(&tx, completeData); err != nil {
					panic(err)
				} else if len(tx.Signatures) == 0 {
					panic("no signatures")
				}
				{
					fmt.Println("sig=" + tx.Signatures[0].String())
					spew.Dump(decoded)
					if prettyPrintTransactions {
						fmt.Println(tx.String())
					}
					numNodesPrinted++
				}
			} else {
				{
					fmt.Println("transaction data is split into multiple blocks; skipping printing")
				}
			}
			if decoded.Metadata.Total == 1 {
				completeBuffer := decoded.Metadata.Data
				if len(completeBuffer) > 0 {
					uncompressedMeta, err := decompressZstd(completeBuffer)
					if err != nil {
						panic(err)
					}
					status, err := blockstore.ParseAnyTransactionStatusMeta(uncompressedMeta)
					if err != nil {
						panic(err)
					}
					{
						spew.Dump(status)
					}
				}
			} else {
				{
					fmt.Println("transaction metadata is split into multiple blocks; skipping printing")
				}
			}
		case iplddecoders.KindEntry:
			decoded, err := iplddecoders.DecodeEntry(block.RawData())
			if err != nil {
				panic(err)
			}
			{
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindBlock:
			decoded, err := iplddecoders.DecodeBlock(block.RawData())
			if err != nil {
				panic(err)
			}
			{
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindSubset:
			decoded, err := iplddecoders.DecodeSubset(block.RawData())
			if err != nil {
				panic(err)
			}
			{
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindEpoch:
			decoded, err := iplddecoders.DecodeEpoch(block.RawData())
			if err != nil {
				panic(err)
			}
			{
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindRewards:
			decoded, err := iplddecoders.DecodeRewards(block.RawData())
			if err != nil {
				panic(err)
			}

			{
				spew.Dump(decoded)
				numNodesPrinted++

				if decoded.Data.Total == 1 {
					completeBuffer := decoded.Data.Data
					if len(completeBuffer) > 0 {
						uncompressedRewards, err := decompressZstd(completeBuffer)
						if err != nil {
							panic(err)
						}
						// try decoding as protobuf
						parsed, err := blockstore.ParseRewards(uncompressedRewards)
						if err != nil {
							fmt.Println("Rewards are not protobuf: " + err.Error())
						} else {
							spew.Dump(parsed)
						}
					}
				} else {
					fmt.Println("rewards data is split into multiple blocks; skipping printing")
				}
			}
		case iplddecoders.KindDataFrame:
			decoded, err := iplddecoders.DecodeDataFrame(block.RawData())
			if err != nil {
				panic(err)
			}
			spew.Dump(decoded)
		default:
			panic("unknown kind: " + kind.String())
		}
	}
	fmt.Println()
	{ // get keys so can iterate in order
		var nodeKinds []iplddecoders.Kind
		for kind := range nodeKindCounts {
			nodeKinds = append(nodeKinds, kind)
		}
		sort.Slice(nodeKinds, func(i, j int) bool {
			return nodeKinds[i] < nodeKinds[j]
		})
		fmt.Println("Node kind counts:")
		for _, kind := range nodeKinds {
			fmt.Printf("	%s: %s items\n",
				forceStringLengthWithPadding(kind.String(), len("Transaction")),
				humanize.Comma(nodeKindCounts[kind]),
			)
		}
		fmt.Println()
	}
	// calculate total size of all blocks
	var totalSize uint64
	for _, size := range nodeKindTotalSizes {
		totalSize += size
	}
	fmt.Println()
	{
		var nodeKinds []iplddecoders.Kind
		for kind := range nodeKindTotalSizes {
			nodeKinds = append(nodeKinds, kind)
		}
		sort.Slice(nodeKinds, func(i, j int) bool {
			return nodeKinds[i] < nodeKinds[j]
		})
		fmt.Println("Node kind total sizes:")

		for _, kind := range nodeKinds {
			percentage := float64(nodeKindTotalSizes[kind]) / float64(totalSize) * 100
			fmt.Printf(
				"	%s: %d bytes (%s), %.2f%% of total\n",
				forceStringLengthWithPadding(kind.String(), len("Transaction")),
				nodeKindTotalSizes[kind],
				humanize.Bytes((nodeKindTotalSizes[kind])),
				percentage,
			)
		}
	}

	klog.Infof("CAR file traversed successfully")
}

func forceStringLengthWithPadding(str string, length int) string {
	if len(str) > length {
		return str[:length]
	}
	return str + strings.Repeat(" ", length-len(str))
}

type CidToDataFrame struct {
	Cid       cid.Cid
	DataFrame ipldbindcode.DataFrame
}

var decoder, _ = zstd.NewReader(nil)

func decompressZstd(data []byte) ([]byte, error) {
	return decoder.DecodeAll(data, nil)
}

func hashBytes(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}
