package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
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
	hashToFrames := make(map[int][]CidToDataFrame)
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
		if printID {
			fmt.Printf("\nCID=%s Multicodec=%#x Kind=%s\n", block.Cid(), block.Cid().Type(), kind)
		}

		switch kind {
		case iplddecoders.KindTransaction:
			decoded, err := iplddecoders.DecodeTransaction(block.RawData())
			if err != nil {
				panic(err)
			}
			doPrint := filter.has(int(iplddecoders.KindTransaction)) || filter.empty()
			if decoded.Data.Total == 1 {
				completeData := decoded.Data.Data
				var tx solana.Transaction
				if err := bin.UnmarshalBin(&tx, completeData); err != nil {
					panic(err)
				} else if len(tx.Signatures) == 0 {
					panic("no signatures")
				}
				if doPrint {
					fmt.Println("sig=" + tx.Signatures[0].String())
					spew.Dump(decoded)
					if prettyPrintTransactions {
						fmt.Println(tx.String())
					}
					numNodesPrinted++
				}
			} else {
				if doPrint {
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
					if doPrint {
						spew.Dump(status)
					}
				}
			} else {
				if doPrint {
					fmt.Println("transaction metadata is split into multiple blocks; skipping printing")
				}
			}
		case iplddecoders.KindEntry:
			decoded, err := iplddecoders.DecodeEntry(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindEntry)) || filter.empty() {
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindBlock:
			decoded, err := iplddecoders.DecodeBlock(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindBlock)) || filter.empty() {
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindSubset:
			decoded, err := iplddecoders.DecodeSubset(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindSubset)) || filter.empty() {
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindEpoch:
			decoded, err := iplddecoders.DecodeEpoch(block.RawData())
			if err != nil {
				panic(err)
			}
			if filter.has(int(iplddecoders.KindEpoch)) || filter.empty() {
				spew.Dump(decoded)
				numNodesPrinted++
			}
		case iplddecoders.KindRewards:
			decoded, err := iplddecoders.DecodeRewards(block.RawData())
			if err != nil {
				panic(err)
			}
			doPrint := filter.has(int(iplddecoders.KindRewards)) || filter.empty()
			if doPrint {
				spew.Dump(decoded)
				numNodesPrinted++

				hashToFrames[decoded.Data.Hash] = append(hashToFrames[decoded.Data.Hash], CidToDataFrame{
					Cid:       cid.Undef,
					DataFrame: decoded.Data,
				})

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
			// spew.Dump(decoded)

			hashToFrames[decoded.Hash] = append(hashToFrames[decoded.Hash], CidToDataFrame{
				Cid:       block.Cid(),
				DataFrame: *decoded,
			})

		default:
			panic("unknown kind: " + kind.String())
		}
	}
	klog.Infof("CAR file traversed successfully")

	{
		// now try to put the frames back together
		for hash, frames := range hashToFrames {
			// sort by index
			sort.Slice(frames, func(i, j int) bool {
				return frames[i].DataFrame.Index < frames[j].DataFrame.Index
			})
			{
				for _, frame := range frames {
					frame.DataFrame.Data = nil
					spew.Dump(frame)
				}
				fmt.Println("----------------------------------------------------------------------------------------")
				continue
				// return
			}
			var buf bytes.Buffer
			for _, frame := range frames {
				buf.Write(frame.DataFrame.Data)
			}
			// rehash
			recomputedHash := hashBytes(buf.Bytes())
			if uint64(hash) != recomputedHash {
				panic("hash mismatch")
			} else {
				fmt.Println("hash matches")
			}
			// now check if the reported next cids match the actual next cids
			actualOrderOfCids := make([]cid.Cid, 0, len(frames))
			for _, frame := range frames {
				if frame.Cid == cid.Undef {
					continue
				}
				actualOrderOfCids = append(actualOrderOfCids, frame.Cid)
			}
			wantedOrderOfCids := make([]cid.Cid, 0, len(frames))
			for _, frame := range frames {
				for _, nextCid := range frame.DataFrame.Next {
					wantedOrderOfCids = append(wantedOrderOfCids, nextCid.(cidlink.Link).Cid)
				}
			}
			if !reflect.DeepEqual(actualOrderOfCids, wantedOrderOfCids) {
				spew.Dump(actualOrderOfCids)
				spew.Dump(wantedOrderOfCids)
				panic("next cids mismatch")
			} else {
				fmt.Println("next cids match")
			}
		}
	}
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
