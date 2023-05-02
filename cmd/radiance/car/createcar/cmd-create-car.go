//go:build !lite

package createcar

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/linxGnu/grocksdb"
	"github.com/spf13/cobra"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/iostats"
	"go.firedancer.io/radiance/pkg/shred"
	"go.firedancer.io/radiance/third_party/solana_proto/confirmed_block"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

var Cmd = cobra.Command{
	Use:   "create2 <epoch>",
	Short: "Create CAR file from blockstore",
	Long: "Extracts Solana ledger data from blockstore (RocksDB) databases,\n" +
		"and outputs one IPLD CAR (content-addressable archives).\n" +
		"\n" +
		"The DAG contained in the CAR is deterministic.",
	Args: cobra.ExactArgs(1),
}

var flags = Cmd.Flags()

var (
	flagWorkers          = flags.UintP("workers", "w", uint(runtime.NumCPU()), "Number of goroutines to verify with")
	flagOut              = flags.StringP("out", "o", "", "Output directory")
	flagDBs              = flags.StringArray("db", nil, "Path to RocksDB (can be specified multiple times)")
	flagRequireFullEpoch = flags.Bool("require-full-epoch", true, "Require all blocks in epoch to be present")
	flagLimitSlots       = flags.Uint64("limit-slots", 0, "Limit number of slots to process")
)

func init() {
	Cmd.Run = run
}

func init() {
	spew.Config.DisableMethods = true
	spew.Config.DisablePointerMethods = true
	spew.Config.MaxDepth = 5
}

func run(c *cobra.Command, args []string) {
	start := time.Now()

	defer func() {
		timeTaken := time.Since(start)
		klog.Infof("Time taken: %s", timeTaken)
	}()
	workers := *flagWorkers
	if workers == 0 {
		workers = uint(runtime.NumCPU())
	}

	finalCARFilepath := filepath.Clean(*flagOut)
	epochStr := args[0]
	epoch, err := strconv.ParseUint(epochStr, 10, 32)
	if err != nil {
		klog.Exitf("Invalid epoch arg: %s", epochStr)
	}
	klog.Infof(
		"Flags: out=%s epoch=%d require-full-epoch=%t limit-slots=%v dbs=%v",
		finalCARFilepath,
		epoch,
		*flagRequireFullEpoch,
		*flagLimitSlots,
		*flagDBs,
	)

	// Open blockstores
	dbPaths := *flagDBs
	handles := make([]blockstore.WalkHandle, len(*flagDBs))
	for i := range handles {
		var err error
		handles[i].DB, err = blockstore.OpenReadOnly(dbPaths[i])
		if err != nil {
			klog.Exitf("Failed to open blockstore at %s: %s", dbPaths[i], err)
		}
	}

	// Create new walker object
	walker, err := blockstore.NewBlockWalk(handles, 2 /*TODO*/)
	if err != nil {
		klog.Exitf("Failed to create multi-DB iterator: %s", err)
	}
	defer walker.Close()

	multi, err := NewMultistage(finalCARFilepath)
	if err != nil {
		panic(err)
	}
	multi.SetConcurrency(workers)

	// TODO:
	// - create a slot tracker
	// - iterate over all slots in the DB, and
	//   - create a temporary CAR file for each slot and write all entries to it (appropriately encoded)
	//   - mark the new status of the slot in the tracker
	// - once all slots are processed, iterate over the tracker and check that all slots are marked as "done"
	// - if not, then we have a problem
	// - if yes, then:
	//   - create a new CAR file for the entire DB
	//   - iterate over all slot CAR files and create the DAG (nested callbacks)
	wg := new(errgroup.Group)
	wg.SetLimit(int(workers))
	callback := func(slotMeta *blockstore.SlotMeta, entries [][]shred.Entry, txMetas []*confirmed_block.TransactionStatusMeta) error {
		wg.Go(func() error {
			_, err = multi.OnBlock(slotMeta, entries, txMetas)
			if err != nil {
				panic(fmt.Errorf("fatal error while processing slot %d: %w", slotMeta.Slot, err))
			}
			for _, txMeta := range txMetas {
				if txMeta != nil {
					txMeta = nil
				}
			}
			return nil
		})
		return nil
	}
	// Create new cargen worker.
	w, err := NewIterator(
		epoch,
		walker,
		*flagRequireFullEpoch,
		*flagLimitSlots,
		callback,
	)
	if err != nil {
		klog.Exitf("Failed to init cargen: %s", err)
	}

	ctx := c.Context()
	if err = w.Run(ctx); err != nil {
		klog.Exitf("FATAL: %s", err)
	}
	if err = wg.Wait(); err != nil {
		klog.Exitf("FATAL: %s", err)
	}
	klog.Infof("Finalizing DAG in the CAR file for epoch %d, at path: %s", epoch, finalCARFilepath)
	epochCID, err := multi.FinalizeDAG(epoch)
	if err != nil {
		panic(err)
	}
	klog.Infof("Root of the DAG (epoch CID): %s", epochCID)
	klog.Info("DONE")

	timeTaken := time.Since(start)
	klog.Infof("Time taken: %s", timeTaken)
	{
		numBytesReadFromDisk, err := iostats.GetDiskReadBytes()
		if err != nil {
			panic(err)
		}
		klog.Infof(
			"This process read %d bytes (%s) from disk (%v/s)",
			numBytesReadFromDisk,
			humanize.Bytes(numBytesReadFromDisk),
			humanize.Bytes(uint64(float64(numBytesReadFromDisk)/timeTaken.Seconds())),
		)
		numBytesWrittenToDisk, err := iostats.GetDiskWriteBytes()
		if err != nil {
			panic(err)
		}
		klog.Infof(
			"This process wrote %d bytes (%s) to disk (%v/s)",
			numBytesWrittenToDisk,
			humanize.Bytes(numBytesWrittenToDisk),
			humanize.Bytes(uint64(float64(numBytesWrittenToDisk)/timeTaken.Seconds())),
		)
	}
	time.Sleep(1 * time.Second)
	os.Exit(0)
}

// slotBounds returns the lowest and highest available slots in the meta table.
func slotBounds(db *blockstore.DB) (low uint64, high uint64, ok bool) {
	iter := db.DB.NewIteratorCF(grocksdb.NewDefaultReadOptions(), db.CfMeta)
	defer iter.Close()

	iter.SeekToFirst()
	if ok = iter.Valid(); !ok {
		return
	}
	low, ok = blockstore.ParseSlotKey(iter.Key().Data())
	if !ok {
		return
	}

	iter.SeekToLast()
	if ok = iter.Valid(); !ok {
		return
	}
	high, ok = blockstore.ParseSlotKey(iter.Key().Data())
	high++
	return
}
