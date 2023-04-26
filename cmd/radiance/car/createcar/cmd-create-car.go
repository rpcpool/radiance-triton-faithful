//go:build !lite

package createcar

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/klauspost/compress/zstd"
	"github.com/linxGnu/grocksdb"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/iostats"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

var Cmd = cobra.Command{
	Use:   "verify-data <rocksdb>",
	Short: "Verify ledger data integrity",
	Long: "Iterates through all data shreds and performs sanity checks.\n" +
		"Useful for checking the correctness of the Radiance implementation.\n" +
		"\n" +
		"Scans through the data-shreds column family with multiple threads (divide-and-conquer).",
	Args: cobra.ExactArgs(1),
}

var flags = Cmd.Flags()

var (
	flagWorkers  = flags.UintP("workers", "w", uint(runtime.NumCPU()), "Number of goroutines to verify with")
	flagMaxErrs  = flags.Uint32("max-errors", 100, "Abort after N errors")
	flagStatIvl  = flags.Duration("stat-interval", 5*time.Second, "Stats interval")
	flagDumpSigs = flags.Bool("dump-sigs", false, "Print first signature of each transaction")
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

	workers := *flagWorkers
	if workers == 0 {
		workers = uint(runtime.NumCPU())
	}
	rocksDB := args[0]
	db, err := blockstore.OpenReadOnly(rocksDB)
	if err != nil {
		klog.Exitf("Failed to open blockstore: %s", err)
	}
	defer db.Close()

	// total amount of slots
	slotLo, slotHi, ok := slotBounds(db)
	if !ok {
		klog.Exitf("Cannot find slot boundaries")
	}
	if slotLo > slotHi {
		panic("wtf: slotLo > slotHi")
	}
	total := slotHi - slotLo
	klog.Infof("Verifying %d slots", total)

	// per-worker amount of slots
	step := total / uint64(workers)
	if step == 0 {
		step = 1
	}
	cursor := slotLo
	klog.Infof("Slots per worker: %d", step)

	// stats trackers
	var numSuccess atomic.Uint64
	var numSkipped atomic.Uint64
	var numFailure atomic.Uint32
	var numTxns atomic.Uint64

	// application lifetime
	rootCtx := c.Context()
	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	txRate := ewma.NewMovingAverage(7)
	lastStatsUpdate := time.Now()
	var lastNumTxns uint64
	updateEWMA := func() {
		now := time.Now()
		sinceLast := now.Sub(lastStatsUpdate)
		curNumTxns := numTxns.Load()
		increase := curNumTxns - lastNumTxns
		iRate := float64(increase) / sinceLast.Seconds()
		txRate.Add(iRate)
		lastNumTxns = curNumTxns
		lastStatsUpdate = now
	}
	stats := func() {
		klog.Infof("[stats] good=%d skipped=%d bad=%d tps=%.0f",
			numSuccess.Load(), numSkipped.Load(), numFailure.Load(), txRate.Value())
	}

	var barOutput io.Writer
	isAtty := isatty.IsTerminal(os.Stderr.Fd())
	if isAtty {
		barOutput = os.Stderr
	} else {
		barOutput = io.Discard
	}

	progress := mpb.NewWithContext(ctx, mpb.WithOutput(barOutput))
	bar := progress.New(int64(total), mpb.BarStyle(),
		mpb.PrependDecorators(
			decor.Spinner(nil),
			decor.CurrentNoUnit(" %d"),
			decor.TotalNoUnit(" / %d slots"),
			decor.NewPercentage(" (% d)"),
		),
		mpb.AppendDecorators(
			decor.Name("eta="),
			decor.AverageETA(decor.ET_STYLE_GO),
		))

	if isAtty {
		klog.LogToStderr(false)
		klog.SetOutput(progress)
	}

	statInterval := *flagStatIvl
	if statInterval > 0 {
		statTicker := time.NewTicker(statInterval)
		rateTicker := time.NewTicker(250 * time.Millisecond)
		go func() {
			defer statTicker.Stop()
			defer rateTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-statTicker.C:
					stats()
				case <-rateTicker.C:
					updateEWMA()
				}
			}
		}()
	}

	maxTxMetaSize := uint64(0)
	numFoundTxMeta := uint64(0)
	numFoundEmtyTxMeta := uint64(0)

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
	callback := func(slotMeta blockstore.SlotMeta, entries []blockstore.Entries) bool {
		keysToBeFound := make([][]byte, 0)

		for _, outer := range entries {
			for _, e := range outer.Entries {
				for _, tx := range e.Txns {
					if len(tx.Signatures) > 0 {
						{
							firstSignature := tx.Signatures[0]
							key := blockstore.FormatTxMetadataKey(slotMeta.Slot, firstSignature)
							keysToBeFound = append(keysToBeFound, key)
						}
					}
				}
			}
		}
		{
			got, err := db.DB.MultiGetCF(grocksdb.NewDefaultReadOptions(), db.CfTxStatus, keysToBeFound...)
			if err != nil {
				panic(err)
			}
			defer got.Destroy()
			{
				for i, key := range keysToBeFound {
					if got[i] == nil {
						panic(fmt.Errorf("tx meta not found for key: %x", key))
					}
					atomic.AddUint64(&numFoundTxMeta, 1)
					metaBytes := got[i].Data()
					if got[i].Size() == 0 {
						atomic.AddUint64(&numFoundEmtyTxMeta, 1)
					}
					if thisSize := got[i].Size(); uint64(thisSize) > atomic.LoadUint64(&maxTxMetaSize) {
						atomic.StoreUint64(&maxTxMetaSize, uint64(thisSize))
						klog.Infof("new maxTxMetaSize: %s", humanize.Bytes(uint64(thisSize)))
					}
					txMeta, err := blockstore.ParseTransactionStatusMeta(metaBytes)
					if err != nil {
						panic(err)
					}
					{
						// TODO: use txMeta
					}
					txMeta.Reset()
				}
			}
		}
		return true
	}

	for i := uint(0); i < workers; i++ {
		// Find segment assigned to worker
		wLo := cursor
		wHi := wLo + step
		if wHi > slotHi || i == workers-1 {
			wHi = slotHi
		}
		cursor = wHi
		if wLo >= wHi {
			break
		}

		klog.Infof("[worker %d]: range=[%d:%d]", i, wLo, wHi)
		w := &worker{
			id:          i,
			bar:         bar,
			stop:        wHi,
			numSuccess:  &numSuccess,
			numSkipped:  &numSkipped,
			numFailures: &numFailure,
			maxFailures: *flagMaxErrs,
			numTxns:     &numTxns,
		}
		w.init(db, wLo)
		group.Go(func() error {
			defer w.close()
			return w.run(ctx, callback)
		})
	}

	err = group.Wait()
	if isAtty {
		klog.Flush()
		klog.SetOutput(os.Stderr)
	}

	var exitCode int
	if err != nil {
		klog.Errorf("Aborting: %s", err)
		exitCode = 1
	} else if err = rootCtx.Err(); err == nil {
		klog.Info("Done!")
		exitCode = 0
	} else {
		klog.Infof("Aborted: %s", err)
		exitCode = 1
	}

	stats()
	timeTaken := time.Since(start)
	klog.Infof("Time taken: %s", timeTaken)
	klog.Infof("Transaction Count: %d (%.2f tps)", numTxns.Load(), float64(numTxns.Load())/timeTaken.Seconds())
	klog.Infof("Transaction Metadata Count: %d", numFoundTxMeta)
	klog.Infof("Empty Transaction Metadata Count: %d", numFoundEmtyTxMeta)
	klog.Infof("Max tx metadata size: %d (%s)", maxTxMetaSize, humanize.Bytes((maxTxMetaSize)))
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
	os.Exit(exitCode)
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

var encoder, _ = zstd.NewWriter(nil,
	// zstd.WithEncoderLevel(zstd.SpeedBestCompression),
	zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
)

// Compress a buffer.
// If you have a destination buffer, the allocation in the call can also be eliminated.
func Compress(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func hunamizeAndRate(sizeOriginal uint64, sizeCompressed uint64) string {
	return fmt.Sprintf("%s (x%.2f)", humanize.Bytes((sizeCompressed)), calcCompressionRate(sizeOriginal, sizeCompressed))
}

func calcCompressionRate(sizeOriginal uint64, sizeCompressed uint64) float64 {
	return float64(sizeOriginal) / float64(sizeCompressed)
}

type WriterCounter struct {
	counter *uint64
}

func NewWriterCounter() *WriterCounter {
	return &WriterCounter{counter: new(uint64)}
}

func (w *WriterCounter) Write(p []byte) (int, error) {
	*w.counter += uint64(len(p))
	return len(p), nil
}

func (w *WriterCounter) Count() uint64 {
	if w.counter == nil {
		return 0
	}
	return *w.counter
}
