//go:build !lite

package createcar

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/linxGnu/grocksdb"
	"github.com/minio/sha256-simd"
	"github.com/spf13/cobra"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/iostats"
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
	flagWorkers                         = flags.UintP("workers", "w", uint(runtime.NumCPU()), "Number of goroutines to verify with")
	flagOut                             = flags.StringP("out", "o", "", "Output directory")
	flagDBs                             = flags.StringArray("db", nil, "Path to RocksDB (can be specified multiple times)")
	flagRequireFullEpoch                = flags.Bool("require-full-epoch", true, "Require all blocks in epoch to be present")
	flagLimitSlots                      = flags.Uint64("limit-slots", 0, "Limit number of slots to process")
	flagSkipHash                        = flags.Bool("skip-hash", false, "Skip hashing the final CAR file after the generation is complete (for debugging)")
	flagShredRevision                   = flags.Int("shred-revision", 2, "Shred revision to use (2 = latest)")
	flagNextShredRevisionActivationSlot = flags.Uint64("next-shred-revision-activation-slot", 0, "Next shred revision activation slot")
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
	{
		// try using the hardware-accelerated SHA256 implementation.
		// if it fails, better to fail early than to fail after processing.
		h := sha256.New()
		h.Write([]byte("test"))
		if hex.EncodeToString(h.Sum(nil)) != "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" {
			klog.Exitf("SHA256 hardware-acceleration is not working")
		}
	}
	start := time.Now()

	defer func() {
		timeTaken := time.Since(start)
		klog.Infof("Time taken: %s", timeTaken)
	}()
	numWorkers := *flagWorkers
	if numWorkers == 0 {
		numWorkers = uint(runtime.NumCPU())
	}

	finalCARFilepath := filepath.Clean(*flagOut)
	epochStr := args[0]
	epoch, err := strconv.ParseUint(epochStr, 10, 32)
	if err != nil {
		klog.Exitf("Invalid epoch arg: %s", epochStr)
	}
	klog.Infof(
		"Flags: out=%s epoch=%d require-full-epoch=%t limit-slots=%v dbs=%v workers=%d skip-hash=%t shred-revision=%d next-shred-revision-activation-slot=%d",
		finalCARFilepath,
		epoch,
		*flagRequireFullEpoch,
		*flagLimitSlots,
		*flagDBs,
		*flagWorkers,
		*flagSkipHash,
		*flagShredRevision,
		*flagNextShredRevisionActivationSlot,
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
	walker, err := blockstore.NewBlockWalkWithNextShredRevisionActivationSlot(
		handles,
		*flagShredRevision,
		*flagNextShredRevisionActivationSlot,
	)
	if err != nil {
		klog.Exitf("Failed to create multi-DB iterator: %s", err)
	}
	defer walker.Close()

	multi, err := NewMultistage(
		finalCARFilepath,
		numWorkers,
		walker,
	)
	if err != nil {
		panic(err)
	}

	hadFirstSlot := false
	latestSlot := uint64(0)
	latestDB := int(0) // 0 is the first DB
	callback := func(slotMeta *blockstore.SlotMeta, latestDBIndex int) error {
		if slotMeta.Slot > latestSlot || slotMeta.Slot == 0 {
			if !hadFirstSlot {
				hadFirstSlot = true
				klog.Infof("Started processing DB %d from slot %d", latestDBIndex, slotMeta.Slot)
			}
			if latestDBIndex != latestDB {
				klog.Infof("Switched to DB %d; started processing new DB from slot %d (prev: %d)", latestDBIndex, slotMeta.Slot, latestSlot)
				// TODO: warn if we skipped slots
				if slotMeta.Slot > latestSlot+1 {
					klog.Warningf(
						"Detected skipped slots %d to %d after DB switch (last slot of previous DB: %d); started processing new DB from slot %d",
						latestSlot+1,
						slotMeta.Slot-1,
						latestSlot,
						slotMeta.Slot,
					)
				}
			}
			latestSlot = slotMeta.Slot
			latestDB = latestDBIndex
		} else if slotMeta.Slot < latestSlot {
			if latestDBIndex == latestDB {
				panic(fmt.Errorf("slot %d is out of order (previous processed slot was: %d)", slotMeta.Slot, latestSlot))
			} else {
				// TODO: remove
				// we switched to another DB; print warning and return
				// klog.Infof("New DB; slot %d was supposedly already processed (previous processed slot was: %d); skipping", slotMeta.Slot, latestSlot)
				return nil
			}
		} else {
			// slotMeta.Slot == latestSlot
			if slotMeta.Slot != 0 && latestDBIndex == latestDB {
				panic(fmt.Errorf("slot %d is already processed", slotMeta.Slot))
			} else {
				// we switched to another DB; print warning and return
				// klog.Infof("New DB; slot %d is already processed; skipping", slotMeta.Slot)
				return nil
			}
		}
		err = multi.OnSlotFromDB(slotMeta)
		if err != nil {
			panic(fmt.Errorf("fatal error while processing slot %d: %w", slotMeta.Slot, err))
		}
		return nil
	}
	wrk, err := NewIterator(
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
	if err = wrk.Run(ctx); err != nil {
		klog.Exitf("FATAL: %s", err)
	}
	klog.Infof("Finalizing DAG in the CAR file for epoch %d, at path: %s", epoch, finalCARFilepath)
	epochCID, err := multi.FinalizeDAG(epoch)
	if err != nil {
		panic(err)
	}
	klog.Infof("Root of the DAG (Epoch CID): %s", epochCID)
	klog.Infof("Done. Completed CAR file generation in %s", time.Since(start))

	{
		epochCidString := epochCID.(cidlink.Link).Cid.String()
		// save the epoch CID to a file, in the format {epoch}.cid
		epochCIDFilepath := filepath.Join(filepath.Dir(finalCARFilepath), fmt.Sprintf("%d.cid", epoch))
		klog.Infof("Saving epoch CID to file: %s", epochCIDFilepath)
		err := os.WriteFile(epochCIDFilepath, []byte(epochCidString+"\n"), 0o644)
		if err != nil {
			klog.Warningf("Failed to save epoch CID to file: %s", err)
		}
	}
	{
		klog.Info("---")
		// print the size of each DB directory
		var totalSize uint64
		hadError := false
		for _, dbPath := range dbPaths {
			dbPath = filepath.Clean(dbPath)
			dbSize, err := getDirSize(dbPath)
			if err != nil {
				hadError = true
				klog.Errorf("Failed to get size of DB %s: %s", dbPath, err)
				continue
			}
			totalSize += dbSize
			klog.Infof("DB %s size: %s", dbPath, humanize.Bytes(uint64(dbSize)))
		}
		if hadError {
			klog.Warning("Failed to get size of one or more DBs")
		}
		klog.Infof("Total DBs size: %s", humanize.Bytes(totalSize))
	}

	{
		klog.Info("---")
		timeTakenUntilAfterCARFinalization := time.Since(start)
		numBytesReadFromDisk, err := iostats.GetDiskReadBytes()
		if err != nil {
			panic(err)
		}
		klog.Infof(
			"This process read %d bytes (%s) from disk (%v/s)",
			numBytesReadFromDisk,
			humanize.Bytes(numBytesReadFromDisk),
			humanize.Bytes(uint64(float64(numBytesReadFromDisk)/timeTakenUntilAfterCARFinalization.Seconds())),
		)
		numBytesWrittenToDisk, err := iostats.GetDiskWriteBytes()
		if err != nil {
			panic(err)
		}
		klog.Infof(
			"This process wrote %d bytes (%s) to disk (%v/s)",
			numBytesWrittenToDisk,
			humanize.Bytes(numBytesWrittenToDisk),
			humanize.Bytes(uint64(float64(numBytesWrittenToDisk)/timeTakenUntilAfterCARFinalization.Seconds())),
		)
	}

	if !*flagSkipHash {
		hashStartedAt := time.Now()
		klog.Info("---")
		klog.Info("Calculating SHA256 hash of CAR file...")
		gotHash, err := hashFileSha256(finalCARFilepath)
		if err != nil {
			klog.Infof("Failed to hash CAR file: %s", err)
		} else {
			klog.Infof("CAR file SHA256 hash: %s (took %s)", gotHash, time.Since(hashStartedAt))
		}
	} else {
		klog.Info("Skipping hashing the CAR file")
	}

	timeTaken := time.Since(start)
	klog.Info("---")
	klog.Infof("Total time taken: %s", timeTaken)

	time.Sleep(1 * time.Second)
	os.Exit(0)
}

// slotBounds returns the lowest and highest available slots in the meta table.
func slotBounds(db *blockstore.DB) (low uint64, high uint64, ok bool) {
	opts := grocksdb.NewDefaultReadOptions()
	opts.SetVerifyChecksums(false)
	opts.SetFillCache(false)
	iter := db.DB.NewIteratorCF(opts, db.CfMeta)
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

func hashFileSha256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	fmt.Println(hex.EncodeToString(h.Sum(nil)))
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func fileSize(path string) (uint64, error) {
	st, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return uint64(st.Size()), nil
}

func getDirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}
