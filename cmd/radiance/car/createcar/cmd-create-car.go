//go:build !lite

package createcar

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/gagliardetto/solana-go"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/linxGnu/grocksdb"
	"github.com/minio/sha256-simd"
	"github.com/spf13/cobra"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/iostats"
	"go.firedancer.io/radiance/pkg/slotedges"
	"go.firedancer.io/radiance/pkg/versioninfo"
	"gopkg.in/yaml.v2"
	"k8s.io/klog/v2"
)

var Cmd = cobra.Command{
	Use:     "create2 <epoch>",
	Aliases: []string{"create"},
	Short:   "Create CAR file from blockstore",
	Long: "Extracts Solana ledger data from blockstore (RocksDB) databases,\n" +
		"and outputs one IPLD CAR (content-addressable archives).\n" +
		"\n" +
		"The DAG contained in the CAR is deterministic, as is the generated CAR file.",
	Args: cobra.ExactArgs(1),
}

var flags = Cmd.Flags()

var (
	flagWorkers                         = flags.UintP("workers", "w", uint(runtime.NumCPU()), "Number of workers to use")
	flagOut                             = flags.StringP("out", "o", "", "Output directory")
	flagDBs                             = flags.StringArray("db", nil, "Path to RocksDB (can be specified multiple times)")
	flagRequireFullEpoch                = flags.Bool("require-full-epoch", true, "Require all blocks in epoch to be present")
	flagLimitSlots                      = flags.Uint64("limit-slots", 0, "Limit number of slots to process")
	flagSkipHash                        = flags.Bool("skip-hash", false, "Skip hashing the final CAR file after the generation is complete (for debugging)")
	flagShredRevision                   = flags.Int("shred-revision", 2, "Shred revision to use (2 = latest)")
	flagNextShredRevisionActivationSlot = flags.Uint64("next-shred-revision-activation-slot", 0, "Next shred revision activation slot; maybe depends on when the validator creating the snapshot upgraded to the latest version.")
	flagCheckOnly                       = flags.Bool("check", false, "Only check if the data is available, without creating the CAR file")
	flagStopAtSlot                      = flags.Uint64("stop-at-slot", 0, "Stop processing at this slot, excluding any slots after it")
	optRocksDBVerifyChecksums           = flags.Bool("rocksdb-verify-checksums", true, "Verify checksums of data read from RocksDB")
	//
	flagAllowMissingTxMeta = flags.Bool("allow-missing-tx-meta", false, "Allow missing transaction metadata")
	flagFillTxMetaFromBT   = flags.Bool("enable-backfill", false, "Backfill missing transaction metadata from BigTable")
	flagFillConcurrency    = flags.Uint("backfill-workers", 1, "Number of concurrent requests to make to BigTable for getting blocks for tx metadata backfill")
	fillDBPath             = flags.String("backfill-cache-dir", "", "Path to the dir where to save the blocks fetched from BigTable.")
	preloadBlocks          = flags.String("backfill-preload-blocks", "", "Comma-separated list of blocks to preload for tx meta backfill; can be ranges (e.g., 1-100,200-300) or 'all'")
)

func init() {
	Cmd.Run = run
}

func init() {
	spew.Config.DisableMethods = true
	spew.Config.DisablePointerMethods = true
	spew.Config.MaxDepth = 5
}

func init() {
	// try using the hardware-accelerated SHA256 implementation.
	// if it fails, better to fail early than to fail after processing.
	h := sha256.New()
	h.Write([]byte("test"))
	if hex.EncodeToString(h.Sum(nil)) != "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" {
		klog.Exitf("SHA256 hardware-acceleration is not working")
	}
}

func run(c *cobra.Command, args []string) {
	startedAt := time.Now()

	defer func() {
		timeTaken := time.Since(startedAt)
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

	if *flagRequireFullEpoch && *flagLimitSlots > 0 {
		klog.Exitf("Cannot use both --require-full-epoch and --limit-slots")
	}

	if finalCARFilepath == "" {
		klog.Exitf("Output CAR filepath is required")
	}
	if ok, err := fileExists(finalCARFilepath); err != nil {
		klog.Exitf("Failed to check if CAR file exists: %s", err)
	} else if ok && !*flagCheckOnly {
		klog.Exitf("CAR file already exists: %s", finalCARFilepath)
	}

	officialEpochStart, officialEpochStop := slotedges.CalcEpochLimits(epoch)
	if *flagRequireFullEpoch {
		klog.Infof(
			"Epoch %d limits: %d to %d",
			epoch,
			officialEpochStart,
			officialEpochStop,
		)
	}

	// Open blockstores
	dbPaths := *flagDBs
	handles := make([]*blockstore.WalkHandle, len(*flagDBs))
	for i := range handles {
		var err error
		handles[i] = &blockstore.WalkHandle{}
		handles[i].DB, err = blockstore.OpenReadOnly(dbPaths[i])
		if err != nil {
			klog.Exitf("Failed to open blockstore at %s: %s", dbPaths[i], err)
		}
	}

	klog.Infof("Calculating traversal schedule...")
	schedule, err := blockstore.NewSchedule(
		epoch,
		*flagRequireFullEpoch,
		handles,
		*flagShredRevision,
		*flagNextShredRevisionActivationSlot,
	)
	if err != nil {
		panic(err)
	}
	defer schedule.Close()
	klog.Infof("Traversal schedule:")

	// Print the slots range in each DB:
	err = schedule.Each(
		c.Context(),
		func(dbIndex int, db *blockstore.WalkHandle, slots []uint64) error {
			if len(slots) == 0 {
				return nil
			}
			klog.Infof("DB %d: slot %d - slot %d", dbIndex, slots[0], slots[len(slots)-1])
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	klog.Info("---")

	if *flagRequireFullEpoch {
		err := schedule.SatisfiesEpochEdges(epoch)
		if err != nil {
			klog.Exitf("Schedule does not satisfy epoch %d: %s", epoch, err)
		}
		klog.Infof("Schedule satisfies epoch %d", epoch)
	}
	klog.Info("---")

	if *flagStopAtSlot > 0 {
		if schedule.HasSlot(*flagStopAtSlot) {
			klog.Infof("You specified --stop-at-slot=%d; once we reach and process this slot (inclusive), we will exit. Removing all higher slots from schedule.", *flagStopAtSlot)
			schedule.PruneHigherThan(*flagStopAtSlot)
		} else {
			klog.Exitf("You specified --stop-at-slot=%d, but this slot is not in the schedule.", *flagStopAtSlot)
		}
	}

	slots := schedule.Slots()
	if len(slots) == 0 {
		klog.Exitf("No slots to process")
	}
	klog.Infof("Total slots to process: %d", len(slots))
	klog.Infof("First slot in schedule: %d", slots[0])
	klog.Infof("Last slot in schedule: %d", slots[len(slots)-1])
	klog.Infof(
		"Epoch %d limits: %d to %d",
		epoch,
		officialEpochStart,
		officialEpochStop,
	)

	// write slots to a file {epoch}.slots.txt
	slotsFilepath := filepath.Join(filepath.Dir(finalCARFilepath), fmt.Sprintf("%d.slots.txt", epoch))
	klog.Infof("Saving slot list to file: %s", slotsFilepath)
	slotsFile, err := os.Create(slotsFilepath)
	if err != nil {
		klog.Exitf("Failed to create slot list file: %s", err)
	}
	defer slotsFile.Close()
	for _, slot := range slots {
		_, err := slotsFile.WriteString(fmt.Sprintf("%d\n", slot))
		if err != nil {
			klog.Exitf("Failed to write slot to slots file: %s", err)
		}
	}

	klog.Info("---")

	totalSlotsToProcess := uint64(len(slots))
	if !*flagRequireFullEpoch {
		first, last := slots[0], slots[len(slots)-1]
		klog.Infof(
			"NOT REQUIRING FULL EPOCH; will process available slots only [%d:%d] (~%d slots)",
			first, last,
			// NOTE: there might be gaps in the data (as we are considering the min/max of the provided DBs),
			// so this is not a reliable estimate.
			totalSlotsToProcess,
		)
	} else {
		klog.Infof(
			"Will process slots only in the %d epoch range [%d:%d] (discarding slots outside)",
			epoch,
			officialEpochStart,
			officialEpochStop,
		)
	}

	limitSlots := *flagLimitSlots
	if limitSlots > 0 && limitSlots < totalSlotsToProcess {
		totalSlotsToProcess = limitSlots
		klog.Infof(
			"Limiting slots to %d",
			limitSlots,
		)
	}

	if *flagCheckOnly {
		klog.Infof("Check only; exiting")
		time.Sleep(1 * time.Second)
		os.Exit(0)
	}

	var txMetaBackfillerDB *BlockFillerStorage
	var btFiller *BigTableFiller

	if !*flagAllowMissingTxMeta && !*flagFillTxMetaFromBT {
		klog.Infof("Will not fill missing transaction metadata; missing transaction metadata will be treated as an error")
	}

	if *flagFillTxMetaFromBT {
		if *fillDBPath == "" {
			klog.Exitf("You specified --enable-backfill, but did not specify --backfill-cache-dir; please specify where to store the blocks fetched from BigTable")
		}
		// check that GOOGLE_APPLICATION_CREDENTIALS is set
		if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
			klog.Exitf("You specified --enable-backfill, but GOOGLE_APPLICATION_CREDENTIALS is not set; please set it to the path to your Google Cloud service account key file")
		}
		txMetaBackfillerDB, err = NewBlockFillerStorage(*fillDBPath, epoch)
		if err != nil {
			klog.Exitf("Failed to create tx meta backfill storage: %s", err)
		}
		btFiller, err = NewBigTableFiller(c.Context(), txMetaBackfillerDB)
		if err != nil {
			klog.Exitf("Failed to create BigTable filler: %s", err)
		}

		klog.Infof("Will fill missing transaction metadata from BigTable")

		{
			klog.Info("---")
			klog.Infof("Checking if we need to preload slots for tx metadata backfill")
			slotsToPreloadFromBT := make([]uint64, 0)
			{
				if *preloadBlocks != "" {
					if *preloadBlocks == "all" {
						slotsToPreloadFromBT = slots
						klog.Warningf("Requested to preload all slots; will preload from BigTable all slots")
					} else {
						slotsToPreload, err := ParseIntervals(*preloadBlocks)
						if err != nil {
							klog.Exitf("Failed to parse --preload-slots: %s", err)
						}
						DeduplicateInts(slotsToPreload)
						for _, slot := range slotsToPreload {
							if !contains(slots, uint64(slot)) {
								// skip slots that are not in the schedule
								continue
							}
							slotsToPreloadFromBT = append(slotsToPreloadFromBT, uint64(slot))
						}
					}
				}
			}
			if len(slotsToPreloadFromBT) > 0 {
				klog.Infof("Will preload %d slots for tx metadata backfill", len(slotsToPreloadFromBT))
				klog.Infof("Prefetching %d blocks with concurrency %d (will take a while) ...", len(slotsToPreloadFromBT), *flagFillConcurrency)
				btFiller.PrefetchBlocks(c.Context(), int(*flagFillConcurrency), slotsToPreloadFromBT)
				klog.Infof("Done prefetching blocks")
			} else {
				klog.Infof("No slots will be preloaded for tx metadata backfill")
			}
		}
	}

	multi, err := NewMultistage(
		finalCARFilepath,
		numWorkers,
		*flagAllowMissingTxMeta,
	)
	if err != nil {
		panic(err)
	}

	hadFirstSlot := false
	latestSlot := uint64(0)
	latestDB := int(0) // 0 is the first DB

	schedule.EnableProgressBar()

	iter := schedule.NewIterator(limitSlots)
	err = iter.Iterate(
		c.Context(),
		func(dbIdex int, h *blockstore.WalkHandle, slot uint64, shredRevision int) error {
			if *flagRequireFullEpoch && slotedges.CalcEpochForSlot(slot) != epoch {
				return nil
			}
			slotMeta, err := h.DB.GetSlotMeta(slot)
			if err != nil {
				return fmt.Errorf("failed to get slot meta for slot %d: %w", slot, err)
			}
			if slotMeta.Slot > latestSlot || slotMeta.Slot == 0 {
				if !hadFirstSlot {
					hadFirstSlot = true
					klog.Infof("Started processing DB #%d from slot %d", dbIdex, slotMeta.Slot)
				}
				if dbIdex != latestDB {
					msg := fmt.Sprintf("Switched to DB #%d; started processing new DB from slot %d", dbIdex, slotMeta.Slot)
					if latestSlot > 0 {
						msg += fmt.Sprintf(" (prev: %d)", latestSlot)
					} else {
						msg += fmt.Sprintf(" (no prev)")
					}
					klog.Info(msg)
					// TODO: warn if we skipped slots
					if latestSlot > 0 && slotMeta.Slot > latestSlot+1 {
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
				latestDB = dbIdex
			} else if slotMeta.Slot < latestSlot {
				if dbIdex == latestDB {
					panic(fmt.Errorf("slot %d is out of order (previous processed slot was: %d)", slotMeta.Slot, latestSlot))
				} else {
					// TODO: remove
					// we switched to another DB; print warning and return
					// klog.Infof("New DB; slot %d was supposedly already processed (previous processed slot was: %d); skipping", slotMeta.Slot, latestSlot)
					return nil
				}
			} else {
				// slotMeta.Slot == latestSlot
				if slotMeta.Slot != 0 && dbIdex == latestDB {
					panic(fmt.Errorf("slot %d is already processed", slotMeta.Slot))
				} else {
					// we switched to another DB; print warning and return
					// klog.Infof("New DB; slot %d is already processed; skipping", slotMeta.Slot)
					return nil
				}
			}
			var txMetaFillerFunc blockstore.AlternativeMetaGetter
			if btFiller != nil {
				var block ParsedBlock
				mu := &sync.Mutex{}
				txMetaFillerFunc = func(slot uint64, sig solana.Signature) ([]byte, error) {
					mu.Lock()
					defer mu.Unlock()
					if block == nil {
						block, err = btFiller.GetBlock(c.Context(), slot)
						if err != nil {
							return nil, fmt.Errorf("failed to get block %d from txMetaCache: %w", slot, err)
						}
					}
					metaBuf, ok := block[sig]
					if !ok {
						return nil, fmt.Errorf("transaction metadata not found for signature %s in slot %d", sig, slot)
					}
					if len(metaBuf) == 0 {
						return nil, fmt.Errorf("transaction metadata is empty for signature %s in slot %d", sig, slot)
					}
					return metaBuf, nil
				}
				defer func() {
					// destroy the block
					for sig := range block {
						delete(block, sig)
					}
					block = nil
				}()
			} else {
				txMetaFillerFunc = nil
			}
			err = multi.OnSlotFromDB(h, slotMeta, txMetaFillerFunc)
			if err != nil {
				panic(fmt.Errorf("fatal error while processing slot %d: %w", slotMeta.Slot, err))
			}
			return nil
		})
	if err != nil {
		panic(err)
	}

	klog.Infof("Finalizing DAG in the CAR file for epoch %d, at path: %s", epoch, finalCARFilepath)
	epochCID, slotRecap, gotSlots, err := multi.FinalizeDAG(epoch)
	if err != nil {
		panic(err)
	}
	{
		calculatedSchedule := removeIf(slots, func(block uint64) bool {
			return slotedges.CalcEpochForSlot(block) != epoch
		})
		processedSlots := removeIf(gotSlots, func(block uint64) bool {
			return slotedges.CalcEpochForSlot(block) != epoch
		})

		_, _, areDifferent := compare(calculatedSchedule, processedSlots)
		if areDifferent {
			klog.Exitf("The calculated schedule and the processed slots (that were written to the CAR file) are different")
		}
	}
	klog.Infof("Root of the DAG (Epoch CID): %s", epochCID)
	tookCarCreation := time.Since(startedAt)
	klog.Infof("Done. Completed CAR file generation in %s", tookCarCreation)
	type Recap struct {
		Epoch                 uint64                 `yaml:"epoch"`
		EpochCID              string                 `yaml:"epoch_cid"`
		NumTransactions       uint64                 `yaml:"num_transactions"`
		CarFileSha256         string                 `yaml:"car_file_sha256"`
		CarFileSizeBytes      uint64                 `yaml:"car_file_size_bytes"`
		CarNumObjects         uint64                 `yaml:"car_num_objects"`
		NumSlots              uint64                 `yaml:"num_slots"`
		FirstSlot             uint64                 `yaml:"first_slot"`
		LastSlot              uint64                 `yaml:"last_slot"`
		TookCarCreation       time.Duration          `yaml:"took_car_creation"`
		TookTotal             time.Duration          `yaml:"took_total"`
		NumBytesReadFromDisk  uint64                 `yaml:"num_bytes_read_from_disk"`
		NumBytesWrittenToDisk uint64                 `yaml:"num_bytes_written_to_disk"`
		VersionInfo           map[string]interface{} `yaml:"version_info"`
		Cmd                   string                 `yaml:"cmd"`
		SizesByKind           KVSlice                `yaml:"sizes_by_kind"`
		NumTotalMissingTxMeta uint64                 `yaml:"num_total_missing_tx_meta"`
		NumMissingTxMetaByDB  map[string]uint64      `yaml:"num_missing_tx_meta_by_db"`
	}

	thisCarRecap := Recap{
		Epoch:           epoch,
		EpochCID:        epochCID.String(),
		NumTransactions: multi.NumberOfTransactions(),
		CarNumObjects:   multi.NumberOfWrittenObjects(),
		NumSlots:        slotRecap.TotalSlots,
		FirstSlot:       slotRecap.FirstSlot,
		LastSlot:        slotRecap.LastSlot,
		TookCarCreation: tookCarCreation,
		SizesByKind:     slotRecap.StatsBySize,
	}
	{
		totalMissingTxMeta := multi.statsNotFoundMeta.Total()
		thisCarRecap.NumTotalMissingTxMeta = totalMissingTxMeta

		thisCarRecap.NumMissingTxMetaByDB = make(map[string]uint64)
		for dbPath, numMissing := range multi.statsNotFoundMeta.GetMap() {
			thisCarRecap.NumMissingTxMetaByDB[dbPath] = numMissing
		}
	}
	{
		versionInfo, ok := versioninfo.GetBuildSettings()
		if ok {
			thisCarRecap.VersionInfo = make(map[string]interface{})
			for _, setting := range versionInfo {
				thisCarRecap.VersionInfo[setting.Key] = setting.Value
			}
		}
	}
	{
		// save the command used to generate this CAR file
		thisCarRecap.Cmd = strings.Join(os.Args, " ")
	}

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
		var totalDBsSize uint64
		hadError := false
		for _, dbPath := range dbPaths {
			dbPath = filepath.Clean(dbPath)
			dbSize, err := getDirSize(dbPath)
			if err != nil {
				hadError = true
				klog.Errorf("Failed to get size of DB %s: %s", dbPath, err)
				continue
			}
			totalDBsSize += dbSize
			klog.Infof("- DB %s size: %s", dbPath, humanize.Bytes(uint64(dbSize)))
		}
		if hadError {
			klog.Warning("Failed to get size of one or more DBs")
		}
		klog.Infof("Total DBs size: %s", humanize.Bytes(totalDBsSize))
	}

	{
		klog.Info("---")
		timeTakenUntilAfterCARFinalization := time.Since(startedAt)
		numBytesReadFromDisk, err := iostats.GetDiskReadBytes()
		if err != nil {
			panic(err)
		}
		thisCarRecap.NumBytesReadFromDisk = numBytesReadFromDisk
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
		thisCarRecap.NumBytesWrittenToDisk = numBytesWrittenToDisk
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
		thisCarRecap.CarFileSha256 = gotHash
	} else {
		klog.Info("Skipping hashing the CAR file")
	}
	{
		carFileSize, err := fileSize(finalCARFilepath)
		if err != nil {
			klog.Warningf("Failed to get CAR file size: %s", err)
		} else {
			klog.Infof("CAR file size: %s", humanize.Bytes(carFileSize))
		}
		thisCarRecap.CarFileSizeBytes = carFileSize
	}
	thisCarRecap.TookTotal = time.Since(startedAt)

	{
		// save the recap to a file, in the format {epoch}.recap.yaml
		recapFilepath := filepath.Join(filepath.Dir(finalCARFilepath), fmt.Sprintf("%d.recap.yaml", epoch))
		klog.Infof("Saving recap to file: %s", recapFilepath)
		recapFile, err := os.Create(recapFilepath)
		if err != nil {
			klog.Errorf("Failed to create recap file: %s", err)
		} else {
			defer recapFile.Close()
			err = yaml.NewEncoder(recapFile).Encode(thisCarRecap)
			if err != nil {
				klog.Errorf("Failed to write recap file: %s", err)
			}
		}
	}

	timeTaken := time.Since(startedAt)
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

func removeIf(slice []uint64, remover func(uint64) bool) []uint64 {
	var out []uint64
	for _, item := range slice {
		if !remover(item) {
			out = append(out, item)
		}
	}
	return out
}

func uniqueBlocks(blocks []uint64) []uint64 {
	// sort, then remove duplicates:
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i] < blocks[j]
	})
	var out []uint64
	for i := range blocks {
		if i == 0 || blocks[i] != blocks[i-1] {
			out = append(out, blocks[i])
		}
	}
	return out
}

func compare(scheduleList []uint64, carList []uint64) ([]uint64, []uint64, bool) {
	scheduleList = uniqueBlocks(scheduleList)
	carList = uniqueBlocks(carList)

	hasDiff := false
	// blocks in schedule but not in car:
	var removed []uint64
	for _, block := range scheduleList {
		if !contains(carList, block) {
			removed = append(removed, block)
		}
	}
	if len(removed) > 0 {
		fmt.Printf("\n🚫 blocks in %s but not in %s:\n", green("schedule"), red("car"))
		for _, block := range removed {
			fmt.Println(block)
		}
		hasDiff = true
	}

	// blocks in car but not in schedule:
	var added []uint64
	for _, block := range carList {
		if !contains(scheduleList, block) {
			added = append(added, block)
		}
	}
	if len(added) > 0 {
		fmt.Printf("\n🚫 blocks in %s but not in %s:\n", green("car"), red("schedule"))
		for _, block := range added {
			fmt.Println(block)
		}
		hasDiff = true
	}

	if !hasDiff {
		fmt.Println("\n✅ No differences between schedule and car")
	}
	return added, removed, hasDiff
}

func contains(slots []uint64, slot uint64) bool {
	i := SearchUint64(slots, slot)
	return i < len(slots) && slots[i] == slot
}

func SearchUint64(a []uint64, x uint64) int {
	return sort.Search(len(a), func(i int) bool { return a[i] >= x })
}

func red(s string) string {
	return "\033[31m" + s + "\033[0m"
}

func green(s string) string {
	return "\033[32m" + s + "\033[0m"
}
