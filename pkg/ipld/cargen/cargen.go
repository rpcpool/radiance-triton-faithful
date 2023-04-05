// Package cargen transforms blockstores into CAR files.
package cargen

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/gagliardetto/solana-go"
	"go.firedancer.io/radiance/pkg/blockstore"
	"go.firedancer.io/radiance/pkg/ipld/car"
	"go.firedancer.io/radiance/pkg/ipld/ipldgen"
	"go.firedancer.io/radiance/pkg/shred"
	"k8s.io/klog/v2"
)

// MaxCARSize is the maximum size of a CARv1 file.
//
// Dictated by Filecoin's preferred sector size (currently 32 GiB).
// cargen will attempt to pack CARs as large as possible but never exceed.
//
// Filecoin miners may append CARv2 indexes, which would exceed the total CAR size.
const MaxCARSize = 1 << 35

// Worker extracts Solana blocks from blockstore and produces CAR files.
//
// # Data Source
//
// Solana validator RocksDB archives serve as input.
// Any gaps in history will be detected and thrown as an error.
//
// The worker uses a blockstore.BlockWalk to seamlessly iterate over multiple RocksDBs if needed.
// This is useful if an epoch is split across multiple archives.
//
// # Epochs
//
// Each worker processes a single epoch of Solana history (432000 slots) in rooted order.
//
// Transforming a single epoch, which takes about a day on mainnet, should take a few hours to transform into CAR.
//
// Because of epoch alignment, it is safe to run multiple workers in parallel on distinct epochs.
// Therefore, the CAR generation process can be trivially parallelized by launching multiple instances.
//
// The CAR output will be byte-by-byte deterministic with regard to Solana's authenticated ledger content.
// In other words, regardless of which node operator runs this tool, they should always get the same CAR file.
//
// # Blocks
//
// Each block will be parsed and turned into an IPLD graph.
//
// The procedure respects Worker.CARSize and splits data across multiple CARs if needed.
// This allows us to assign a slot range to each CAR for the reader's convenience, at negligible alignment cost.
//
// # Output
//
// Each run produces one or more car files in the target directory,
// named `ledger-e{epoch}-s{slot}.car`, where slot is the first slot number in the epoch.
//
// The interlinked IPLD blocks are internally encoded with DAG-CBOR.
// Except for the ipldgen.SolanaTx "leaf" nodes, which are encoded using bincode (native).
type Worker struct {
	dir   string
	walk  blockstore.BlockWalkI
	epoch uint64
	stop  uint64 // exclusive

	handle carHandle

	CARSize uint

	statsBlockCount uint64
}

func (w *Worker) statBlockPlusOne() {
	w.statsBlockCount++
	if w.statsBlockCount%100 == 0 {
		fmt.Print(".")
	}
}

// uint64RangesHavePartialOverlapIncludingEdges returns true if the two ranges have any overlap.
func uint64RangesHavePartialOverlapIncludingEdges(r1 [2]uint64, r2 [2]uint64) bool {
	if r1[0] < r2[0] {
		return r1[1] >= r2[0]
	} else {
		return r2[1] >= r1[0]
	}
}

// NewWorker creates a new worker to transform an epoch from blockstore.BlockWalk into CAR files in the given dir.
//
// Creates the directory if it doesn't exist yet.
func NewWorker(
	outDir string,
	epoch uint64,
	walk blockstore.BlockWalkI,
	requireFullEpoch bool,
) (*Worker, error) {
	if err := os.Mkdir(outDir, 0o777); err != nil && !errors.Is(err, fs.ErrExist) {
		return nil, err
	}

	// Seek to epoch start and make sure we have all data
	const epochLen = 432000
	officialEpochStart := epoch * epochLen
	officialEpochStop := officialEpochStart + epochLen
	if requireFullEpoch && !walk.Seek(officialEpochStart) {
		return nil, fmt.Errorf("slot %d not available in any DB", officialEpochStart)
	}

	slotsAvailable := walk.SlotsAvailable()
	if requireFullEpoch && slotsAvailable < epochLen {
		return nil, fmt.Errorf("need slots [%d:%d] (epoch %d) but only have up to %d",
			officialEpochStart, officialEpochStop, epoch, officialEpochStart+slotsAvailable)
	}

	haveStart, haveStop := walk.SlotEdges()

	if !requireFullEpoch {
		klog.Infof(
			"Not requiring full epoch; will process available slots [%d:%d] (~%d slots)",
			haveStart, haveStop,
			// NOTE: there might be gaps in the data (as we are considering the min/max of the provided DBs),
			// so this is not a reliable estimate.
			haveStop-haveStart,
		)
	} else {
		klog.Infof(
			"Will process slots only in the %d epoch range [%d:%d] (discarding slots outside)",
			epoch, officialEpochStart, officialEpochStop,
		)
	}

	if !uint64RangesHavePartialOverlapIncludingEdges(
		[2]uint64{officialEpochStart, officialEpochStop},
		[2]uint64{haveStart, haveStop},
	) {
		return nil, fmt.Errorf("no overlap between requested epoch [%d:%d] and available slots [%d:%d]",
			officialEpochStart, officialEpochStop,
			haveStart, haveStop)
	}

	w := &Worker{
		dir:     outDir,
		walk:    walk,
		epoch:   epoch,
		stop:    officialEpochStop,
		CARSize: MaxCARSize,
	}
	return w, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		w.statBlockPlusOne()
		next, err := w.step()
		if err != nil {
			return err
		}
		if !next {
			break
		}
	}
	if w.handle.ok() {
		if err := w.handle.close(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// step iterates one block forward.
func (w *Worker) step() (next bool, err error) {
	meta, ok := w.walk.Next()
	if !ok {
		return false, nil
	}
	if meta.Slot > w.stop {
		return false, nil
	}
	entries, err := w.walk.Entries(meta)
	if err != nil {
		return false, fmt.Errorf("failed to get entry at slot %d: %w", meta.Slot, err)
	}
	if err := w.ensureHandle(meta.Slot); err != nil {
		return false, err
	}
	if err := w.writeSlot(meta, entries); err != nil {
		return false, err
	}
	if err := w.splitHandle(meta.Slot); err != nil {
		return false, err
	}
	return true, nil
}

// ensureHandle makes sure we have a CAR handle that we can write to.
func (w *Worker) ensureHandle(slot uint64) error {
	if w.handle.ok() {
		w.handle.lastOffset = w.handle.writer.Written()
		return nil
	}
	return w.handle.open(w.dir, w.epoch, slot)
}

// splitHandle creates a new CAR file if the current one is oversized.
//
// Internally moves blocks that exceed max CAR size from old to new file.
func (w *Worker) splitHandle(slot uint64) (err error) {
	size := w.handle.writer.Written()
	if size <= int64(w.CARSize) {
		return nil
	}
	// CAR is too large and needs to be split.
	klog.Infof("CAR file %s too large, splitting...", w.handle.file.Name())
	// Create new target CAR.
	var newCAR carHandle
	if err = newCAR.open(w.dir, w.epoch, slot); err != nil {
		return err
	}
	// Seek old CAR back to before block.
	w.handle.writer = nil
	if err = w.handle.cache.Flush(); err != nil {
		return fmt.Errorf("failed to flush CAR cache: %w", err)
	}
	klog.Infof("Seeking to offset %d and copying rest", w.handle.lastOffset)
	if _, err = w.handle.file.Seek(w.handle.lastOffset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to rewind CAR: %w", err)
	}
	// Move block from old to new.
	if _, err = io.Copy(newCAR.writer, w.handle.file); err != nil {
		return fmt.Errorf("failed to move block between CARs: %w", err)
	}
	// Truncate old handle to make it fit max size.
	if err = w.handle.file.Truncate(w.handle.lastOffset); err != nil {
		return fmt.Errorf("failed to truncate old CAR (%s) to %d bytes: %w",
			w.handle.file.Name(), w.handle.lastOffset, err)
	}
	// Swap handles.
	err = w.handle.close()
	w.handle = newCAR
	if written := w.handle.writer.Written(); written > int64(w.CARSize) {
		klog.Errorf("Slot %d exceeds size of a single CAR (%d > %d)", slot, written, w.CARSize)
	}
	return err
}

// writeSlot writes a filled Solana slot to the CAR.
// Creates multiple IPLD blocks internally.
func (w *Worker) writeSlot(meta *blockstore.SlotMeta, entries [][]shred.Entry) error {
	slot := meta.Slot
	asm := ipldgen.NewBlockAssembler(w.handle.writer, slot)

	transactionMetaKeys, err := transactionMetaKeysFromEntries(slot, entries)
	if err != nil {
		return err
	}

	txMetas, err := w.walk.TransactionMetas(transactionMetaKeys...)
	if err != nil {
		return fmt.Errorf("failed to get transaction metas for slot %d: %w", slot, err)
	}

	entryNum := 0
	txNum := 0
	klog.V(3).Infof("Slot %d", slot)
	for i, batch := range entries {
		klog.V(6).Infof("Slot %d batch %d", slot, i)

		for j, entry := range batch {
			pos := ipldgen.EntryPos{
				Slot:       slot,
				EntryIndex: entryNum,
				Batch:      i,
				BatchIndex: j,
				LastShred:  -1,
			}
			if j == len(batch)-1 {
				// We map "last shred of batch" to each "last entry of batch"
				// so we can reconstruct the shred/entry-batch assignments.
				if i >= len(meta.EntryEndIndexes) {
					return fmt.Errorf("out-of-bounds batch index %d (have %d batches in slot %d)",
						i, len(meta.EntryEndIndexes), slot)
				}
				pos.LastShred = int(meta.EntryEndIndexes[i])
			}

			// TODO: is this correct?
			transactionMetas := txMetas[txNum : txNum+len(entry.Txns)]

			if err := asm.WriteEntry(entry, pos, transactionMetas); err != nil {
				return fmt.Errorf("failed to write slot %d shred %d (batch %d index %d): %s",
					slot, entryNum, i, j, err)
			}
			txNum += len(entry.Txns)
			entryNum++
		}
	}

	// TODO roll up into ledger entries
	if _, err := asm.Finish(); err != nil {
		klog.Exitf("Failed to write block: %s", err)
	}

	return nil
}

func transactionMetaKeysFromEntries(slot uint64, entries [][]shred.Entry) ([][]byte, error) {
	keys := make([][]byte, 0)
	for _, batch := range entries {
		for _, entry := range batch {
			for _, tx := range entry.Txns {
				firstSig := tx.Signatures[0]
				keys = append(keys, FormatTxMetadataKey(slot, firstSig))
			}
		}
	}
	return keys, nil
}

func FormatTxMetadataKey(slot uint64, sig solana.Signature) []byte {
	key := make([]byte, 80)
	// the first 8 bytes are empty; fill them with zeroes
	// copy(key[:8], []byte{0, 0, 0, 0, 0, 0, 0, 0})
	// then comes the signature
	copy(key[8:], sig[:])
	// then comes the slot
	binary.BigEndian.PutUint64(key[72:], slot)
	return key
}

type carHandle struct {
	file       *os.File
	cache      *bufio.Writer
	writer     *car.Writer
	lastOffset int64
}

const writeBufSize = 16384

func (c *carHandle) open(dir string, epoch uint64, slot uint64) error {
	if c.ok() {
		return fmt.Errorf("handle not closed")
	}
	p := filepath.Join(dir, fmt.Sprintf("ledger-e%d-s%d.car", epoch, slot))
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create CAR: %w", err)
	}
	cache := bufio.NewWriterSize(f, writeBufSize)
	writer, err := car.NewWriter(cache)
	if err != nil {
		return fmt.Errorf("failed to start CAR at %s: %w", p, err)
	}
	*c = carHandle{
		file:       f,
		cache:      cache,
		writer:     writer,
		lastOffset: 0,
	}
	klog.Infof("Created new CAR file %s", f.Name())
	return nil
}

func (c *carHandle) ok() bool {
	return c.writer != nil
}

func (c *carHandle) close() (err error) {
	if err = c.cache.Flush(); err != nil {
		return err
	}
	err = c.file.Close()
	*c = carHandle{}
	return
}
