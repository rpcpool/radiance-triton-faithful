package slotstatustracker

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
)

type Tracker struct {
	// maps slot number to its status (0 = null, 1 = packaging, 2 = packaged, 3 = included in DAG)
	slots map[uint64]statusAndOffset
	mu    sync.RWMutex
	file  storageInterface
}

type statusAndOffset struct {
	status SlotStatus
	offset int64
}

type storageInterface interface {
	io.Reader
	io.WriterAt
	io.ReaderAt
	io.Closer
}

func New(trackerFilepath string) (*Tracker, error) {
	if trackerFilepath == "" {
		return nil, errors.New("dir path is empty")
	}
	if ok, err := fileExists(trackerFilepath); err != nil {
		return nil, err
	} else if ok {
		return openExistingSlotTracker(trackerFilepath)
	} else {
		return createNewSlotTracker(trackerFilepath)
	}
}

func openExistingSlotTracker(pathToFile string) (*Tracker, error) {
	if pathToFile == "" {
		return nil, errors.New("path is empty")
	}
	if exists, err := fileExists(pathToFile); err != nil {
		return nil, fmt.Errorf("failed to check if file exists: %w", err)
	} else if !exists {
		return nil, fmt.Errorf("file %q does not exist", pathToFile)
	}

	// open file in read/write mode
	file, err := os.OpenFile(pathToFile, os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}

	// read the slots
	slots, _, err := readSlotsFromReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read slots: %w", err)
	}

	return &Tracker{
		slots: slots,
		file:  file,
	}, nil
}

func createNewSlotTracker(pathToFile string) (*Tracker, error) {
	if pathToFile == "" {
		return nil, errors.New("path is empty")
	}

	path := pathToFile
	if exists, err := fileExists(path); err != nil {
		return nil, fmt.Errorf("failed to check if file exists: %w", err)
	} else if exists {
		return nil, fmt.Errorf("file %q already exists", path)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	// write the current count of slots, which is 0
	if err := writeUint64ToWriterAt(file, 0, 0); err != nil {
		return nil, fmt.Errorf("failed to write number of slots: %w", err)
	}

	slotToOffset := make(map[uint64]statusAndOffset)

	return &Tracker{
		slots: slotToOffset,
		file:  file,
	}, nil
}

func readSlotsFromReader(r io.Reader) (map[uint64]statusAndOffset, hash.Hash, error) {
	// read the the number of slots (uint64)
	n, err := readUint64FromReader(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read number of slots: %w", err)
	}
	rollingHash := sha256.New()
	// read each slot (uint64) and its status (uint8)
	slots := make(map[uint64]statusAndOffset)
	for i := uint64(0); i < n; i++ {
		slot, err := readUint64FromReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read slot %d: %w", i, err)
		}
		status, err := readUint8FromReader(r)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read slot %d status: %w", i, err)
		}
		if !isValidSlotStatus(SlotStatus(status)) {
			return nil, nil, fmt.Errorf("invalid slot status %d for slot %d", status, i)
		}
		slots[slot] = statusAndOffset{
			status: SlotStatus(status),
			offset: int64(8 + 9*i),
		}
		// write slot to rolling hash
		if _, err := rollingHash.Write(uint64ToBytes(slot)); err != nil {
			return nil, nil, fmt.Errorf("failed to write slot %d to rolling hash: %w", i, err)
		}
	}
	return slots, rollingHash, nil
}

func (st *Tracker) writeStatusAtOffset(offset int64, status SlotStatus) error {
	n, err := st.file.WriteAt([]byte{byte(status)}, offset)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("expected to write 1 byte, wrote %d", n)
	}
	return nil
}

var ErrSlotNotFound = errors.New("slot not found")

func (st *Tracker) getSlotHandleNoMutex(slot uint64) (statusAndOffset, error) {
	// check if the slot exists
	sl, ok := st.slots[slot]
	if !ok {
		// read current number of slots from file
		n, err := readUint64FromReaderAt(st.file, 0)
		if err != nil {
			return statusAndOffset{}, fmt.Errorf("failed to read number of slots: %w", err)
		}
		// compare the number of slots with the number of slots in memory
		if uint64(len(st.slots)) != n {
			return statusAndOffset{}, fmt.Errorf("number of slots in memory (%d) does not match number of slots in file (%d)", len(st.slots), n)
		}
		// declare a new slot
		sl = statusAndOffset{
			status: SlotStatusNull,
			offset: int64(8 + 9*n),
		}

		// write the new slot to the file
		if err := writeUint64ToWriterAt(st.file, sl.offset, slot); err != nil {
			return statusAndOffset{}, fmt.Errorf("failed to write slot to file: %w", err)
		}
		if err := st.writeStatusAtOffset(sl.offset+8, SlotStatusNull); err != nil {
			return statusAndOffset{}, fmt.Errorf("failed to write slot status to file: %w", err)
		}
		// update the number of slots in the file
		if err := writeUint64ToWriterAt(st.file, 0, n+1); err != nil {
			return statusAndOffset{}, fmt.Errorf("failed to write number of slots to file: %w", err)
		}
		// update the slot in memory
		st.slots[slot] = sl
	}
	return sl, nil
}

// SetStatus sets the status of the provided slot to the given status.
func (st *Tracker) SetStatus(slot uint64, status SlotStatus) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	// check if the slot exists
	sl, err := st.getSlotHandleNoMutex(slot)
	if err != nil {
		return err
	}
	// write to the file
	if err := st.writeStatusAtOffset(sl.offset, status); err != nil {
		return err
	}
	// update the in-memory status
	sl.status = status
	st.slots[slot] = sl
	return nil
}

// SetStatusIf sets the status of the provided slot to the given new status if
// the current status is the provided ifStatus.
func (st *Tracker) SetStatusIf(slot uint64, newStatus SlotStatus, ifStatus SlotStatus) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	// check if the slot exists
	sl, err := st.getSlotHandleNoMutex(slot)
	if err != nil {
		return false, err
	}
	// check if the current status is the expected status
	if sl.status != ifStatus {
		return false, nil
	}
	// write to the file
	if err := st.writeStatusAtOffset(sl.offset, newStatus); err != nil {
		return false, err
	}
	// update the in-memory status
	sl.status = newStatus
	st.slots[slot] = sl
	return true, nil
}

// SetStatusIfAtLeast sets the status of the provided slot to the given new status
// if the current status is at least the provided ifStatus.
func (st *Tracker) SetStatusIfAtLeast(slot uint64, newStatus SlotStatus, ifStatus SlotStatus) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	// check if the slot exists
	sl, err := st.getSlotHandleNoMutex(slot)
	if err != nil {
		return false, err
	}
	// check if the status matches
	if sl.status < ifStatus {
		return false, nil
	}
	// write to the file
	if err := st.writeStatusAtOffset(sl.offset, newStatus); err != nil {
		return false, err
	}
	// update the in-memory status
	sl.status = newStatus
	st.slots[slot] = sl
	return true, nil
}

// GetStatus returns the status of the given slot.
func (st *Tracker) GetStatus(slot uint64) (SlotStatus, bool) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	// check if the slot exists
	sl, ok := st.slots[slot]
	if !ok {
		return SlotStatusNull, false
	}
	return sl.status, true
}

// GetStatuses returns a copy of the statuses of all slots.
func (st *Tracker) GetStatuses() map[uint64]SlotStatus {
	st.mu.RLock()
	defer st.mu.RUnlock()
	cp := make(map[uint64]SlotStatus, len(st.slots))
	for k, v := range st.slots {
		cp[k] = v.status
	}
	return cp
}

// GetWhereStatus returns a list of slots with the given status.
func (st *Tracker) GetWhereStatus(status SlotStatus) []uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var slots []uint64
	for slot, s := range st.slots {
		if s.status == status {
			slots = append(slots, slot)
		}
	}
	sortSlots(slots)
	return slots
}

// GetWhereStatusAny returns a list of slots with any of the given statuses.
func (st *Tracker) GetWhereStatusAny(statuses ...SlotStatus) []uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var slots []uint64
	for slot, s := range st.slots {
		if s.status.IsAny(statuses...) {
			slots = append(slots, slot)
		}
	}
	sortSlots(slots)
	return slots
}

// ListSlots returns a list of all slots in the tracker.
func (st *Tracker) ListSlots() []uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var slots []uint64
	for slot := range st.slots {
		slots = append(slots, slot)
	}
	sortSlots(slots)
	return slots
}

func (st *Tracker) Close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.file.Close()
}
