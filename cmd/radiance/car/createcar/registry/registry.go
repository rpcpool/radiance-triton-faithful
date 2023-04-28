package registry

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ipfs/go-cid"
)

// Registry tracks for each slot the processing status and the (eventual) CID.
type Registry struct {
	file          storageInterface
	cidByteLength int
	knownOffsets  map[uint64]int64
	mu            sync.RWMutex
	fileLen       int
}

type storageInterface interface {
	io.Reader
	io.WriterAt
	io.ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}

func New(filepath string, cidByteLength int) (*Registry, error) {
	// open file in read/write mode, or create it if it doesn't exist
	if ok, err := fileExists(filepath); err != nil {
		return nil, err
	} else if ok {
		return openFromExistingFile(filepath, cidByteLength)
	} else {
		return createNewRegistry(filepath, cidByteLength)
	}
}

func openFromExistingFile(filepath string, cidByteLength int) (*Registry, error) {
	// open file in read/write mode
	f, err := os.OpenFile(filepath, os.O_RDWR, 0o666)
	if err != nil {
		return nil, err
	}
	return openExistingRegistryFromInterface(f, cidByteLength)
}

func createNewRegistry(filepath string, cidByteLength int) (*Registry, error) {
	f, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	return newRegistryFromInterface(f, cidByteLength)
}

func calcSlotSize(cidByteLength int) uint64 {
	return uint64(8 + 1 + 4 + cidByteLength)
}

func openExistingRegistryFromInterface(file storageInterface, cidByteLength int) (*Registry, error) {
	// read the slots
	slotSize := calcSlotSize(cidByteLength)
	slots, err := readSlotsFromReader(file, slotSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read slots: %w", err)
	}

	// build the known offsets map
	knownOffsets := make(map[uint64]int64)
	for _, slot := range slots {
		knownOffsets[slot.Slot] = slot.offset
	}

	fileLen := int(0)
	if stat, err := file.Stat(); err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	} else {
		fileLen = int(stat.Size())
	}

	return &Registry{
		file:          file,
		cidByteLength: cidByteLength,
		knownOffsets:  knownOffsets,
		fileLen:       fileLen,
	}, nil
}

func readSlotsFromReader(r storageInterface, slotSize uint64) ([]Slot, error) {
	slots, err := readSlots(r, slotSize)
	if err != nil {
		return nil, fmt.Errorf("failed to read slots: %w", err)
	}
	return slots, nil
}

func readSlots(r storageInterface, slotSize uint64) ([]Slot, error) {
	slots := make([]Slot, 0)
	for i := uint64(0); ; i++ {
		slot, err := readSlot(r, i, slotSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read slot %d: %w", i, err)
		}
		slots = append(slots, slot)
	}
	return slots, nil
}

func readSlot(r storageInterface, slotIndex uint64, slotSize uint64) (Slot, error) {
	offset := (slotIndex * slotSize)
	slotBytes := make([]byte, slotSize)
	if _, err := r.ReadAt(slotBytes, int64(offset)); err != nil {
		return Slot{}, fmt.Errorf("failed to read slot: %w", err)
	}

	slot := Slot{
		Slot:   binary.LittleEndian.Uint64(slotBytes[0:8]),
		Status: SlotStatus(slotBytes[8]),
		offset: int64(offset),
	}

	if slot.Status == SlotStatusIncluded {
		cidLength := binary.LittleEndian.Uint32(slotBytes[9:13])
		cidBytes := make([]byte, cidLength)
		if _, err := r.ReadAt(cidBytes, int64(offset+13)); err != nil {
			return slot, fmt.Errorf("failed to read cid: %w", err)
		}
		slot.CID = cidBytes
	}

	return slot, nil
}

type Slot struct {
	Slot   uint64
	Status SlotStatus
	CID    []byte
	offset int64
}

func fileExists(filepath string) (bool, error) {
	if filepath == "" {
		return false, fmt.Errorf("path is empty")
	}

	_, err := os.Stat(filepath)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("failed to check if file exists: %w", err)
	}
	return true, nil
}

func newRegistryFromInterface(file storageInterface, cidByteLength int) (*Registry, error) {
	return &Registry{
		file:          file,
		cidByteLength: cidByteLength,
		knownOffsets:  make(map[uint64]int64),
	}, nil
}

func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.file.Close()
}

func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.knownOffsets)
}

// SetCID updates the status of the slot as "included" and sets the CID.
func (r *Registry) SetCID(slot uint64, cid []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(cid) != r.cidByteLength {
		return fmt.Errorf("cid length is %d, expected %d", len(cid), r.cidByteLength)
	}

	offset, exists := r.knownOffsets[slot]
	if exists {
		return r.updateEntryAtOffset(offset, SlotStatusIncluded, cid)
	}

	// append the entry
	offset = int64(r.fileLen)
	if bytesWritten, err := r.writeEntryAtOffset(offset, slot, SlotStatusIncluded, cid); err != nil {
		return err
	} else {
		r.fileLen += (bytesWritten)
	}
	r.knownOffsets[slot] = offset

	return nil
}

func (r *Registry) writeEntryAtOffset(offset int64, slot uint64, status SlotStatus, cid []byte) (int, error) {
	// the layout is:
	//  - 8 bytes for the slot uint64
	//  - 1 byte for the status
	//  - 4 bytes for the length of the CID
	//  - N bytes for the CID

	totalWritten := 0
	// write the slot
	if wl, err := r.file.WriteAt(uint64ToBytes(slot), (offset)); err != nil {
		return totalWritten, fmt.Errorf("failed to write slot: %w", err)
	} else {
		totalWritten += wl
	}
	offset += 8

	// write the status
	if wl, err := r.file.WriteAt([]byte{byte(status)}, (offset)); err != nil {
		return totalWritten, fmt.Errorf("failed to write status: %w", err)
	} else {
		totalWritten += wl
	}
	offset++

	// write the length of the CID as a 4 byte uint32
	cidLength := uint32(len(cid))
	if wl, err := r.file.WriteAt(uint32ToBytes(cidLength), (offset)); err != nil {
		return totalWritten, fmt.Errorf("failed to write CID length: %w", err)
	} else {
		totalWritten += wl
	}
	offset += 4

	// write the CID
	if wl, err := r.file.WriteAt(cid, (offset)); err != nil {
		return totalWritten, fmt.Errorf("failed to write CID: %w", err)
	} else {
		totalWritten += wl
	}

	return totalWritten, nil
}

func writeUint64ToWriterAt(w storageInterface, offset int64, value uint64) error {
	if _, err := w.WriteAt(uint64ToBytes(value), (offset)); err != nil {
		return fmt.Errorf("failed to write CID length: %w", err)
	}
	return nil
}

func (r *Registry) updateEntryAtOffset(offset int64, status SlotStatus, cid []byte) error {
	// the layout is:
	//  - 8 bytes for the slot uint64
	//  - 1 byte for the status
	//  - 4 bytes for the length of the CID
	//  - N bytes for the CID

	// skip the slot
	offset += 8

	// write the status
	if _, err := r.file.WriteAt([]byte{byte(status)}, (offset)); err != nil {
		return fmt.Errorf("failed to write status: %w", err)
	}
	offset++

	// write the length of the CID as a 4 byte uint32
	cidLength := uint32(len(cid))
	if _, err := r.file.WriteAt(uint32ToBytes(cidLength), (offset)); err != nil {
		return fmt.Errorf("failed to write CID length: %w", err)
	}
	offset += 4

	// write the CID
	if _, err := r.file.WriteAt(cid, (offset)); err != nil {
		return fmt.Errorf("failed to write CID: %w", err)
	}

	return nil
}

func uint32ToBytes(i uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, i)
	return buf
}

func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func readUint64FromReaderAt(r io.ReaderAt, offset int64) (uint64, error) {
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, offset); err != nil {
		return 0, fmt.Errorf("failed to read uint64: %w", err)
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func readUint32FromReaderAt(r io.ReaderAt, offset int64) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := r.ReadAt(buf, offset); err != nil {
		return 0, fmt.Errorf("failed to read uint32: %w", err)
	}
	return binary.LittleEndian.Uint32(buf), nil
}

// GetCID returns the CID for the given slot.
func (r *Registry) GetCID(slot uint64) (*cid.Cid, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	offset, exists := r.knownOffsets[slot]
	if !exists {
		return nil, fmt.Errorf("slot %d not found", slot)
	}

	// the layout is:
	//  - 8 bytes for the slot uint64
	//  - 1 byte for the status
	//  - 4 bytes for the length of the CID
	//  - N bytes for the CID

	// read the slot and check it matches
	if gotSlot, err := readUint64FromReaderAt(r.file, offset); err != nil {
		return nil, fmt.Errorf("failed to read slot: %w", err)
	} else if gotSlot != slot {
		return nil, fmt.Errorf("slot mismatch: expected %d, got %d", slot, gotSlot)
	}
	offset += 8

	// skip the status
	offset++

	// read the length of the CID as a 4 byte uint32
	cidLength, err := readUint32FromReaderAt(r.file, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read CID length: %w", err)
	} else if cidLength != uint32(r.cidByteLength) {
		return nil, fmt.Errorf("CID length mismatch: expected %d, got %d", r.cidByteLength, cidLength)
	}
	offset += 4

	// read the CID
	cidBytes := make([]byte, cidLength)
	if _, err := r.file.ReadAt(cidBytes, (offset)); err != nil {
		return nil, fmt.Errorf("failed to read CID: %w", err)
	}

	l, c, err := cid.CidFromBytes(cidBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CID: %w", err)
	}
	if l != r.cidByteLength {
		return nil, fmt.Errorf("CID length mismatch: expected %d, got %d", r.cidByteLength, l)
	}
	return &c, nil
}

func (r *Registry) GetAll() ([]Slot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	slotSize := calcSlotSize(r.cidByteLength)
	return readSlotsFromReader(r.file, slotSize)
}

type SlotStatus byte

const (
	SlotStatusNull     SlotStatus = iota // 0 = null
	SlotStatusIncluded                   // 1 = the slot and its subgraph are included in the car
)

func isValidSlotStatus(status SlotStatus) bool {
	return status.IsAny(
		SlotStatusNull,
		SlotStatusIncluded,
	)
}

func (s SlotStatus) Is(status SlotStatus) bool {
	return s == status
}

func (s SlotStatus) IsAny(statuses ...SlotStatus) bool {
	for _, status := range statuses {
		if s == status {
			return true
		}
	}
	return false
}
