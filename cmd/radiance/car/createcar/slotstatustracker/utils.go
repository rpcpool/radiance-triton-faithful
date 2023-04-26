package slotstatustracker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("path %s is a directory", path)
	}
	return true, nil
}

func uint64ToBytes(i uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, i)
	return b
}

func writeUint64ToWriter(w io.Writer, n uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, n)
	_, err := w.Write(buf)
	return err
}

func writeUint8ToWriter(w io.Writer, n uint8) error {
	_, err := w.Write([]byte{byte(n)})
	return err
}

func readUint64FromReader(r io.Reader) (uint64, error) {
	var n uint64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func readUint8FromReader(r io.Reader) (uint8, error) {
	var n uint8
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func sortSlots(slots []uint64) {
	sort.Slice(slots, func(i, j int) bool {
		return slots[i] < slots[j]
	})
}

func writeUint64ToWriterAt(w io.WriterAt, offset int64, n uint64) error {
	if w == nil {
		return errors.New("writer is nil")
	}
	if _, err := w.WriteAt(uint64ToBytes(n), offset); err != nil {
		return fmt.Errorf("failed to write %d at offset %d: %w", n, offset, err)
	}
	return nil
}

func readUint64FromReaderAt(r io.ReaderAt, offset int64) (uint64, error) {
	if r == nil {
		return 0, errors.New("reader is nil")
	}
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, offset); err != nil {
		return 0, fmt.Errorf("failed to read from offset %d: %w", offset, err)
	}
	return binary.LittleEndian.Uint64(buf), nil
}
