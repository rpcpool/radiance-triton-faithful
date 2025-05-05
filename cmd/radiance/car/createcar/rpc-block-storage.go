package createcar

import (
	"encoding/binary"
	"log"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

type BlockFillerStorage struct {
	badgerDBPath string
	db           *badger.DB
}

func NewBlockFillerStorage(badgerDBPath string) (*BlockFillerStorage, error) {
	slotDB := &BlockFillerStorage{
		badgerDBPath: badgerDBPath,
	}

	opts := badger.DefaultOptions(badgerDBPath)
	opts.MetricsEnabled = false
	opts.Logger = nil
	opts.Compression = options.ZSTD
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	slotDB.db = db

	return slotDB, nil
}

// Close
func (s *BlockFillerStorage) Close() error {
	return s.db.Close()
}

// Get
func (s *BlockFillerStorage) Get(slot uint64) ([]byte, error) {
	var value []byte
	key := slotToBytes(slot)
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Set
func (s *BlockFillerStorage) Set(slot uint64, value []byte) error {
	key := slotToBytes(slot)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete
func (s *BlockFillerStorage) Delete(slot uint64) error {
	key := slotToBytes(slot)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// Has
func (s *BlockFillerStorage) Has(slot uint64) (bool, error) {
	key := slotToBytes(slot)
	var has bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			has = true
		} else if err == badger.ErrKeyNotFound {
			has = false
		} else {
			return err
		}
		return nil
	})
	return has, err
}

func slotToBytes(slot uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, slot)
	return b
}
