package createcar

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"go.firedancer.io/radiance/pkg/ledger_bigtable"
)

type BlockFillerStorage struct {
	badgerDBPath string
	db           *badger.DB
}

func NewBlockFillerStorage(badgerDBPath string, epoch uint64) (*BlockFillerStorage, error) {
	slotDB := &BlockFillerStorage{
		badgerDBPath: badgerDBPath,
	}

	opts := badger.DefaultOptions(badgerDBPath)
	opts.MetricsEnabled = false
	opts.Logger = nil
	opts.Compression = options.ZSTD
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open/create badger DB at %s: %w", badgerDBPath, err)
	}
	slotDB.db = db

	// Check or set the epoch in the database
	err = checkOrSetEpochAndVersion(db, epoch)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed DB init checks: %w", err)
	}

	return slotDB, nil
}

func epochKey() []byte {
	return []byte("__epoch__")
}

func encodeEpochValue(epoch uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, epoch)
	return b
}

func parseEpochValue(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid epoch value length: %d", len(value))
	}
	return binary.LittleEndian.Uint64(value), nil
}

const backFillDBVersion = 1

func versionKey() []byte {
	return []byte("__version__")
}

func encodeVersionValue(version int) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(version))
	return b
}

func parseVersionValue(value []byte) (int, error) {
	if len(value) != 4 {
		return 0, fmt.Errorf("invalid version value length: %d", len(value))
	}
	return int(binary.LittleEndian.Uint32(value)), nil
}

func checkOrSetEpochAndVersion(db *badger.DB, epoch uint64) error {
	var currentEpoch uint64
	var found bool

	var currentVersion int
	err := db.View(func(txn *badger.Txn) error {
		{
			item, err := txn.Get(epochKey())
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return nil // Key not found, epoch not set
				}
				return fmt.Errorf("failed to get epoch: %w", err)
			}
			found = true
			value, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy epoch value: %w", err)
			}
			currentEpoch, err = parseEpochValue(value)
			if err != nil {
				return fmt.Errorf("failed to parse epoch value: %w", err)
			}
		}
		{
			item, err := txn.Get(versionKey())
			if err != nil {
				if err == badger.ErrKeyNotFound {
					return nil // Key not found, version not set
				}
				return fmt.Errorf("failed to get version: %w", err)
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("failed to copy version value: %w", err)
			}
			currentVersion, err = parseVersionValue(value)
			if err != nil {
				return fmt.Errorf("failed to parse version value: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		// in case we don't have the epoch, the DB must be empty or not initialized
		{
			// if not empty, probably is an older version that is not compatible with bigtable
			var numDBItems int
			err = db.View(func(txn *badger.Txn) error {
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				for it.Rewind(); it.Valid(); it.Next() {
					numDBItems++
					if numDBItems > 0 {
						break // We only care if there are any items
					}
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to iterate over DB items: %w", err)
			}
			if numDBItems > 0 {
				return fmt.Errorf("the DB is not empty, but the epoch is not set. This might be an older version of the DB that is not compatible with bigtable")
			}
		}
		return db.Update(func(txn *badger.Txn) error {
			err := txn.Set(epochKey(), encodeEpochValue(epoch))
			if err != nil {
				return fmt.Errorf("failed to set epoch: %w", err)
			}
			err = txn.Set(versionKey(), encodeVersionValue(backFillDBVersion))
			if err != nil {
				return fmt.Errorf("failed to set version: %w", err)
			}
			return nil
		})
	}
	// Check if the current version is compatible
	if currentVersion != backFillDBVersion {
		return fmt.Errorf("incompatible DB version: expected %d, got %d", backFillDBVersion, currentVersion)
	}
	if currentEpoch == epoch {
		// Epoch already set to the expected value
		return nil
	}
	// Epoch mismatch, return false
	return fmt.Errorf("epoch mismatch: the DB was created with epoch %d, but the current epoch is %d", currentEpoch, epoch)
}

// Close
func (s *BlockFillerStorage) Close() error {
	return s.db.Close()
}

// Get
func (s *BlockFillerStorage) Get(slot uint64) ([]byte, ledger_bigtable.BigTableDataEncoding, error) {
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
	encoding, prefixLen, err := ledger_bigtable.GetEncodingFromBytes(value[:ledger_bigtable.MaxEncodingSize])
	if err != nil {
		return nil, "", err
	}
	return value[prefixLen:], encoding, nil
}

// Set
func (s *BlockFillerStorage) Set(slot uint64, encoding ledger_bigtable.BigTableDataEncoding, value []byte) error {
	key := slotToBytes(slot)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, append([]byte(encoding), value...))
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
