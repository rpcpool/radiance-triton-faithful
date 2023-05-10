package indexcidtooffset

import (
	"context"
	"encoding/binary"

	"github.com/ipfs/go-cid"
	store "github.com/ipld/go-storethehash/store"
	storetypes "github.com/ipld/go-storethehash/store/types"
)

type errorType string

func (e errorType) Error() string {
	return string(e)
}

// ErrNotSupported indicates and error that is not supported because this store is append only
const ErrNotSupported = errorType("Operation not supported")

// Store is a blockstore that uses a simple hash table and two files to write
type Store struct {
	store *store.Store
}

func OpenStore(ctx context.Context, indexPath string, dataPath string, options ...store.Option) (*Store, error) {
	store, err := store.OpenStore(
		ctx,
		store.CIDPrimary,
		dataPath,
		indexPath,
		false,
		options...,
	)
	if err != nil {
		return nil, err
	}
	return &Store{store}, nil
}

func (as *Store) Delete(ctx context.Context, c cid.Cid) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, err := as.store.Remove(c.Hash())
	return err
}

// Has indicates if a Slot exists in the store.
func (as *Store) Has(ctx context.Context, c cid.Cid) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return as.store.Has(c.Hash())
}

type CidToOffset struct {
	CID    cid.Cid
	Offset uint64
}

var (
	ErrInvalidValue = errorType("invalid value")
	ErrWrongSlot    = errorType("wrong slot")
)

func parseOffset(value []byte) (uint64, error) {
	if len(value) < 8 {
		return 0, ErrInvalidValue
	}
	// parse offset
	offset := binary.LittleEndian.Uint64(value[:8])
	return offset, nil
}

func offsetToBytes(offset uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, offset)
	return buf
}

func (as *Store) Get(ctx context.Context, c cid.Cid) (uint64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	value, found, err := as.store.Get(c.Hash())
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrNotFound{Cid: c}
	}
	parsed, err := parseOffset(value)
	if err != nil {
		return 0, err
	}
	return parsed, nil
}

func (as *Store) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	// unoptimized implementation for now
	size, found, err := as.store.GetSize(c.Hash())
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrNotFound{Cid: c}
	}
	return int(size), nil
}

func (as *Store) Put(ctx context.Context, c cid.Cid, offset uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	err := as.store.Put(c.Hash(), offsetToBytes(offset))
	// TODO:
	if err == storetypes.ErrKeyExists {
		return nil
	}
	return err
}

type PutMany map[cid.Cid]uint64

func (as *Store) PutMany(ctx context.Context, accs PutMany) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for c, offset := range accs {
		err := as.store.Put(c.Hash(), offsetToBytes(offset))
		if err != nil && err != storetypes.ErrKeyExists {
			return err
		}
	}
	return nil
}

func (as *Store) AllKeysChan(ctx context.Context) (<-chan uint64, error) {
	// return nil, ErrNotSupported
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	iter := as.store.NewIterator()

	ch := make(chan uint64)
	go func() {
		defer close(ch)
		for slotHash, val, err := iter.Next(); err == nil; slotHash, _, err = iter.Next() {
			_ = slotHash
			// parse val[:8] as uint64
			offset := (binary.LittleEndian.Uint64(val[:8]))
			select {
			case ch <- offset:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (as *Store) Start() {
	as.store.Start()
}

func (as *Store) Close() {
	as.store.Close()
}
