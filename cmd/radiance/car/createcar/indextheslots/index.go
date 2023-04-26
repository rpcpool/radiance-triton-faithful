package indextheslots

import (
	"context"
	"encoding/binary"

	"github.com/ipfs/go-cid"
	store "github.com/ipld/go-storethehash/store"
	storetypes "github.com/ipld/go-storethehash/store/types"
	"github.com/multiformats/go-multihash"
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

// OpenStore opens a HashedBlockstore with the default index size
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

type Slot uint64

func (as *Store) DeleteSlot(ctx context.Context, slot Slot) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	pkh, err := multihashKeyFromSlotNumber(slot)
	if err != nil {
		return err
	}
	_, err = as.store.Remove(pkh.Bytes())
	return err
}

// Has indicates if a Slot exists in the store.
func (as *Store) Has(ctx context.Context, slot Slot) (bool, error) {
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	pkh, err := multihashKeyFromSlotNumber(slot)
	if err != nil {
		return false, err
	}
	return as.store.Has(pkh.Bytes())
}

type SlotToCID struct {
	Slot Slot
	CID  cid.Cid
}

var (
	ErrInvalidValue = errorType("invalid value")
	ErrWrongSlot    = errorType("wrong slot")
)

func parseSlotToCID(value []byte) (*SlotToCID, error) {
	if len(value) < 8 {
		return nil, ErrInvalidValue
	}
	slot := binary.LittleEndian.Uint64(value[:8])
	cid, err := cid.Cast(value[8:])
	if err != nil {
		return nil, err
	}
	return &SlotToCID{
		Slot: Slot(slot),
		CID:  cid,
	}, nil
}

func multihashKeyFromSlotNumber(sl Slot) (cid.Cid, error) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(sl))
	h, err := multihash.Sum(buf[:], multihash.SHA2_256, -1)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, h), nil
}

func encodeSlotToCID(slot Slot, cid cid.Cid) []byte {
	buf := make([]byte, 8+cid.ByteLen())
	binary.LittleEndian.PutUint64(buf, uint64(slot))
	copy(buf[8:], cid.Bytes())
	return buf
}

// Get returns an account from the store.
func (as *Store) Get(ctx context.Context, slot Slot) (*SlotToCID, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	slnh, err := multihashKeyFromSlotNumber(slot)
	if err != nil {
		return nil, err
	}
	value, found, err := as.store.Get(slnh.Bytes())
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrNotFound{Slot: uint64(slot)}
	}
	parsed, err := parseSlotToCID(value)
	if err != nil {
		return nil, err
	}
	if parsed.Slot != slot {
		return nil, ErrWrongSlot
	}
	return parsed, nil
}

// GetSize returns the size of an account in the store.
func (as *Store) GetSize(ctx context.Context, slot Slot) (int, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	pkh, err := multihashKeyFromSlotNumber(slot)
	if err != nil {
		return 0, err
	}
	// unoptimized implementation for now
	size, found, err := as.store.GetSize(pkh.Bytes())
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, ErrNotFound{Slot: uint64(slot)}
	}
	return int(size), nil
}

// Put puts a given slot in the underlying store.
func (as *Store) Put(ctx context.Context, slot Slot, id cid.Cid) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	pkh, err := multihashKeyFromSlotNumber(slot)
	if err != nil {
		return err
	}
	err = as.store.Put(pkh.Bytes(), encodeSlotToCID(slot, id))

	// TODO:
	if err == storetypes.ErrKeyExists {
		return nil
	}
	return err
}

type PutMany map[Slot]cid.Cid

// PutMany puts a slice of slots at the same time using batching
// capabilities of the underlying datastore whenever possible.
func (as *Store) PutMany(ctx context.Context, accs PutMany) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for slot, id := range accs {
		pkh, err := multihashKeyFromSlotNumber(slot)
		if err != nil {
			return err
		}
		err = as.store.Put(pkh.Bytes(), encodeSlotToCID(slot, id))
		if err != nil && err != storetypes.ErrKeyExists {
			return err
		}
	}
	return nil
}

func (as *Store) AllKeysChan(ctx context.Context) (<-chan Slot, error) {
	// return nil, ErrNotSupported
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	iter := as.store.NewIterator()

	ch := make(chan Slot)
	go func() {
		defer close(ch)
		for slotHash, val, err := iter.Next(); err == nil; slotHash, _, err = iter.Next() {
			_ = slotHash
			// parse val[:8] as uint64
			slotNum := Slot(binary.LittleEndian.Uint64(val[:8]))
			select {
			case ch <- slotNum:
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
