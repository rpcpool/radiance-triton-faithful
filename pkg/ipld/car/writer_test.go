package car

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbornode "github.com/ipfs/go-ipld-cbor"
	"github.com/ipld/go-car"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWriter(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, CBOR_SHA256_DUMMY_CID)
	require.NoError(t, err)
	require.NotNil(t, w)

	// Ensure that CAR header has been written.
	assert.Equal(t,
		[]byte{
			0x3a,
			0xa2,
			0x65, 0x72, 0x6f, 0x6f, 0x74, 0x73,
			0x81,
			0xd8, 0x2a, 0x58, 0x25, 0x0, 0x1, 0x71, 0x12, 0x20, 0x52, 0xed, 0x28, 0xbe, 0xac, 0x33, 0xbc, 0x96, 0x24, 0x1a, 0x6e, 0x7, 0x4a, 0xa0, 0x83, 0xf0, 0x35, 0xa9, 0xf1, 0x60, 0x3f, 0x41, 0xff, 0x9f, 0x17, 0x32, 0x3f, 0x16, 0x62, 0x9a, 0x20, 0x5,
			0x67, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
			0x1,
		}, buf.Bytes())

	// Ensure CBOR can be deserialized.
	var x struct {
		Roots   []cid.Cid `refmt:"roots"`
		Version uint64    `refmt:"version"`
	}
	cbornode.RegisterCborType(x)
	err = cbornode.DecodeInto(buf.Bytes()[1:], &x)
	require.NoError(t, err)
	assert.Equal(t, []cid.Cid{CBOR_SHA256_DUMMY_CID}, x.Roots)
	assert.Equal(t, uint64(1), x.Version)
}

func TestNewWriter_Error(t *testing.T) {
	var mock mockWriter
	mock.err = io.ErrClosedPipe

	w, err := NewWriter(&mock, CBOR_SHA256_DUMMY_CID)
	assert.Nil(t, w)
	assert.Same(t, mock.err, err)
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w, err := NewWriter(&buf, CBOR_SHA256_DUMMY_CID)
	require.NoError(t, err)
	require.NotNil(t, w)

	// Write a bunch of data
	require.NoError(t, w.WriteBlock(NewBlockFromRaw([]byte{}, uint64(multicodec.Raw))))
	require.NoError(t, w.WriteBlock(NewBlockFromRaw([]byte("hello world"), uint64(multicodec.Raw))))

	assert.Equal(t, int64(buf.Len()), w.Written())

	// Load using ipld/go-car library
	ctx := context.Background()
	store := blockstore.NewBlockstore(ds.NewMapDatastore())
	ch, err := car.LoadCar(ctx, store, &buf)
	require.NoError(t, err)
	assert.Equal(t, &car.CarHeader{
		Roots:   []cid.Cid{CBOR_SHA256_DUMMY_CID},
		Version: 1,
	}, ch)

	allKeys, err := store.AllKeysChan(ctx)
	require.NoError(t, err)
	var keys []cid.Cid
	for key := range allKeys {
		keys = append(keys, key)
		t.Log(key.String())
	}
	assert.Len(t, keys, 2)

	b, err := store.Get(ctx, cid.MustParse("bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"))
	require.NoError(t, err)
	assert.Equal(t, b.RawData(), []byte{})

	b, err = store.Get(ctx, cid.MustParse("bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"))
	require.NoError(t, err)
	assert.Equal(t, b.RawData(), []byte("hello world"))
}

func TestCountingWriter(t *testing.T) {
	var mock mockWriter

	w := newCountingWriter(&mock)
	assert.Equal(t, int64(0), w.written())

	// successful write
	mock.n, mock.err = 5, nil
	n, err := w.Write([]byte("hello"))
	assert.Equal(t, mock.n, n)
	assert.Equal(t, mock.err, err)
	assert.Equal(t, int64(5), w.written())

	// partial write
	mock.n, mock.err = 3, io.ErrClosedPipe
	n, err = w.Write([]byte("hello"))
	assert.Equal(t, mock.n, n)
	assert.Equal(t, mock.err, err)
	assert.Equal(t, int64(8), w.written())

	// failed write
	mock.n, mock.err = 0, io.ErrClosedPipe
	n, err = w.Write([]byte("hello"))
	assert.Equal(t, mock.n, n)
	assert.Equal(t, mock.err, err)
	assert.Equal(t, int64(8), w.written())
}

type mockWriter struct {
	n   int
	err error
}

func (m *mockWriter) Write(_ []byte) (int, error) {
	return m.n, m.err
}
