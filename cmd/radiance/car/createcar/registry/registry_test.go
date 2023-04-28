package registry

import (
	"io/ioutil"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/stretchr/testify/assert"
)

func TestRegistry(t *testing.T) {
	tf, err := ioutil.TempFile("", "registry")
	assert.NoError(t, err)

	cidLen := 36
	reg, err := newRegistryFromInterface(tf, cidLen)
	assert.NoError(t, err)

	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,                          // TODO: what is this?
			Codec:    uint64(multicodec.DagCbor), // See the multicodecs table: https://github.com/multiformats/multicodec/
			MhType:   uint64(multicodec.Sha2_256),
			MhLength: -1,
		},
	}
	emptyCID, err := lp.WithCodec(uint64(multicodec.DagCbor)).Sum([]byte{})
	assert.NoError(t, err)

	slot123UpdatedCID, err := lp.WithCodec(uint64(multicodec.DagCbor)).Sum([]byte{1})
	assert.NoError(t, err)

	slot999UpdatedCID, err := lp.WithCodec(uint64(multicodec.DagCbor)).Sum([]byte{99})
	assert.NoError(t, err)
	{

		slot := uint64(123)
		err = reg.SetCID(slot, emptyCID.Bytes())
		assert.NoError(t, err)
		{
			c, err := reg.GetCID(slot)
			assert.NoError(t, err)
			assert.Equal(t, emptyCID.Bytes(), c.Bytes())
		}
		{
			// update
			err = reg.SetCID(slot, slot123UpdatedCID.Bytes())
			assert.NoError(t, err)
			c, err := reg.GetCID(slot)
			assert.NoError(t, err)
			assert.Equal(t, slot123UpdatedCID.Bytes(), c.Bytes())
		}
	}
	{
		slot := uint64(999)
		err = reg.SetCID(slot, emptyCID.Bytes())
		assert.NoError(t, err)
		{
			c, err := reg.GetCID(slot)
			assert.NoError(t, err)
			assert.Equal(t, emptyCID.Bytes(), c.Bytes())
		}
		{
			// update
			err = reg.SetCID(slot, slot999UpdatedCID.Bytes())
			assert.NoError(t, err)
			c, err := reg.GetCID(slot)
			assert.NoError(t, err)
			assert.Equal(t, slot999UpdatedCID.Bytes(), c.Bytes())
		}
	}
	{
		assert.Equal(t, int(2), reg.Len())
		assert.Equal(t, int((8+1+4+36)+(8+1+4+36)), reg.fileLen)
	}
	// now close and reopen
	{
		reg, err = openExistingRegistryFromInterface(tf, cidLen)
		assert.NoError(t, err)
		spew.Dump(reg)
		assert.Equal(t, int(2), reg.Len())
		assert.Equal(t, int((8+1+4+36)+(8+1+4+36)), reg.fileLen)

		{
			c, err := reg.GetCID(123)
			assert.NoError(t, err)
			assert.Equal(t, slot123UpdatedCID.Bytes(), c.Bytes())
		}
		{
			c, err := reg.GetCID(999)
			assert.NoError(t, err)
			assert.Equal(t, slot999UpdatedCID.Bytes(), c.Bytes())
		}
		{
			// add a new one
			slot := uint64(456)
			err = reg.SetCID(slot, emptyCID.Bytes())
			assert.NoError(t, err)
			c, err := reg.GetCID(slot)
			assert.NoError(t, err)
			assert.Equal(t, emptyCID.Bytes(), c.Bytes())

			assert.Equal(t, int(3), reg.Len())
			assert.Equal(t, int((8+1+4+36)+(8+1+4+36)+(8+1+4+36)), reg.fileLen)

			// now close and reopen
			reg, err = openExistingRegistryFromInterface(tf, cidLen)
			assert.NoError(t, err)
			assert.Equal(t, int(3), reg.Len())
			assert.Equal(t, int((8+1+4+36)+(8+1+4+36)+(8+1+4+36)), reg.fileLen)
			c, err = reg.GetCID(slot)
			assert.NoError(t, err)
			assert.Equal(t, emptyCID.Bytes(), c.Bytes())
		}
		{
			// read a non-existent slot
			_, err := reg.GetCID(999999)
			assert.Error(t, err)
		}
		{
			// read all
			all, err := reg.GetAll()
			assert.NoError(t, err)
			assert.Equal(t, 3, len(all))
			assert.Equal(t, slot123UpdatedCID.Bytes(), all[0].CID)
			assert.Equal(t, slot999UpdatedCID.Bytes(), all[1].CID)
			assert.Equal(t, emptyCID.Bytes(), all[2].CID)
		}
	}
	assert.NoError(t, reg.Close())
}
