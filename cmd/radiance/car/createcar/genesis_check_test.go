package createcar

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.firedancer.io/radiance/fixtures"
	"go.firedancer.io/radiance/pkg/archiveutil"
	"go.firedancer.io/radiance/pkg/slotedges"
)

func TestReadGenesisEpochSchedule(t *testing.T) {
	// Unpack the mainnet genesis.bin next to a fake rocksdb dir, as in an agave ledger.
	ledger := t.TempDir()
	archive := fixtures.Open(t, "genesis", "mainnet.tar.bz2")
	defer archive.Close()
	files, err := archiveutil.OpenTar(archive)
	require.NoError(t, err)
	hdr, err := files.Next()
	require.NoError(t, err)
	require.Equal(t, "genesis.bin", hdr.Name)
	out, err := os.Create(filepath.Join(ledger, "genesis.bin"))
	require.NoError(t, err)
	_, err = io.Copy(out, files)
	require.NoError(t, err)
	require.NoError(t, out.Close())

	got, err := readGenesisEpochSchedule(filepath.Join(ledger, "genesis.bin"))
	require.NoError(t, err)
	require.Equal(t, slotedges.MainnetEpochSchedule, got)

	// A matching --cluster passes (a mismatch would exit).
	checkGenesisEpochSchedule([]string{filepath.Join(ledger, "rocksdb")}, "mainnet", slotedges.MainnetEpochSchedule)

	_, err = readGenesisEpochSchedule(filepath.Join(t.TempDir(), "genesis.bin"))
	require.ErrorIs(t, err, fs.ErrNotExist)
}
