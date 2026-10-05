package createcar

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	bin "github.com/gagliardetto/binary"
	"go.firedancer.io/radiance/pkg/genesis"
	"go.firedancer.io/radiance/pkg/slotedges"
	"k8s.io/klog/v2"
)

// checkGenesisEpochSchedule compares the chosen epoch schedule with the one in the
// ledger's genesis.bin (agave keeps it next to the rocksdb dir), so a CAR is not
// built with the wrong epoch boundaries for its cluster.
func checkGenesisEpochSchedule(dbs []string, cluster string, want slotedges.EpochSchedule) {
	for _, db := range dbs {
		path := filepath.Join(filepath.Dir(filepath.Clean(db)), "genesis.bin")
		got, err := readGenesisEpochSchedule(path)
		if errors.Is(err, fs.ErrNotExist) {
			klog.Infof("No genesis.bin at %s; not checking --cluster=%s against genesis", path, cluster)
			continue
		}
		if err != nil {
			klog.Warningf("Failed to read epoch schedule from %s: %s; not checking --cluster=%s against genesis", path, err, cluster)
			continue
		}
		if got != want {
			klog.Exitf("Genesis at %s has epoch schedule %s, but --cluster=%s uses %s; pass the matching --cluster", path, got, cluster, want)
		}
		klog.Infof("Epoch schedule matches genesis at %s", path)
	}
}

func readGenesisEpochSchedule(path string) (slotedges.EpochSchedule, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return slotedges.EpochSchedule{}, err
	}
	var g genesis.Genesis
	// Only the epoch schedule matters here, so trailing fields added by newer
	// genesis versions are ignored.
	if err := bin.NewBinDecoder(b).Decode(&g); err != nil {
		return slotedges.EpochSchedule{}, err
	}
	return slotedges.EpochSchedule{
		SlotsPerEpoch:    g.EpochSchedule.SlotPerEpoch,
		FirstNormalEpoch: g.EpochSchedule.FirstNormalEpoch,
		FirstNormalSlot:  g.EpochSchedule.FirstNormalSlot,
	}, nil
}
