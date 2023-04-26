package createcar

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type CountWriter struct {
	Count int
}

func (w *CountWriter) Write(p []byte) (n int, err error) {
	w.Count += len(p)
	return len(p), nil
}

func SizeOfFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fi.Size()), nil
}

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
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

func SortLinksByCID(links []datamodel.Link) {
	sort.Slice(links, func(i, j int) bool {
		return bytes.Compare(links[i].(cidlink.Link).Cid.Bytes(), links[j].(cidlink.Link).Cid.Bytes()) < 0
	})
}
