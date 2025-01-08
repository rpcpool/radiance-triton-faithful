package createcar

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"go.firedancer.io/radiance/pkg/ipld/car"
	"k8s.io/klog/v2"
)

// copied from pkg/ipld/cargen/cargen.go
type carHandle struct {
	file              *os.File
	cache             *bufio.Writer
	writer            *car.Writer
	lastOffset        int64
	mu                *sync.Mutex
	numWrittenObjects *atomic.Uint64
	sizeStats         *StatsSizeByKind
}

const (
	writeBufSize = MiB * 1
)

func (c *carHandle) open(finalCARFilepath string, numObj *atomic.Uint64, sizeStats *StatsSizeByKind) error {
	if c.ok() {
		return fmt.Errorf("handle not closed")
	}
	file, err := os.OpenFile(finalCARFilepath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create CAR: %w", err)
	}
	cache := bufio.NewWriterSize(file, writeBufSize)
	writer, err := car.NewWriter(cache, car.CBOR_SHA256_DUMMY_CID)
	if err != nil {
		return fmt.Errorf("failed to start CAR at %s: %w", finalCARFilepath, err)
	}
	*c = carHandle{
		file:              file,
		cache:             cache,
		writer:            writer,
		lastOffset:        0,
		mu:                &sync.Mutex{},
		numWrittenObjects: numObj,
		sizeStats:         sizeStats,
	}
	klog.Infof("Created new CAR file %s", file.Name())
	return nil
}

func (c *carHandle) ok() bool {
	return c.writer != nil
}

func (c *carHandle) close() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err = c.cache.Flush(); err != nil {
		return err
	}
	err = c.file.Close()
	if err != nil {
		return err
	}
	*c = carHandle{}
	return
}

const MAX_BLOCK_SIZE = 1 << 20

func (c *carHandle) WriteBlock(block car.Block) error {
	totalLength := len(block.Data) + block.Cid.ByteLen()
	if totalLength > MAX_BLOCK_SIZE {
		return fmt.Errorf("block too large: %d bytes (max = %d, %d bytes too big)", totalLength, MAX_BLOCK_SIZE, totalLength-MAX_BLOCK_SIZE)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.numWrittenObjects.Add(1)
	c.sizeStats.Infer(block.Data)
	return c.writer.WriteBlock(block)
}

func (c *carHandle) NumberOfWrittenObjects() uint64 {
	return c.numWrittenObjects.Load()
}
