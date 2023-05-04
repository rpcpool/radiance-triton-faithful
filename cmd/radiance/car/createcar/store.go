package createcar

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"go.firedancer.io/radiance/pkg/ipld/car"
	"k8s.io/klog/v2"
)

// copied from pkg/ipld/cargen/cargen.go
type carHandle struct {
	file       *os.File
	cache      *bufio.Writer
	writer     *car.Writer
	lastOffset int64
	mu         *sync.Mutex
}

const (
	writeBufSize = 256 * KiB
)

func (c *carHandle) open(finalCARFilepath string) error {
	if c.ok() {
		return fmt.Errorf("handle not closed")
	}
	f, err := os.OpenFile(finalCARFilepath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create CAR: %w", err)
	}
	cache := bufio.NewWriterSize(f, writeBufSize)
	writer, err := car.NewWriter(cache)
	if err != nil {
		return fmt.Errorf("failed to start CAR at %s: %w", finalCARFilepath, err)
	}
	*c = carHandle{
		file:       f,
		cache:      cache,
		writer:     writer,
		lastOffset: 0,
		mu:         &sync.Mutex{},
	}
	klog.Infof("Created new CAR file %s", f.Name())
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
	*c = carHandle{}
	return
}

func (c *carHandle) WriteBlock(block car.Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writer.WriteBlock(block)
}
