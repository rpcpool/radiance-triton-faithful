package createcar

import (
	"bufio"
	"fmt"
	"os"
	"sync"

	"github.com/iceber/iouring-go"
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

	ringW *RingedWriter
}

const (
	writeBufSize = MiB * 1
)

type RingedWriter struct {
	ring   *iouring.IOURing
	file   *os.File
	offset uint64
	mu     sync.Mutex
}

func NewRingedWriter(ring *iouring.IOURing, file *os.File) *RingedWriter {
	return &RingedWriter{
		ring: ring,
		file: file,
	}
}

func (w *RingedWriter) Write(data []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	compCh := make(chan iouring.Result, 1)
	_, err = w.ring.Pwrite(w.file, data, w.offset, compCh)
	if err != nil {
		return 0, err
	}
	w.offset += uint64(len(data))
	<-compCh
	return len(data), err
}

func (w *RingedWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ring == nil {
		return nil
	}
	return w.ring.Close()
}

func (c *carHandle) open(finalCARFilepath string) error {
	if c.ok() {
		return fmt.Errorf("handle not closed")
	}
	file, err := os.OpenFile(finalCARFilepath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		return fmt.Errorf("failed to create CAR: %w", err)
	}
	iour, err := iouring.New(10)
	if err != nil {
		return fmt.Errorf("failed to create iouring: %w", err)
	}
	ringWriter := NewRingedWriter(iour, file)
	cache := bufio.NewWriterSize(ringWriter, writeBufSize)
	writer, err := car.NewWriter(cache)
	if err != nil {
		return fmt.Errorf("failed to start CAR at %s: %w", finalCARFilepath, err)
	}
	*c = carHandle{
		file:       file,
		cache:      cache,
		writer:     writer,
		lastOffset: 0,
		mu:         &sync.Mutex{},
		ringW:      ringWriter,
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
	err = c.ringW.Close()
	if err != nil {
		return err
	}
	err = c.file.Close()
	if err != nil {
		return err
	}
	*c = carHandle{}
	return
}

func (c *carHandle) WriteBlock(block car.Block) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writer.WriteBlock(block)
}
