package cargen

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/dustin/go-humanize"
	"github.com/mattn/go-isatty"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.firedancer.io/radiance/pkg/iostats"
	"k8s.io/klog/v2"
)

func (w *Worker) initStatsTracker(ctx context.Context) error {
	var numTxns atomic.Uint64

	txRate := ewma.NewMovingAverage(7)
	lastStatsUpdate := time.Now()
	var lastNumTxns uint64
	updateEWMA := func() {
		now := time.Now()
		sinceLast := now.Sub(lastStatsUpdate)
		curNumTxns := numTxns.Load()
		increase := curNumTxns - lastNumTxns
		iRate := float64(increase) / sinceLast.Seconds()
		txRate.Add(iRate)
		lastNumTxns = curNumTxns
		lastStatsUpdate = now
	}
	stats := func() {
		numBytesRead, _ := iostats.GetDiskReadBytes()
		numBytesWritten, _ := iostats.GetDiskWriteBytes()

		klog.Infof(
			"[stats] tps=%.0f io-r=%s io-w=%s",
			txRate.Value(),
			humanize.Bytes(numBytesRead),
			humanize.Bytes(numBytesWritten),
		)
	}

	var barOutput io.Writer
	isAtty := isatty.IsTerminal(os.Stderr.Fd())
	if isAtty {
		barOutput = os.Stderr
	} else {
		barOutput = io.Discard
	}

	progress := mpb.NewWithContext(ctx, mpb.WithOutput(barOutput))
	bar := progress.New(int64(w.totalSlotsToProcess), mpb.BarStyle(),
		mpb.PrependDecorators(
			decor.Spinner(nil),
			decor.CurrentNoUnit(" %d"),
			decor.TotalNoUnit(" / %d slots"),
			decor.NewPercentage(" (% d)"),
		),
		mpb.AppendDecorators(
			decor.Name("eta="),
			decor.AverageETA(decor.ET_STYLE_GO),
		))

	if isAtty {
		klog.LogToStderr(false)
		klog.SetOutput(progress)
	}
	statInterval := time.Second * 5
	if statInterval > 0 {
		statTicker := time.NewTicker(statInterval)
		rateTicker := time.NewTicker(250 * time.Millisecond)
		go func() {
			defer statTicker.Stop()
			defer rateTicker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-statTicker.C:
					stats()
				case <-rateTicker.C:
					updateEWMA()
				}
			}
		}()
	}
	w.bar = bar
	w.numTxns = &numTxns
	return nil
}
