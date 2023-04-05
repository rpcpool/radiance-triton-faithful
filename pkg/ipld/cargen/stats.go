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
	discReadRate := ewma.NewMovingAverage(7)
	discWriteRate := ewma.NewMovingAverage(7)
	var lastNumTxns uint64
	var lastDiscReadRate uint64
	var lastDiscWriteRate uint64

	lastStatsUpdate := time.Now()
	updateEWMA := func() {
		now := time.Now()
		sinceLast := now.Sub(lastStatsUpdate)
		{
			curNumTxns := numTxns.Load()
			increase := curNumTxns - lastNumTxns
			iRate := float64(increase) / sinceLast.Seconds()
			txRate.Add(iRate)
			lastNumTxns = curNumTxns
		}
		{
			curDiscReadRate, _ := iostats.GetDiskReadBytes()
			increase := curDiscReadRate - lastDiscReadRate
			iRate := float64(increase) / sinceLast.Seconds()
			discReadRate.Add(iRate)
			lastDiscReadRate = curDiscReadRate
		}
		{
			curDiscWriteRate, _ := iostats.GetDiskWriteBytes()
			increase := curDiscWriteRate - lastDiscWriteRate
			iRate := float64(increase) / sinceLast.Seconds()
			discWriteRate.Add(iRate)
			lastDiscWriteRate = curDiscWriteRate
		}
		lastStatsUpdate = now
	}
	stats := func() {
		numBytesRead, _ := iostats.GetDiskReadBytes()
		numBytesWritten, _ := iostats.GetDiskWriteBytes()

		klog.Infof(
			"[stats] tps=%.0f io-r=%s io-w=%s io-r/s=%s io-w/s=%s",
			txRate.Value(),
			humanize.IBytes(numBytesRead),
			humanize.IBytes(numBytesWritten),
			humanize.IBytes(uint64(discReadRate.Value())),
			humanize.IBytes(uint64(discWriteRate.Value())),
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
