package cachers

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"
)

// timeKeeper can be used to measure time and bytes of operations
// It works in async channel to not block the main thread
type timeKeeper struct {
	Count             int64
	TotalBytes        int64
	AvgBytesPerSecond float64
	metricsChan       chan metric
	wg                *errgroup.Group
}

// metric holds data of single op event: size in bytes and duration
type metric struct {
	bytes    int64
	duration time.Duration
}

func newTimeKeeper() *timeKeeper {
	return &timeKeeper{
		metricsChan: make(chan metric, 1024),
	}
}

func (c *timeKeeper) Start(ctx context.Context) {
	c.wg, _ = errgroup.WithContext(ctx)
	c.wg.Go(func() error {
		for m := range c.metricsChan {
			c.TotalBytes += m.bytes
			speed := float64(m.bytes) / m.duration.Seconds()
			c.AvgBytesPerSecond = newAverage(c.AvgBytesPerSecond, c.Count, speed)
			c.Count++
		}
		return nil
	})
}

func (c *timeKeeper) Stop() error {
	close(c.metricsChan)
	return c.wg.Wait()
}

func (c *timeKeeper) Summary() string {
	return fmt.Sprintf("%s (%s/sec)",
		formatBytes(float64(c.TotalBytes)), formatBytes(c.AvgBytesPerSecond))
}

func newAverage(oldAverage float64, count int64, newValue float64) float64 {
	return (oldAverage*float64(count) + newValue) / float64(count+1)
}

func (c *timeKeeper) DoWithMeasure(bytesCount int64, f func() (string, error)) (string, error) {
	start := time.Now()
	s, err := f()
	duration := time.Since(start)
	if err == nil {
		c.metricsChan <- metric{
			bytes:    bytesCount,
			duration: duration,
		}
	}
	return s, err
}

// formatBytes formats a number of bytes into a human-readable string.
func formatBytes(size float64) string {
	const (
		b = 1 << (10 * iota)
		kb
		mb
		gb
		tb
		pb
		eb
	)
	switch {
	case size < kb:
		return fmt.Sprintf("%.2f B", size)
	case size < mb:
		return fmt.Sprintf("%.2f KB", size/float64(kb))
	case size < gb:
		return fmt.Sprintf("%.2f MB", size/float64(mb))
	case size < tb:
		return fmt.Sprintf("%.2f GB", size/float64(gb))
	case size < pb:
		return fmt.Sprintf("%.2f TB", size/float64(tb))
	case size < eb:
		return fmt.Sprintf("%.2f PB", size/float64(pb))
	default:
		return fmt.Sprintf("%.2f EB", size/float64(eb))
	}
}
