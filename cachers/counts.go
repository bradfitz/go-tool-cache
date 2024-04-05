package cachers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
)

// Counts keeps counts cache events
type Counts struct {
	// TODO: not sure why these need to be atomic
	gets      atomic.Int64
	hits      atomic.Int64
	misses    atomic.Int64
	puts      atomic.Int64
	getErrors atomic.Int64
	putErrors atomic.Int64
}

func (c *Counts) Summary() string {
	return fmt.Sprintf("%d gets (%d hits, %d misses, %d errors); %d puts (%d errors)",
		c.gets.Load(), c.hits.Load(), c.misses.Load(), c.getErrors.Load(), c.puts.Load(), c.putErrors.Load())
}

type LocalCacheWithCounts struct {
	Counts
	cache LocalCache
}

func (l *LocalCacheWithCounts) Start(ctx context.Context) error {
	return l.cache.Start(ctx)
}

func (l *LocalCacheWithCounts) Close() error {
	slog.Default().WithGroup("local").Info("close", "summary", l.Summary())
	return l.cache.Close()
}

func (l *LocalCacheWithCounts) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	l.gets.Add(1)
	outputID, diskPath, err = l.cache.Get(ctx, actionID)
	if err != nil {
		l.getErrors.Add(1)
		return
	}
	if outputID == "" {
		l.misses.Add(1)
		return
	}
	l.hits.Add(1)
	return
}

func (l *LocalCacheWithCounts) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error) {
	diskPath, err = l.cache.Put(ctx, actionID, outputID, size, body)
	if err != nil {
		l.putErrors.Add(1)
		return
	}
	l.puts.Add(1)
	return
}

func NewLocalCacheStats(cache LocalCache) *LocalCacheWithCounts {
	return &LocalCacheWithCounts{
		cache: cache,
	}
}

var _ LocalCache = &LocalCacheWithCounts{}
