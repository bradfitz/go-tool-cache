package cachers

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync/atomic"
)

// Counts keeps counts cache events
type Counts struct {
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

func (l *LocalCacheWithCounts) Kind() string {
	return l.Kind()
}

type RemoteCacheWithCounts struct {
	Counts
	cache RemoteCache
}

func (r *RemoteCacheWithCounts) Kind() string {
	return r.cache.Kind()
}

func (r *RemoteCacheWithCounts) Start(ctx context.Context) error {
	return r.cache.Start(ctx)
}

func (r *RemoteCacheWithCounts) Close() error {
	log.Printf("[%s]\t%s", r.cache.Kind(), r.Summary())
	return r.cache.Close()
}

func (r *RemoteCacheWithCounts) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	r.gets.Add(1)
	outputID, size, output, err = r.cache.Get(ctx, actionID)
	if err != nil {
		r.getErrors.Add(1)
		return
	}
	if outputID == "" {
		r.misses.Add(1)
		return
	}
	r.hits.Add(1)
	return
}

func (r *RemoteCacheWithCounts) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error) {
	err = r.cache.Put(ctx, actionID, outputID, size, body)
	if err != nil {
		r.putErrors.Add(1)
		return
	}
	r.puts.Add(1)
	return
}

func (l *LocalCacheWithCounts) Start(ctx context.Context) error {
	return l.cache.Start(ctx)
}

func (l *LocalCacheWithCounts) Close() error {
	log.Printf("[%s]\t%s", l.cache.Kind(), l.Summary())
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

func NewLocalCacheStates(cache LocalCache) *LocalCacheWithCounts {
	return &LocalCacheWithCounts{
		cache: cache,
	}
}

func NewRemoteCacheStats(cache RemoteCache) *RemoteCacheWithCounts {
	return &RemoteCacheWithCounts{
		cache: cache,
	}
}

var _ LocalCache = &LocalCacheWithCounts{}
var _ RemoteCache = &RemoteCacheWithCounts{}
