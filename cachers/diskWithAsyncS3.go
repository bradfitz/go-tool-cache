package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
)

type putWork struct {
	actionID string
	outputID string
	size     int64
	diskPath string
}

type DiskAsyncS3Cache struct {
	log            *slog.Logger
	localCache     *LocalCacheWithCounts
	s3Client       s3Client
	bucketName     string
	cacheKey       string
	remoteWork     chan putWork
	remoteWG       *sync.WaitGroup
	remoteGetCache *RemoteCacheWithCounts
	putsMetrics    *timeKeeper
	getsMetrics    *timeKeeper
	nWorkers       int
}

var _ LocalCache = &DiskAsyncS3Cache{}

func NewDiskAsyncS3Cache(localCache LocalCache, client s3Client, bucketName string, cacheKey string, queueLen int, nWorkers int) LocalCache {
	remoteGet := NewRemoteCacheStats(NewS3Cache(client, bucketName, cacheKey))
	remoteGet.Name = "remoteGet"
	return NewLocalCacheStats(&DiskAsyncS3Cache{
		log:        slog.Default().WithGroup("DiskAsyncS3"),
		remoteWork: make(chan putWork, queueLen),
		remoteWG:   &sync.WaitGroup{},
		nWorkers:   nWorkers,
		s3Client:   client,
		bucketName: bucketName,
		cacheKey:   cacheKey,
		// note: we initialize remoteWG in Start
		// TODO: instead of making wrappers, just integrate Counts with the real things
		localCache:     NewLocalCacheStats(localCache),
		remoteGetCache: remoteGet,
		putsMetrics:    newTimeKeeper(),
		getsMetrics:    newTimeKeeper(),
	})
}

func (c *DiskAsyncS3Cache) Start(ctx context.Context) error {
	err := c.localCache.Start(ctx)
	if err != nil {
		return fmt.Errorf("local cache start failed: %w", err)
	}

	err = c.remoteGetCache.Start(ctx)
	if err != nil {
		c.localCache.Close()
		return fmt.Errorf("remote cache start failed: %w", err)
	}

	c.log.Info("probing remote cache")
	probeStr := "_probe"
	err = c.remoteGetCache.Put(ctx, probeStr, probeStr, int64(len([]byte(probeStr))), bytes.NewReader([]byte(probeStr)))
	if err != nil {
		c.localCache.Close()
		c.remoteGetCache.Close()
		return fmt.Errorf("remote cache probe put failed: %w", err)
	}
	_, sz, _, err := c.remoteGetCache.Get(ctx, probeStr)
	if err != nil {
		c.localCache.Close()
		c.remoteGetCache.Close()
		return fmt.Errorf("remote cache probe get failed: %w", err)
	}
	if sz != int64(len([]byte(probeStr))) {
		c.localCache.Close()
		c.remoteGetCache.Close()
		return fmt.Errorf("remote cache probe get size mismatch: expected %d, got %d", len([]byte(probeStr)), sz)
	}
	c.log.Info("probe success")

	c.remoteWG.Add(c.nWorkers)
	for i := 0; i < c.nWorkers; i++ {
		go func() {
			defer c.remoteWG.Done()
			rc := NewRemoteCacheStats(NewS3Cache(c.s3Client, c.bucketName, c.cacheKey))
			rc.Name = "remoteWorker"
			err := rc.Start(ctx)
			if err != nil {
				c.log.Error("remote worker start failed", "err", err)
				return
			}
			defer rc.Close()
			for {
				select {
				case w, ok := <-c.remoteWork:
					if !ok {
						c.log.Info("remote worker done by closed work channel")
						return
					}
					c.log.Debug("remote worker put", "actionID", w.actionID, "outputID", w.outputID, "size", w.size, "diskPath", w.diskPath)
					var r io.Reader
					if w.size == 0 {
						r = bytes.NewReader(nil)
					} else {
						f, err := os.Open(w.diskPath)
						// TODO: currently we just log errors, but maybe we want a mode that fails
						if err != nil {
							c.log.Error("opening file for remote", "path", w.diskPath, "err", err)
							continue
						}
						defer f.Close()
						r = f
					}
					// TODO: not 100% on the lifetime of this context; is it until everything is started? or until Close? we may want a separate Context for workers so that they can be stopped before all work is done (i.e., on Close)
					err := rc.Put(ctx, w.actionID, w.outputID, w.size, r)
					if err != nil {
						c.log.Error("putting to remote", "actionID", w.actionID, "outputID", w.outputID, "err", err)
						continue
					}
				case <-ctx.Done():
					c.log.Info("remote worker done by ctx.Done")
					return
				}
			}
		}()
	}

	c.putsMetrics.Start(ctx)
	c.getsMetrics.Start(ctx)
	return nil
}

func (c *DiskAsyncS3Cache) Get(ctx context.Context, actionID string) (string, string, error) {
	c.log.Debug("get", "actionID", actionID)
	outputID, diskPath, err := c.localCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	outputID, size, output, err := c.remoteGetCache.Get(ctx, actionID)
	if err != nil {
		return "", "", err
	}
	if outputID == "" {
		return "", "", nil
	}
	diskPath, err = c.getsMetrics.DoWithMeasure(size, func() (string, error) {
		defer output.Close()
		return c.localCache.Put(ctx, actionID, outputID, size, output)
	})
	if err != nil {
		return "", "", err
	}
	return outputID, diskPath, nil
}

func (c *DiskAsyncS3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (string, error) {
	c.log.Debug("put", "actionID", actionID, "outputID", outputID, "size", size)
	// special case for empty files, nead empty reader
	if size == 0 {
		body = bytes.NewReader(nil)
	}

	// TODO: restore metrics
	diskPath, err := c.localCache.Put(ctx, actionID, outputID, size, body)
	if err != nil {
		return "", fmt.Errorf("local cache put failed: %w", err)
	}
	c.remoteWork <- putWork{
		actionID: actionID,
		outputID: outputID,
		size:     size,
		diskPath: diskPath,
	}
	return diskPath, nil
}

func (c *DiskAsyncS3Cache) Close() error {
	c.log.Info("close")
	var errAll error
	if err := c.localCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("local cache stop failed: %w", err), errAll)
	}
	// TODO: this means we wait till all the remote workers finish; we may want to just abandon the rest of the work (or offer a mode)
	close(c.remoteWork)
	c.remoteWG.Wait()
	if err := c.remoteGetCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("remote get cache stop failed: %w", err), errAll)
	}
	if err := c.putsMetrics.Stop(); err != nil {
		errAll = errors.Join(fmt.Errorf("puts metrics stop failed: %w", err), errAll)
	}
	if err := c.getsMetrics.Stop(); err != nil {
		errAll = errors.Join(fmt.Errorf("gets metrics stop failed: %w", err), errAll)
	}
	// TODO: pull out the metrics into log KV
	c.log.Info(fmt.Sprintf("Downloads: %s, Uploads %s", c.getsMetrics.Summary(), c.putsMetrics.Summary()))
	return errAll
}
