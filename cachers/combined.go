package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
)

// CombinedCache is a LocalCache that wraps a LocalCache and a RemoteCache.
// It also keeps times for the remote cache Download/Uploads
type CombinedCache struct {
	verbose     bool
	localCache  LocalCache
	remoteCache RemoteCache
	putsMetrics *timeKeeper
	getsMetrics *timeKeeper
}

var _ LocalCache = &CombinedCache{}

func NewCombinedCache(localCache LocalCache, remoteCache RemoteCache, verbose bool) LocalCache {
	cache := &CombinedCache{
		verbose:     verbose,
		localCache:  localCache,
		remoteCache: remoteCache,
		putsMetrics: newTimeKeeper(),
		getsMetrics: newTimeKeeper(),
	}
	if verbose {
		cache.localCache = NewLocalCacheStates(localCache)
		cache.remoteCache = NewRemoteCacheStats(remoteCache)
		return NewLocalCacheStates(cache)
	}
	return cache
}

func (l *CombinedCache) Kind() string {
	return "combined"
}

func (l *CombinedCache) Start() error {
	err := l.localCache.Start()
	if err != nil {
		return fmt.Errorf("local cache start failed: %w", err)
	}
	err = l.remoteCache.Start()
	if err != nil {
		return fmt.Errorf("remote cache start failed: %w", err)
	}
	l.putsMetrics.Start()
	l.getsMetrics.Start()
	return nil
}

func (l *CombinedCache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = l.localCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	outputID, size, output, err := l.remoteCache.Get(ctx, actionID)
	if err != nil {
		return "", "", err
	}
	if outputID == "" {
		return "", "", nil
	}
	diskPath, err = l.getsMetrics.DoWithMeasure(size, func() (string, error) {
		defer output.Close()
		return l.localCache.Put(ctx, actionID, outputID, size, output)
	})
	if err != nil {
		return "", "", err
	}
	return outputID, diskPath, nil
}

func (l *CombinedCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (string, error) {
	pr, pw := io.Pipe()
	diskPathCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		var putBody io.Reader = pr
		if size == 0 {
			putBody = bytes.NewReader(nil)
		}
		diskPath, err := l.localCache.Put(ctx, actionID, outputID, size, putBody)
		if err != nil {
			errCh <- err
		} else {
			diskPathCh <- diskPath
		}
	}()

	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewReader(nil)
	} else {

		putBody = io.TeeReader(body, pw)
	}
	// tolerate remote write errors
	_, _ = l.putsMetrics.DoWithMeasure(size, func() (string, error) {
		e := l.remoteCache.Put(ctx, actionID, outputID, size, putBody)
		return "", e
	})
	pw.Close()
	select {
	case err := <-errCh:
		log.Printf("[%s]\terror: %v", l.localCache.Kind(), err)
		return "", err
	case diskPath := <-diskPathCh:
		return diskPath, nil
	}
}

func (l *CombinedCache) Close() error {
	err := l.localCache.Close()
	if err != nil {
		err = fmt.Errorf("local cache stop failed: %w", err)
	}
	err = l.remoteCache.Close()
	if err != nil {
		err = errors.Join(fmt.Errorf("remote cache stop failed: %w", err))
	}
	l.putsMetrics.Stop()
	l.getsMetrics.Stop()
	if l.verbose {
		log.Printf("[%s]\tDownloads: %s, Uploads %s", l.remoteCache.Kind(), l.getsMetrics.Summary(), l.putsMetrics.Summary())
	}
	return err
}
