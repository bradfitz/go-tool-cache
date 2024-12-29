package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"golang.org/x/sync/errgroup"
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

func (l *CombinedCache) Start(ctx context.Context) error {
	err := l.localCache.Start(ctx)
	if err != nil {
		return fmt.Errorf("local cache start failed: %w", err)
	}
	err = l.remoteCache.Start(ctx)
	if err != nil {
		_ = l.localCache.Close()
		return fmt.Errorf("remote cache start failed: %w", err)
	}
	l.putsMetrics.Start(ctx)
	l.getsMetrics.Start(ctx)
	return nil
}

func (l *CombinedCache) Get(ctx context.Context, actionID string) (string, string, error) {
	outputID, diskPath, err := l.localCache.Get(ctx, actionID)
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

func (l *CombinedCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error) {
	if br, ok := body.(*bytes.Buffer); ok {
		return l.putBytes(ctx, actionID, outputID, size, br.Bytes())
	}
	pr, pw := io.Pipe()
	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error {
		var putBody io.Reader = pr
		if size == 0 {
			putBody = bytes.NewBuffer(nil)
		}
		var err2 error
		diskPath, err2 = l.localCache.Put(ctx, actionID, outputID, size, putBody)
		return err2
	})

	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewBuffer(nil)
	} else {

		putBody = io.TeeReader(body, pw)
	}
	// tolerate remote write errors
	_, _ = l.putsMetrics.DoWithMeasure(size, func() (string, error) {
		e := l.remoteCache.Put(ctx, actionID, outputID, size, putBody)
		return "", e
	})
	_ = pw.Close()
	if err := wg.Wait(); err != nil {
		log.Printf("[%s]\terror: %v", l.localCache.Kind(), err)
		return "", err
	}
	return diskPath, nil
}

func (l *CombinedCache) putBytes(ctx context.Context, actionID, outputID string, size int64, body []byte) (diskPath string, err error) {
	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error {
		var err2 error
		diskPath, err2 = l.localCache.Put(ctx, actionID, outputID, size, bytes.NewBuffer(body))
		return err2
	})

	// tolerate remote write errors
	_, _ = l.putsMetrics.DoWithMeasure(size, func() (string, error) {
		e := l.remoteCache.Put(ctx, actionID, outputID, size, bytes.NewBuffer(body))
		return "", e
	})

	if err := wg.Wait(); err != nil {
		log.Printf("[%s]\terror: %v", l.localCache.Kind(), err)
		return "", err
	}
	return diskPath, nil
}

func (l *CombinedCache) Close() error {
	var errAll error
	if err := l.localCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("local cache stop failed: %w", err), errAll)
	}
	if err := l.remoteCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("remote cache stop failed: %w", err), errAll)
	}
	if err := l.putsMetrics.Stop(); err != nil {
		errAll = errors.Join(fmt.Errorf("puts metrics stop failed: %w", err), errAll)
	}
	if err := l.getsMetrics.Stop(); err != nil {
		errAll = errors.Join(fmt.Errorf("gets metrics stop failed: %w", err), errAll)
	}
	if l.verbose {
		log.Printf("[%s]\tDownloads: %s, Uploads %s", l.remoteCache.Kind(), l.getsMetrics.Summary(), l.putsMetrics.Summary())
	}
	return errAll
}
