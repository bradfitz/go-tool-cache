package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/go-multierror"
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
		cache.localCache = NewLocalCacheStats(localCache)
		cache.remoteCache = NewRemoteCacheStats(remoteCache)
		return NewLocalCacheStats(cache)
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
	if l.verbose {
		log.Printf("[%s]\tGet(%q)", l.Kind(), actionID)
	}
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

func (l *CombinedCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (string, error) {
	if l.verbose {
		log.Printf("[%s]\tPut(%q, %q, %d)", l.Kind(), actionID, outputID, size)
	}
	// special case for empty files, nead empty reader
	// TODO: not sure why/when this would happen
	// TODO: seems like for disk and s3 at least, Put(..., 0, nil) should work automatically
	if size == 0 {
		path, err := l.localCache.Put(ctx, actionID, outputID, size, bytes.NewReader(nil))
		multierror.Append(err, l.remoteCache.Put(ctx, actionID, outputID, size, bytes.NewReader(nil)))
		return path, err
	}

	pr, pw := io.Pipe()
	tr := io.TeeReader(body, pw)
	wg, wgCtx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		_, err := l.putsMetrics.DoWithMeasure(size, func() (string, error) {
			err := l.remoteCache.Put(wgCtx, actionID, outputID, size, pr)
			// TODO: don't know if we should close the reader here, or Put should
			if err != nil {
				pr.CloseWithError(err)
			}
			return "", err
		})
		return err
	})

	// TODO: restore metrics
	diskPath, err := l.localCache.Put(ctx, actionID, outputID, size, tr)
	if err != nil {
		return diskPath, err
	}
	pw.Close()
	if remoteErr := wg.Wait(); remoteErr != nil {
		// only log errors on remote
		// TODO: maybe a mode that *does* fail if remote fails?
		log.Printf("[%s]\terror: %v", l.remoteCache.Kind(), remoteErr)
	}
	return diskPath, err
}

func (l *CombinedCache) Close() error {
	if l.verbose {
		log.Printf("[%s]\tClose()", l.Kind())
	}
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
