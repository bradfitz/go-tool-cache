package cachers

import (
	"context"
	"io"
)

// Cache is the interface implemented by all caches.
type Cache interface {
	Start(ctx context.Context) error
	Close() error
}

// LocalCache is the basic interface for a local cache.
// It supposed to write to Disk, thus the signature include diskPath.
type LocalCache interface {
	Cache
	Get(ctx context.Context, actionID string) (outputID, diskPath string, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error)
}

// RemoteCache is the basic interface for a remote cache.
type RemoteCache interface {
	Cache
	Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error)
}
