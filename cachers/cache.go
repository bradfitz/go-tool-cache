package cachers

import (
	"context"
	"io"
	"log"
)

// Cache is the interface implemented by all caches.
type Cache interface {
	Start(ctx context.Context) error
	Close() error
	Kind() string
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

type NoopLocalCache struct {
	verbose bool
}

func (c *NoopLocalCache) Start(ctx context.Context) error {
	return nil
}

func (c *NoopLocalCache) Close() error {
	return nil
}

func (c *NoopLocalCache) Kind() string {
	return "noop"
}

func (c *NoopLocalCache) Get(ctx context.Context, actionID string) (outputID string, diskPath string, err error) {
	if c.verbose {
		log.Printf("[%s]\tGet(%q)", c.Kind(), actionID)
	}
	return "", "", nil
}

func (c *NoopLocalCache) Put(ctx context.Context, actionID string, outputID string, size int64, body io.Reader) (diskPath string, err error) {
	if c.verbose {
		log.Printf("[%s]\tPut(%q, %q, %d bytes)", c.Kind(), actionID, outputID, size)
	}
	// combined has a pipe here, so we need to read the body to avoid deadlock
	io.ReadAll(body)
	return "", nil
}
