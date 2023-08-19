package cachers

import (
	"context"
	"io"
)

type ActionCache interface {
	Get(ctx context.Context, actionID string) (outputID string, size int64, err error)
	Put(ctx context.Context, actionID string, outputID string, size int64) (err error)
}

type OutputDiskCache interface {
	Get(ctx context.Context, outputID string) (diskPath string, err error)
	Put(ctx context.Context, outputID string, size int64, body io.Reader) (diskPath string, err error)
}

// Cache provides cache access.
type Cache interface {
	// Get returns the outputID and diskPath for the given actionID.
	Get(ctx context.Context, actionID string) (outputID, diskPath string, err error)

	// Put stores the given outputID and body for the given actionID.
	Put(
		ctx context.Context,
		actionID string,
		outputID string,
		size int64,
		body io.Reader,
	) (diskPath string, err error)
}
