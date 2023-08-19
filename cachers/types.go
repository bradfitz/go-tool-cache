package cachers

import (
	"context"
	"io"
)

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

	// OutputFilename returns the disk path for the given outputID.
	OutputFilename(outputID string) string
}
