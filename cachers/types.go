package cachers

import (
	"context"
	"errors"
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

// ActionValue is the JSON value returned by the cacher server for an GET /action request.
type ActionValue struct {
	OutputID string `json:"outputID"`
	Size     int64  `json:"size"`
}

type Upstream interface {
	GetAction(ctx context.Context, actionID string) (*ActionValue, error)
	GetOutput(ctx context.Context, outputID string) (body io.ReadCloser, err error)

	Put(ctx context.Context, actionID string, outputID string, size int64, body io.Reader) error
}

var errNotFound = errors.New("not found")

func IgnoreNotFound(err error) error {
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}