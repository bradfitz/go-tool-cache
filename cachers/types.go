package cachers

import (
	"context"
	"errors"
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
}

// ActionValue is the JSON value returned by the upstream cacher server for a GetAction request.
type ActionValue struct {
	OutputID string `json:"outputID"`
	Size     int64  `json:"size"`
}

// Upstream provides access to the upstream cacher server.
type Upstream interface {
	// GetAction returns the ActionValue for the given actionID.
	GetAction(ctx context.Context, actionID string) (*ActionValue, error)
	// GetOutput returns the output body for the given outputID.
	GetOutput(ctx context.Context, outputID string) (body io.ReadCloser, err error)
	// Put stores an action with the given output id and body in upstream.
	Put(ctx context.Context, actionID string, outputID string, size int64, body io.Reader) error
}

var errNotFound = errors.New("not found")

func IgnoreNotFound(err error) error {
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}