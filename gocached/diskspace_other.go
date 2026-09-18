//go:build !unix

package gocached

import "errors"

// fsTotalBytes returns the total size in bytes of the filesystem holding dir.
// It is not implemented on this platform; callers fall back to a default.
func fsTotalBytes(dir string) (int64, error) {
	return 0, errors.ErrUnsupported
}
