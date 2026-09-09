//go:build !windows

package cachers

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
)

func writeActionFile(dest string, b []byte) error {
	_, err := writeAtomic(dest, bytes.NewReader(b), 0o644)
	return err
}

// writeOutputFile writes output files with the executable bit set
// because they can be linked binaries (e.g. cached test binaries)
// that cmd/go executes directly from the cache directory.
func writeOutputFile(dest string, r io.Reader, _ int64, _ string) (int64, error) {
	return writeAtomic(dest, r, 0o755)
}

func writeAtomic(dest string, r io.Reader, perm os.FileMode) (int64, error) {
	tf, err := os.CreateTemp(filepath.Dir(dest), filepath.Base(dest)+".*")
	if err != nil {
		return 0, err
	}
	size, err := io.Copy(tf, r)
	if err != nil {
		tf.Close()
		os.Remove(tf.Name())
		return 0, err
	}
	if err := tf.Chmod(perm); err != nil {
		tf.Close()
		os.Remove(tf.Name())
		return 0, err
	}
	if err := tf.Close(); err != nil {
		os.Remove(tf.Name())
		return 0, err
	}
	if err := os.Rename(tf.Name(), dest); err != nil {
		os.Remove(tf.Name())
		return 0, err
	}
	return size, nil
}
