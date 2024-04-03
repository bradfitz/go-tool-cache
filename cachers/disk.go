package cachers

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// indexEntry is the metadata that SimpleDiskCache stores on disk for an ActionID.
type indexEntry struct {
	Version   int    `json:"v"`
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeNanos int64  `json:"t"`
}

// SimpleDiskCache is a LocalCache that stores data on disk.
type SimpleDiskCache struct {
	dir string
	log *slog.Logger
}

func NewSimpleDiskCache(dir string) *SimpleDiskCache {
	return &SimpleDiskCache{
		dir: dir,
		log: slog.With("kind", "disk"),
	}
}

var _ LocalCache = &SimpleDiskCache{}

func (c *SimpleDiskCache) Start(context.Context) error {
	c.log.Info("start", "dir", c.dir)
	return os.MkdirAll(c.dir, 0755)
}

func (c *SimpleDiskCache) Get(_ context.Context, actionID string) (outputID, diskPath string, err error) {
	c.log.Debug("get", "actionID", actionID)
	actionFile := filepath.Join(c.dir, fmt.Sprintf("a-%s", actionID))
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return "", "", err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		c.log.Error("json error", "actionID", actionID, "err", err)
		return "", "", nil
	}
	if _, err := hex.DecodeString(ie.OutputID); err != nil {
		// Protect against malicious non-hex OutputID on disk
		return "", "", nil
	}
	return ie.OutputID, filepath.Join(c.dir, fmt.Sprintf("o-%v", ie.OutputID)), nil
}

func (c *SimpleDiskCache) Put(_ context.Context, actionID, objectID string, size int64, body io.Reader) (diskPath string, _ error) {
	c.log.Debug("put", "actionID", actionID, "objectID", objectID, "size", size)
	file := filepath.Join(c.dir, fmt.Sprintf("o-%s", objectID))

	// Special case empty files; they're both common and easier to do race-free.
	if size == 0 {
		zf, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
		_ = zf.Close()
	} else {
		wrote, err := writeAtomic(file, body)
		if err != nil {
			return "", err
		}
		if wrote != size {
			return "", fmt.Errorf("wrote %d bytes, expected %d", wrote, size)
		}
	}

	ij, err := json.Marshal(indexEntry{
		Version:   1,
		OutputID:  objectID,
		Size:      size,
		TimeNanos: time.Now().UnixNano(),
	})
	if err != nil {
		return "", err
	}
	actionFile := filepath.Join(c.dir, fmt.Sprintf("a-%s", actionID))
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		return "", err
	}
	return file, nil
}

func (c *SimpleDiskCache) Close() error {
	c.log.Info("close")
	return nil
}

func writeTempFile(dest string, r io.Reader) (string, int64, error) {
	tf, err := os.CreateTemp(filepath.Dir(dest), filepath.Base(dest)+".*")
	if err != nil {
		return "", 0, err
	}
	fileName := tf.Name()
	defer func() {
		_ = tf.Close()
		if err != nil {
			_ = os.Remove(fileName)
		}
	}()
	size, err := io.Copy(tf, r)
	if err != nil {
		return "", 0, err
	}
	return fileName, size, nil
}

func writeAtomic(dest string, r io.Reader) (int64, error) {
	tempFile, size, err := writeTempFile(dest, r)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tempFile)
		}
	}()
	if err = os.Rename(tempFile, dest); err != nil {
		return 0, err
	}
	return size, nil
}
