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

// DiskCache is a LocalCache that stores data on disk.
type DiskCache struct {
	Counts
	dir string
	log *slog.Logger
}

func NewDiskCache(dir string) *DiskCache {
	return &DiskCache{
		dir: dir,
		log: slog.Default().WithGroup("disk"),
	}
}

var _ LocalCache = &DiskCache{}

func (c *DiskCache) Start(context.Context) error {
	c.log.Info("start", "dir", c.dir)
	return os.MkdirAll(c.dir, 0755)
}

func (c *DiskCache) Get(_ context.Context, actionID string) (outputID, diskPath string, err error) {
	c.Counts.gets.Add(1)
	c.log.Debug("get", "actionID", actionID)
	actionFile := filepath.Join(c.dir, fmt.Sprintf("a-%s", actionID))
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		if os.IsNotExist(err) {
			c.Counts.misses.Add(1)
			return "", "", nil
		}
		c.Counts.getErrors.Add(1)
		return "", "", err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		c.log.Error("json error", "actionID", actionID, "err", err)
		c.Counts.getErrors.Add(1)
		return "", "", nil
	}
	if _, err := hex.DecodeString(ie.OutputID); err != nil {
		c.Counts.getErrors.Add(1)
		// Protect against malicious non-hex OutputID on disk
		return "", "", nil
	}
	c.Counts.hits.Add(1)
	return ie.OutputID, filepath.Join(c.dir, fmt.Sprintf("o-%v", ie.OutputID)), nil
}

func (c *DiskCache) Put(_ context.Context, actionID, objectID string, size int64, body io.Reader) (diskPath string, _ error) {
	c.Counts.puts.Add(1)
	c.log.Debug("put", "actionID", actionID, "objectID", objectID, "size", size)
	file := filepath.Join(c.dir, fmt.Sprintf("o-%s", objectID))

	// Special case empty files; they're both common and easier to do race-free.
	if size == 0 {
		zf, err := os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
		if err != nil {
			c.Counts.putErrors.Add(1)
			return "", err
		}
		_ = zf.Close()
	} else {
		wrote, err := writeAtomic(file, body)
		if err != nil {
			c.Counts.putErrors.Add(1)
			return "", err
		}
		if wrote != size {
			c.Counts.putErrors.Add(1)
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
		c.Counts.putErrors.Add(1)
		return "", err
	}
	actionFile := filepath.Join(c.dir, fmt.Sprintf("a-%s", actionID))
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		c.Counts.putErrors.Add(1)
		return "", err
	}
	return file, nil
}

func (c *DiskCache) Close() error {
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
