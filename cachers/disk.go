package cachers

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

// indexEntry is the metadata that DiskCache stores on disk for an ActionID.
type indexEntry struct {
	Version   int    `json:"v"`
	OutputID  string `json:"o"`
	Size      int64  `json:"n"`
	TimeNanos int64  `json:"t"`
}

type DiskCache struct {
	Dir     string
	Verbose bool
}

func (dc *DiskCache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	actionFile := filepath.Join(dc.Dir, fmt.Sprintf("a-%s", actionID))
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			if dc.Verbose {
				log.Printf("disk miss: %v", actionID)
			}
		}
		return "", "", err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		log.Printf("Warning: JSON error for action %q: %v", actionID, err)
		return "", "", nil
	}
	if _, err := hex.DecodeString(ie.OutputID); err != nil {
		// Protect against malicious non-hex OutputID on disk
		return "", "", nil
	}
	return ie.OutputID, filepath.Join(dc.Dir, fmt.Sprintf("o-%v", ie.OutputID)), nil
}

func (dc *DiskCache) OutputFilename(outputID string) string {
	if len(outputID) < 4 || len(outputID) > 1000 {
		return ""
	}
	for i := range outputID {
		b := outputID[i]
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return ""
	}
	return filepath.Join(dc.Dir, fmt.Sprintf("o-%s", outputID))
}

func (dc *DiskCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	file := filepath.Join(dc.Dir, fmt.Sprintf("o-%s", outputID))

	// Special case empty files; they're both common and easier to do race-free.
	if size == 0 {
		zf, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
		zf.Close()
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
		OutputID:  outputID,
		Size:      size,
		TimeNanos: time.Now().UnixNano(),
	})
	if err != nil {
		return "", err
	}
	actionFile := filepath.Join(dc.Dir, fmt.Sprintf("a-%s", actionID))
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		return "", err
	}
	return file, nil
}

func writeAtomic(dest string, r io.Reader) (int64, error) {
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
