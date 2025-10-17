package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	Logf    func(format string, args ...any) // optional alt logger
}

func (dc *DiskCache) logf(format string, args ...any) {
	if dc.Logf != nil {
		dc.Logf(format, args...)
	} else if dc.Verbose {
		log.Printf(format, args...)
	}
}

func (dc *DiskCache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	if !validHex(actionID) {
		return "", "", fmt.Errorf("actionID must be valid hex strings")
	}

	actionFile := dc.ActionFilename(actionID)
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			if dc.Verbose {
				dc.logf("disk miss: %v", actionID)
			}
		}
		return "", "", err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		dc.logf("Warning: JSON error for action %q: %v", actionID, err)
		return "", "", nil
	}
	if !validHex(ie.OutputID) {
		// Protect against malicious non-hex OutputID on disk
		return "", "", nil
	}
	return ie.OutputID, dc.OutputFilename(ie.OutputID), nil
}

func (dc *DiskCache) OutputFilename(outputID string) string {
	if !validHex(outputID) {
		return ""
	}
	return filepath.Join(dc.Dir, outputID[:2], fmt.Sprintf("o-%s", outputID))
}

func (dc *DiskCache) ActionFilename(actionID string) string {
	if !validHex(actionID) {
		return ""
	}
	return filepath.Join(dc.Dir, actionID[:2], fmt.Sprintf("a-%s", actionID))
}

func validHex(x string) bool {
	if len(x) < 4 || len(x) > 100 {
		return false
	}
	for _, b := range x {
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return false
	}
	return true
}

func (dc *DiskCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	if len(actionID) < 4 || len(outputID) < 4 {
		return "", fmt.Errorf("actionID and outputID must be at least 4 characters long")
	}
	if !validHex(actionID) {
		log.Printf("diskcache: got invalid actionID %q", actionID)
		return "", errors.New("actionID must be hex")
	}
	if !validHex(outputID) {
		log.Printf("diskcache: got invalid outputID %q", outputID)
		return "", errors.New("outputID must be hex")
	}

	actionFile := dc.ActionFilename(actionID)
	outputFile := dc.OutputFilename(outputID)
	actionDir := filepath.Dir(actionFile)
	outputDir := filepath.Dir(outputFile)

	if err := os.MkdirAll(actionDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create action directory: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	// Special case empty files; they're both common and easier to do race-free.
	if size == 0 {
		zf, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
		zf.Close()
	} else {
		wrote, err := writeAtomic(outputFile, body)
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
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		return "", err
	}
	return outputFile, nil
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
