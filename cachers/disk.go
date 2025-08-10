package cachers

import (
	"bytes"
	"context"
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
	action2 := actionID[:2]

	actionFile := filepath.Join(dc.Dir, action2, fmt.Sprintf("a-%s", actionID))
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
	output2 := ie.OutputID[:2]
	return ie.OutputID, filepath.Join(dc.Dir, output2, fmt.Sprintf("o-%v", ie.OutputID)), nil
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

func validHex(x string) bool {
	if len(x) < 4 || len(x) > 100 {
		return false
	}
	for i := range len(x) {
		b := x[i]
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
	if !validHex(actionID) || !validHex(outputID) {
		return "", fmt.Errorf("actionID and outputID must be valid hex strings")
	}

	action2, output2 := actionID[:2], outputID[:2]
	outputDir := filepath.Join(dc.Dir, output2)
	actionDir := filepath.Join(dc.Dir, action2)

	if err := os.MkdirAll(actionDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create action directory: %w", err)
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	file := filepath.Join(outputDir, fmt.Sprintf("o-%s", outputID))

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
	actionFile := filepath.Join(actionDir, fmt.Sprintf("a-%s", actionID))
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
