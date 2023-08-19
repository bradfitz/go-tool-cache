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

type DiskActionCache struct {
	Dir     string
	Verbose bool
}

func NewDiskActionCache(dir string, verbose bool) *DiskActionCache {
	return &DiskActionCache{
		Dir:     dir,
		Verbose: verbose,
	}
}

var _ ActionCache = (*DiskActionCache)(nil)

func (dac *DiskActionCache) actionFile(actionID string) string {
	return filepath.Join(dac.Dir, fmt.Sprintf("a-%s", actionID))
}

func (dac *DiskActionCache) Get(ctx context.Context, actionID string) (outputID string, size int64, err error) {
	actionFile := dac.actionFile(actionID)
	ij, err := os.ReadFile(actionFile)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			if dac.Verbose {
				log.Printf("disk miss: %v", actionID)
			}
		}
		return "", 0, err
	}
	var ie indexEntry
	if err := json.Unmarshal(ij, &ie); err != nil {
		log.Printf("Warning: JSON error for action %q: %v", actionID, err)
		return "", 0, nil
	}
	if _, err := hex.DecodeString(ie.OutputID); err != nil {
		// Protect against malicious non-hex OutputID on disk
		return "", 0, nil
	}
	return ie.OutputID, ie.Size, nil

}

func (dac *DiskActionCache) Put(
	ctx context.Context,
	actionID string,
	outputID string,
	size int64,
) (err error) {
	ij, err := json.Marshal(indexEntry{
		Version:   1,
		OutputID:  outputID,
		Size:      size,
		TimeNanos: time.Now().UnixNano(),
	})
	if err != nil {
		return err
	}
	actionFile := dac.actionFile(actionID)
	if _, err := writeAtomic(actionFile, bytes.NewReader(ij)); err != nil {
		return err
	}
	return nil
}

type LocalOutputDiskCache struct {
	Dir     string
	Verbose bool
}

func NewLocalOutputDiskCache(dir string, verbose bool) *LocalOutputDiskCache {
	return &LocalOutputDiskCache{
		Dir:     dir,
		Verbose: verbose,
	}
}

var _ OutputDiskCache = (*LocalOutputDiskCache)(nil)

func (local *LocalOutputDiskCache) outputFile(outputID string) string {
	return filepath.Join(local.Dir, fmt.Sprintf("o-%s", outputID))
}

func (local *LocalOutputDiskCache) Get(
	ctx context.Context,
	outputID string,
) (diskPath string, err error) {
	if len(outputID) < 4 || len(outputID) > 1000 {
		return "", fmt.Errorf("invalid outputID %q", outputID)
	}
	for i := range outputID {
		b := outputID[i]
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return "", fmt.Errorf("invalid outputID %q", outputID)
	}

	return local.outputFile(outputID), nil
}

func (local *LocalOutputDiskCache) Put(
	ctx context.Context,
	outputID string,
	size int64,
	body io.Reader,
) (diskPath string, err error) {
	outputFile := local.outputFile(outputID)

	// Special case empty files; they're both common and easier to do race-free.
	if size == 0 {
		zf, err := os.OpenFile(outputFile, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return "", err
		}
		if err := zf.Close(); err != nil {
			return "", err
		}
		return outputFile, nil
	}

	wrote, err := writeAtomic(outputFile, body)
	if err != nil {
		return "", err
	}
	if wrote != size {
		return "", fmt.Errorf("wrote %d bytes, expected %d", wrote, size)
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

type DiskCache struct {
	Action ActionCache
	Output OutputDiskCache
}

func NewDiskCache(dir string, verbose bool) *DiskCache {
	return &DiskCache{
		Action: NewDiskActionCache(dir, verbose),
		Output: NewLocalOutputDiskCache(dir, verbose),
	}
}

func (dc *DiskCache) Get(ctx context.Context, actionID string) (string, string, error) {
	outputID, _, err := dc.Action.Get(ctx, actionID)
	if err != nil {
		return "", "", err
	}

	diskPath, err := dc.Output.Get(ctx, outputID)
	if err != nil {
		return "", "", err
	}
	return outputID, diskPath, nil
}

func (dc *DiskCache) Put(ctx context.Context, actionID, objectID string, size int64, body io.Reader) (diskPath string, _ error) {
	diskPath, err := dc.Output.Put(ctx, objectID, size, body)
	if err != nil {
		return "", err
	}

	if err := dc.Action.Put(ctx, actionID, objectID, size); err != nil {
		return "", err
	}

	return diskPath, nil
}