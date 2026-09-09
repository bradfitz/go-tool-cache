//go:build !windows

package cachers

import (
	"context"
	"os"
	"strings"
	"testing"
)

// TestOutputFileExecutable verifies that output files are written with the
// executable bit set, as cache entries can be linked binaries that cmd/go
// executes directly from the cache directory.
func TestOutputFileExecutable(t *testing.T) {
	dc := &DiskCache{Dir: t.TempDir()}
	actionID := strings.Repeat("a", 64)
	outputID := strings.Repeat("b", 64)
	body := strings.NewReader("some output")
	diskPath, err := dc.Put(context.Background(), actionID, outputID, int64(body.Len()), body)
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(diskPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fi.Mode().Perm(), os.FileMode(0o755); got != want {
		t.Errorf("output file mode = %v; want %v", got, want)
	}
}
