package main

import (
	"context"
	"expvar"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bradfitz/go-tool-cache/cachers"
)

func TestServer(t *testing.T) {
	ctx := context.Background()

	dir := t.TempDir()
	srv, err := newServer(dir)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	srv.logf = t.Logf
	srv.verbose = true

	var (
		timeMu sync.Mutex
		now    = time.Unix(1234, 0)
	)
	srv.clock = func() time.Time {
		timeMu.Lock()
		defer timeMu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) {
		timeMu.Lock()
		defer timeMu.Unlock()
		now = now.Add(d)
	}

	hs := httptest.NewServer(srv)
	defer hs.Close()

	mkClient := func() *cachers.HTTPClient {
		clientCacheDir := t.TempDir()
		return &cachers.HTTPClient{
			BaseURL: hs.URL,
			Disk: &cachers.DiskCache{
				Dir: clientCacheDir,
				Logf: func(format string, args ...any) {
					t.Logf("client-disk: "+format, args...)
				},
			},
		}
	}

	// wantMetric is a helper to check an expvar.Int metric and reset it
	// for future tests.
	wantMetric := func(m *expvar.Int, want int64) {
		t.Helper()
		if got := m.Value(); got != want {
			t.Errorf("metric = %d, want %d", got, want)
		}
		m.Set(0)
	}

	// Make two clients (imagine: two different builder VMs)
	c1 := mkClient()
	c2 := mkClient()

	const testActionID = "0001"
	const testActionIDMiss = "0002" // this one doesn't exist
	const testActionIDBig = "0bbb"  // non-inline object
	const testOutputID = "9900"
	const testOutputIDBig = "9bbb"
	const testObjectValue = "test data"
	testObjectValueBig := strings.Repeat("x", smallObjectSize+1)

	wantPut := func(c *cachers.HTTPClient, actionID, outputID string, val string) {
		t.Helper()
		clientDiskPath, err := c.Put(ctx, actionID, outputID, int64(len(val)), strings.NewReader(val))
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		if clientDiskPath == "" {
			t.Fatal("Put returned empty disk path")
		}
		wantMetric(&srv.Puts, 1)
		wrote, err := os.ReadFile(clientDiskPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(wrote) != val {
			t.Errorf("ReadFile got %q, want %q", wrote, val)
		}
	}

	wantGet := func(c *cachers.HTTPClient, actionID, outputID, wantVal string) {
		t.Helper()
		gotOutputID, diskPath, err := c.Get(ctx, actionID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if gotOutputID != outputID {
			t.Errorf("Get got outputID %q, want %q", gotOutputID, outputID)
		}
		if diskPath == "" {
			t.Fatal("Get returned empty disk path")
		}
		wrote, err := os.ReadFile(diskPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(wrote) != wantVal {
			t.Errorf("ReadFile got %q, want %q", wrote, wantVal)
		}
	}

	// Populate from the first client.
	wantPut(c1, testActionID, testOutputID, testObjectValue)
	wantPut(c1, testActionIDBig, testOutputIDBig, testObjectValueBig)

	// Read from the second client.
	wantGet(c2, testActionID, testOutputID, testObjectValue)
	wantGet(c2, testActionIDBig, testOutputIDBig, testObjectValueBig)

	// Check metrics
	wantMetric(&srv.Gets, 2)
	wantMetric(&srv.GetHits, 2)
	wantMetric(&srv.GetHitsInline, 1)

	// Do the same get again from the same client. This shouldn't hit the network.
	wantGet(c2, testActionID, testOutputID, testObjectValue)
	wantMetric(&srv.Gets, 0)

	// Cache miss. This should hit the network and fail.
	if _, _, err = c2.Get(ctx, testActionIDMiss); err != nil {
		t.Fatalf("miss Get: %v", err)
	}
	wantMetric(&srv.Gets, 1)
	wantMetric(&srv.GetHits, 0)

	// Check that access time gets updated.
	// Do it from a fresh client without a disk cache.
	wantMetric(&srv.GetAccessBumps, 0)
	advanceClock(relAtimeSeconds * 2 * time.Second) // advance clock by 2 days
	c3 := mkClient()
	wantGet(c3, testActionID, testOutputID, testObjectValue)
	wantMetric(&srv.GetAccessBumps, 1)
}
