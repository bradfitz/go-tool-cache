package main

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

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

	// Make two clients (imagine: two different builder VMs)
	c1 := mkClient()
	c2 := mkClient()

	const testActionID = "0001"
	const testActionIDMiss = "0002" // this one doesn't exist
	const testOutputID = "9900"
	const testObjectValue = "test data"

	// Populate from the first client.
	clientDiskPath, err := c1.Put(ctx, testActionID, testOutputID, int64(len(testObjectValue)), strings.NewReader(testObjectValue))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if clientDiskPath == "" {
		t.Fatal("Put returned empty disk path")
	}
	wrote, err := os.ReadFile(clientDiskPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(wrote) != testObjectValue {
		t.Errorf("ReadFile got %q, want %q", wrote, testObjectValue)
	}

	// Read from the second client.
	gotOutputID, diskPath, err := c2.Get(ctx, testActionID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if gotOutputID != testOutputID {
		t.Errorf("Get got outputID %q, want %q", gotOutputID, testOutputID)
	}
	if diskPath == "" {
		t.Fatal("Get returned empty disk path")
	}
	wrote, err = os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(wrote) != testObjectValue {
		t.Errorf("ReadFile got %q, want %q", wrote, testObjectValue)
	}
	// Check metrics
	if got, want := srv.gets.Value(), int64(1); got != want {
		t.Errorf("server metric gets = %d, want %d", got, want)
	}
	if got, want := srv.getHits.Value(), int64(1); got != want {
		t.Errorf("server metric getHits = %d, want %d", got, want)
	}

	// Do the same get again from the same client. This shouldn't hit the network.
	if _, _, err = c2.Get(ctx, testActionID); err != nil {
		t.Fatalf("Get: %v", err)
	} else if srv.gets.Value() != 1 {
		t.Errorf("server metric gets = %d, want 1", srv.gets.Value())
	}

	// Cache miss. This should hit the network and fail.
	if _, _, err = c2.Get(ctx, testActionIDMiss); err != nil {
		t.Fatalf("miss Get: %v", err)
	} else if srv.gets.Value() != 2 {
		t.Errorf("server metric gets = %d, want 1", srv.gets.Value())
	}
}
