// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/bradfitz/go-tool-cache/cachers"
)

const (
	actionID = "aaaaaaaa"
	outputID = "bbbbbbbb"
	content  = "hello"
)

func TestCacheHits(t *testing.T) {
	srvDir := t.TempDir()
	srvDiskCache := &cachers.DiskCache{Dir: srvDir}

	srv := httptest.NewServer(&server{
		cache: srvDiskCache,
	})
	t.Cleanup(srv.Close)

	clientDir := t.TempDir()
	hc := &cachers.HTTPClient{
		BaseURL: srv.URL,
		Disk:    &cachers.DiskCache{Dir: clientDir},
		Verbose: *verbose,
	}

	// Initially, expect cache miss.
	expectCacheHit(t, hc, false)

	// Now populate the server cache, expect cache hit over HTTP.
	srvPath, err := srvDiskCache.Put(context.Background(), actionID, outputID, int64(len(content)), strings.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if srvPath == "" {
		t.Fatal("got empty path from Put")
	}

	expectCacheHit(t, hc, true)

	// Delete it from the server's cache, still expect cache hit due to local disk cache.
	if err := os.Remove(srvPath); err != nil {
		t.Fatal(err)
	}
	expectCacheHit(t, hc, true)
}

func expectCacheHit(t *testing.T, hc *cachers.HTTPClient, hit bool) {
	t.Helper()

	gotOutputID, clientPath, err := hc.Get(context.Background(), actionID)
	if err != nil {
		t.Fatal(err)
	}

	if hit {
		if gotOutputID != outputID {
			t.Fatalf("got outputID %q; want %q", gotOutputID, outputID)
		}
		if gotContent, err := os.ReadFile(clientPath); err != nil || string(gotContent) != content {
			t.Fatalf("read %q; content: %q, err: %v", clientPath, gotContent, err)
		}
	} else {
		if gotOutputID != "" || clientPath != "" {
			t.Fatalf("got %q, %q; want empty (cache miss)", gotOutputID, clientPath)
		}
	}
}
