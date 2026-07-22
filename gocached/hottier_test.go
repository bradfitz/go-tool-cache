// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gocached

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestHotIndex(t *testing.T) {
	h := newHotIndex()
	h.add("aa1", 10)
	h.add("bb2", 20)
	h.add("cc3", 30)
	if got, want := h.usageBytes(), int64(60); got != want {
		t.Errorf("usage = %d, want %d", got, want)
	}
	if got, want := h.len(), 3; got != want {
		t.Errorf("len = %d, want %d", got, want)
	}

	// Re-adding updates size without duplicating.
	h.add("bb2", 25)
	if got, want := h.usageBytes(), int64(65); got != want {
		t.Errorf("usage after re-add = %d, want %d", got, want)
	}
	if got, want := h.len(), 3; got != want {
		t.Errorf("len after re-add = %d, want %d", got, want)
	}

	// Recency order is now (most to least recent): bb2, cc3, aa1. Touching
	// aa1 makes cc3 the least recently used entry.
	h.touch("aa1")
	evicted := h.evictLRU(40)
	var names []string
	for _, e := range evicted {
		names = append(names, e.name)
	}
	if want := []string{"cc3"}; !slices.Equal(names, want) {
		t.Errorf("evicted %v, want %v", names, want)
	}
	if got, want := h.usageBytes(), int64(35); got != want {
		t.Errorf("usage after evict = %d, want %d", got, want)
	}

	if size, ok := h.remove("bb2"); !ok || size != 25 {
		t.Errorf("remove(bb2) = %d, %v; want 25, true", size, ok)
	}
	if _, ok := h.remove("bb2"); ok {
		t.Error("second remove(bb2) reported present")
	}
	if got, want := h.usageBytes(), int64(10); got != want {
		t.Errorf("usage after remove = %d, want %d", got, want)
	}

	// Promotions: rejected until the index is ready, then rejected for
	// in-flight and resident names.
	if h.tryStartPromotion("dd4") {
		t.Error("tryStartPromotion(dd4) = true before ready, want false")
	}
	h.ready.Store(true)
	if !h.tryStartPromotion("dd4") {
		t.Error("tryStartPromotion(dd4) = false, want true")
	}
	if h.tryStartPromotion("dd4") {
		t.Error("second tryStartPromotion(dd4) = true, want false")
	}
	h.endPromotion("dd4")
	if h.tryStartPromotion("aa1") {
		t.Error("tryStartPromotion(aa1) = true for resident entry, want false")
	}
}

func TestSQLiteDir(t *testing.T) {
	sqlDir := t.TempDir()
	st := newServerTester(t, WithSQLiteDir(sqlDir))
	c := st.mkClient()
	st.wantPut(c, "0001", "9901", "test data")
	st.wantGet(c, "0001", "9901", "test data")

	dbName := fmt.Sprintf("gocached-v%d.db", schemaVersion)
	if _, err := os.Stat(filepath.Join(sqlDir, dbName)); err != nil {
		t.Errorf("DB not in sqlite dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(st.srv.dir, dbName)); !os.IsNotExist(err) {
		t.Errorf("DB unexpectedly present in cache dir (err=%v)", err)
	}
}

// hotFiles returns the base names of all non-temp files in the hot dir,
// sorted. Dot-directories (like the put-queue spool dir) are not part of
// the hot tier and are skipped.
func (st *tester) hotFiles() []string {
	st.t.Helper()
	var ret []string
	err := filepath.Walk(st.srv.hotDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if fi.IsDir() && strings.HasPrefix(fi.Name(), ".") && path != st.srv.hotDir {
			return filepath.SkipDir
		}
		if !fi.Mode().IsRegular() || strings.HasPrefix(fi.Name(), "upload-") {
			return nil
		}
		ret = append(ret, fi.Name())
		return nil
	})
	if err != nil {
		st.t.Fatalf("Walk: %v", err)
	}
	slices.Sort(ret)
	return ret
}

// waitFor polls f until it returns true or the timeout expires.
//
// It can't use testing/synctest because these tests exercise the real HTTP
// path via httptest over loopback, and goroutines blocked in network
// syscalls are never durably blocked inside a synctest bubble.
func waitFor(t testing.TB, what string, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestTieringWriteThrough(t *testing.T) {
	st := newServerTester(t, WithHotDir(t.TempDir()), WithHotCapacity(1<<20))
	c := st.mkClient()

	uncompressed := strings.Repeat("x", smallObjectSize+1) // stored raw
	compressed := strings.Repeat("y", smallObjectSize+2)   // stored lz4
	st.wantPut(c, "0001", "9901", uncompressed)
	st.wantPut(c, "0002", "9902", compressed)
	st.wantPut(c, "0003", "9903", "small") // inline; no disk file in either tier

	cold := st.diskFiles()
	hot := st.hotFiles()
	if !slices.Equal(cold, hot) {
		t.Errorf("cold files %v != hot files %v", cold, hot)
	}
	if len(hot) != 2 {
		t.Fatalf("got %d files, want 2", len(hot))
	}

	var wantUsage int64
	for _, name := range hot {
		coldBytes, err := os.ReadFile(filepath.Join(st.srv.dir, name[:2], name))
		if err != nil {
			t.Fatal(err)
		}
		hotBytes, err := os.ReadFile(st.srv.hotFilepath(name))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(coldBytes, hotBytes) {
			t.Errorf("tier contents differ for %v", name)
		}
		wantUsage += int64(len(hotBytes))
	}
	if got := st.srv.hot.usageBytes(); got != wantUsage {
		t.Errorf("hot usage = %d, want %d", got, wantUsage)
	}
	if got := st.srv.hot.len(); got != 2 {
		t.Errorf("hot len = %d, want 2", got)
	}
}

func TestTieringPromotion(t *testing.T) {
	st := newServerTester(t, WithHotDir(t.TempDir()), WithHotCapacity(1<<20))
	c := st.mkClient()

	val := strings.Repeat("x", smallObjectSize+1)
	st.wantPut(c, "0001", "9901", val)

	names := st.hotFiles()
	if len(names) != 1 {
		t.Fatalf("got %d hot files, want 1", len(names))
	}
	name := names[0]

	// Simulate hot tier loss (e.g. ephemeral disk wiped) without telling the
	// index: the next GET must recover by dropping the stale entry and
	// promoting the blob back from the cold tier.
	if err := os.Remove(st.srv.hotFilepath(name)); err != nil {
		t.Fatal(err)
	}

	// GET is served from the cold tier and triggers an async promotion.
	st.wantGet(st.mkClient(), "0001", "9901", val)
	st.wantMetric(&st.srv.m.HotMisses, 1)
	st.wantMetric(&st.srv.m.HotHits, 0)

	waitFor(t, "promotion", func() bool {
		return st.srv.m.HotPromotions.Value() == 1 && st.srv.hot.len() == 1
	})
	hotBytes, err := os.ReadFile(st.srv.hotFilepath(name))
	if err != nil {
		t.Fatalf("promoted file: %v", err)
	}
	coldBytes, err := os.ReadFile(filepath.Join(st.srv.dir, name[:2], name))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hotBytes, coldBytes) {
		t.Error("promoted file differs from cold copy")
	}

	// The next GET is served from the hot tier.
	st.wantGet(st.mkClient(), "0001", "9901", val)
	st.wantMetric(&st.srv.m.HotHits, 1)
	st.wantMetric(&st.srv.m.HotMisses, 0)
}

func TestTieringHotEviction(t *testing.T) {
	st := newServerTester(t, WithHotDir(t.TempDir()), WithHotCapacity(1<<20))
	c := st.mkClient()

	// All blobs are exactly 1025 bytes: big enough to hit disk, small enough
	// to stay below lz4CompressThreshold so stored size == len.
	vals := []string{
		strings.Repeat("a", smallObjectSize+1),
		strings.Repeat("b", smallObjectSize+1),
		strings.Repeat("c", smallObjectSize+1),
		strings.Repeat("d", smallObjectSize+1),
	}
	blobSize := int64(smallObjectSize + 1)
	st.wantPut(c, "0001", "9901", vals[0])
	st.wantPut(c, "0002", "9902", vals[1])
	st.wantPut(c, "0003", "9903", vals[2])
	st.wantPut(c, "0004", "9904", vals[3])

	// Touch the first (oldest) blob so it becomes most recently used.
	st.wantGet(st.mkClient(), "0001", "9901", vals[0])
	st.wantMetric(&st.srv.m.HotHits, 1)

	// Lower the capacity so only the touched blob fits under the 95% target.
	st.srv.hotCap = 2 * blobSize
	st.srv.evictHotIfOver()

	if got, want := st.srv.hot.len(), 1; got != want {
		t.Fatalf("hot len after evict = %d, want %d", got, want)
	}
	if got, want := st.srv.hot.usageBytes(), blobSize; got != want {
		t.Errorf("hot usage after evict = %d, want %d", got, want)
	}
	st.wantMetric(&st.srv.m.HotEvicted, 3)

	// The survivor is the touched blob, still readable from the hot tier.
	st.wantGet(st.mkClient(), "0001", "9901", vals[0])
	st.wantMetric(&st.srv.m.HotHits, 1)

	// Cold tier still has all four blobs; evicted ones are served from cold.
	if got := len(st.diskFiles()); got != 4 {
		t.Errorf("cold files = %d, want 4", got)
	}
	st.wantGet(st.mkClient(), "0004", "9904", vals[3])
	st.wantMetric(&st.srv.m.HotMisses, 1)
}

func TestTieringMainEvictionRemovesBothTiers(t *testing.T) {
	st := newServerTester(t, WithHotDir(t.TempDir()), WithHotCapacity(1<<20))
	st.srv.maxAge = 24 * time.Hour
	c := st.mkClient()

	st.wantPut(c, "0001", "9901", strings.Repeat("x", smallObjectSize+1))
	st.advanceClock(25 * time.Hour)
	st.wantPut(c, "0002", "9902", strings.Repeat("y", smallObjectSize+1))

	if got := st.srv.hot.len(); got != 2 {
		t.Fatalf("hot len = %d, want 2", got)
	}

	clean := st.cleanOldObjects()
	if clean.Count != 1 {
		t.Fatalf("cleaned %v, want Count 1", clean)
	}

	cold := st.diskFiles()
	hot := st.hotFiles()
	if len(cold) != 1 || !slices.Equal(cold, hot) {
		t.Errorf("after eviction cold = %v, hot = %v; want the same single file", cold, hot)
	}
	if got, want := st.srv.hot.usageBytes(), int64(smallObjectSize+1); got != want {
		t.Errorf("hot usage = %d, want %d", got, want)
	}
}

// TestTieringNotReady exercises the window before the async startup scan has
// completed: reads are served from existing hot files, but nothing new is
// written into the hot tier.
func TestTieringNotReady(t *testing.T) {
	st := newServerTester(t, WithHotDir(t.TempDir()), WithHotCapacity(1<<20))
	c := st.mkClient()

	val := strings.Repeat("a", smallObjectSize+1)
	st.wantPut(c, "0001", "9901", val)
	names := st.hotFiles()
	if len(names) != 1 {
		t.Fatalf("got %d hot files, want 1", len(names))
	}

	// Rewind to the not-ready state, as if the scan were still running.
	st.srv.hot.ready.Store(false)

	// Existing hot files are still served.
	st.wantGet(st.mkClient(), "0001", "9901", val)
	st.wantMetric(&st.srv.m.HotHits, 1)

	// New PUTs skip the hot tier.
	val2 := strings.Repeat("b", smallObjectSize+1)
	st.wantPut(c, "0002", "9902", val2)
	if got := st.hotFiles(); len(got) != 1 {
		t.Errorf("hot files after not-ready PUT = %v, want just the original", got)
	}

	// GETs served from cold don't start promotions. tryStartPromotion
	// rejects synchronously when not ready, so no goroutine to wait on.
	st.wantGet(st.mkClient(), "0002", "9902", val2)
	st.wantMetric(&st.srv.m.HotMisses, 1)
	st.wantMetric(&st.srv.m.HotPromotions, 0)
	if got := st.hotFiles(); len(got) != 1 {
		t.Errorf("hot files after not-ready GET = %v, want just the original", got)
	}

	// Once ready again, the same GET promotes.
	st.srv.hot.ready.Store(true)
	st.wantGet(st.mkClient(), "0002", "9902", val2)
	waitFor(t, "promotion", func() bool { return st.srv.m.HotPromotions.Value() == 1 })
	if got := st.hotFiles(); len(got) != 2 {
		t.Errorf("hot files after ready GET = %v, want 2", got)
	}
}

func TestHotStartupScan(t *testing.T) {
	hotDir := t.TempDir()

	writeHotFile := func(name string, size int, age time.Duration) {
		t.Helper()
		dir := filepath.Join(hotDir, name[:2])
		if err := os.MkdirAll(dir, 0750); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, size), 0644); err != nil {
			t.Fatal(err)
		}
		mtime := time.Now().Add(-age)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	writeHotFile("aa11", 10, 3*time.Hour) // oldest
	writeHotFile("bb22", 20, 2*time.Hour)
	writeHotFile("cc33", 30, 1*time.Hour) // newest

	// A stale temp file is deleted by the scan; a fresh one is left alone.
	// writeDiskBlob and promoteToHot create their temp files at the hot dir
	// root, so that's where the scan looks for leftovers.
	writeTemp := func(name string, age time.Duration) {
		t.Helper()
		path := filepath.Join(hotDir, name)
		if err := os.WriteFile(path, []byte("tmp"), 0644); err != nil {
			t.Fatal(err)
		}
		mtime := time.Now().Add(-age)
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
	}
	writeTemp("upload-1-stale", 2*time.Hour)
	writeTemp("upload-2-fresh", 0)

	// Files under dot-directories (such as the put-queue spool dir) are not
	// hot tier entries and must not be indexed; the usage and len checks
	// below would fail if this file were counted.
	if err := os.MkdirAll(filepath.Join(hotDir, ".put-queue"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hotDir, ".put-queue", "put-1-spool"), []byte("spooled"), 0644); err != nil {
		t.Fatal(err)
	}

	st := newServerTester(t, WithHotDir(hotDir), WithHotCapacity(1<<20))

	if got, want := st.srv.hot.usageBytes(), int64(60); got != want {
		t.Errorf("usage = %d, want %d", got, want)
	}
	if got, want := st.srv.hot.len(), 3; got != want {
		t.Errorf("len = %d, want %d", got, want)
	}
	if _, err := os.Stat(filepath.Join(hotDir, "upload-1-stale")); !os.IsNotExist(err) {
		t.Errorf("stale temp not removed (err=%v)", err)
	}
	if _, err := os.Stat(filepath.Join(hotDir, "upload-2-fresh")); err != nil {
		t.Errorf("fresh temp unexpectedly removed: %v", err)
	}

	// Eviction removes the files with the oldest mtimes first: aa11 and bb22
	// go, bringing usage from 60 to 30, at the 95% target of the 32-byte cap.
	st.srv.hotCap = 32
	st.srv.evictHotIfOver()
	if got, want := st.hotFiles(), []string{"cc33"}; !slices.Equal(got, want) {
		t.Errorf("hot files after evict = %v, want %v", got, want)
	}
}
