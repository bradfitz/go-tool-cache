package gocached

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pierrec/lz4/v4"
)

func newTestPutQueue(t *testing.T) *putQueue {
	t.Helper()
	return newPutQueue(&Server{clock: time.Now}, t.TempDir())
}

func TestPutQueueInsertDup(t *testing.T) {
	q := newTestPutQueue(t)

	reserved, err := q.reserve(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	p1 := &pendingPut{key: actionKey{ActionID: "aa11"}, sha256hex: "s1", reservedBytes: reserved}
	if dup := q.insert(p1); dup {
		t.Fatal("first insert reported dup")
	}
	p2 := &pendingPut{key: actionKey{ActionID: "aa11"}, sha256hex: "s2"}
	if dup := q.insert(p2); !dup {
		t.Fatal("second insert of same key not reported as dup")
	}

	// First PUT wins: the original entry is still the one served.
	got, ok := q.lookup(p1.key)
	if !ok || got.sha256hex != "s1" {
		t.Fatalf("lookup = %+v, %v; want the first entry", got, ok)
	}

	// Same ActionID in a different namespace is not a dup.
	p3 := &pendingPut{key: actionKey{NamespaceID: 7, ActionID: "aa11"}}
	if dup := q.insert(p3); dup {
		t.Fatal("insert in different namespace reported dup")
	}

	q.retire(p1)
	if _, ok := q.lookup(p1.key); ok {
		t.Fatal("entry still pending after retire")
	}
}

func TestPutQueueReserveBackpressure(t *testing.T) {
	q := newTestPutQueue(t)
	ctx := context.Background()

	// Fill the byte budget entirely.
	r1, err := q.reserve(ctx, putQueuePendingBytesCap)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != putQueuePendingBytesCap {
		t.Fatalf("reserved = %d, want %d", r1, putQueuePendingBytesCap)
	}

	// A blocked reservation aborts when its context is canceled (e.g. the
	// HTTP client goes away).
	cctx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	go func() {
		_, err := q.reserve(cctx, 1)
		errc <- err
	}()
	select {
	case err := <-errc:
		t.Fatalf("reserve unexpectedly returned %v while at capacity", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked reserve = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked reserve didn't abort on context cancel")
	}

	// Freed capacity admits new reservations.
	q.unreserve(r1)
	r2, err := q.reserve(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	q.unreserve(r2)

	// A single blob bigger than the whole budget is clamped and admitted.
	big, err := q.reserve(ctx, putQueuePendingBytesCap*3)
	if err != nil {
		t.Fatal(err)
	}
	if big != putQueuePendingBytesCap {
		t.Fatalf("oversized reservation = %d, want clamp to %d", big, putQueuePendingBytesCap)
	}
	q.unreserve(big)
}

func TestPutQueueReserveCountCap(t *testing.T) {
	q := newTestPutQueue(t)
	ctx := context.Background()

	for range putQueuePendingCountCap {
		if _, err := q.reserve(ctx, 0); err != nil {
			t.Fatal(err)
		}
	}

	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := q.reserve(cctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("reserve over count cap = %v, want context.Canceled", err)
	}

	q.unreserve(0)
	if _, err := q.reserve(ctx, 0); err != nil {
		t.Fatalf("reserve after freeing a slot: %v", err)
	}
}

func TestPutQueueSpoolBlob(t *testing.T) {
	q := newTestPutQueue(t)

	// Small content stays uncompressed on disk.
	small := []byte("hello put queue")
	diskSize, path, err := q.spoolBlob(int64(len(small)), bytes.NewReader(small))
	if err != nil {
		t.Fatal(err)
	}
	if diskSize != int64(len(small)) {
		t.Errorf("small diskSize = %d, want %d", diskSize, len(small))
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, small) {
		t.Errorf("small spool file = %q, %v; want %q", got, err, small)
	}
	if dir := filepath.Dir(path); dir != q.dir {
		t.Errorf("spool file in %q, want %q", dir, q.dir)
	}
	if !strings.HasPrefix(filepath.Base(path), "put-") {
		t.Errorf("spool file name %q lacks put- prefix", filepath.Base(path))
	}

	// Content at or above the lz4 threshold is compressed; decompressing
	// the spool file yields the original bytes.
	big := bytes.Repeat([]byte("gocached"), 1000)
	diskSize, path, err = q.spoolBlob(int64(len(big)), bytes.NewReader(big))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if fi, err := f.Stat(); err != nil || fi.Size() != diskSize {
		t.Errorf("big spool file size = %v, %v; want %d", fi.Size(), err, diskSize)
	}
	got, err := io.ReadAll(lz4.NewReader(f))
	if err != nil || !bytes.Equal(got, big) {
		t.Errorf("big spool file decompressed to %d bytes, %v; want %d bytes", len(got), err, len(big))
	}

	// A short body (fewer bytes than the declared size) fails and cleans up
	// its spool file.
	_, badPath, err := q.spoolBlob(100, strings.NewReader("short"))
	if err == nil {
		t.Fatal("spoolBlob with short body unexpectedly succeeded")
	}
	if badPath != "" {
		t.Errorf("failed spoolBlob returned path %q, want empty", badPath)
	}
	ents, err := os.ReadDir(q.dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(ents), 2; got != want {
		t.Errorf("spool dir has %d files, want %d (failed spool not cleaned up?)", got, want)
	}
}

func TestPutQueueStartupCleanup(t *testing.T) {
	t.Run("hot", func(t *testing.T) {
		hotDir := t.TempDir()
		leftover := filepath.Join(hotDir, putQueueDirName, "put-1-old")
		if err := os.MkdirAll(filepath.Dir(leftover), 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(leftover, []byte("orphan"), 0644); err != nil {
			t.Fatal(err)
		}

		st := newServerTester(t, WithHotDir(hotDir), WithHotCapacity(1<<20))

		if _, err := os.Stat(leftover); !os.IsNotExist(err) {
			t.Errorf("leftover spool file still exists (err=%v)", err)
		}
		if got, want := st.srv.putq.dir, filepath.Join(hotDir, putQueueDirName); got != want {
			t.Errorf("queue dir = %q, want %q", got, want)
		}
		if fi, err := os.Stat(st.srv.putq.dir); err != nil || !fi.IsDir() {
			t.Errorf("queue dir missing after start: %v", err)
		}
	})

	t.Run("no-hot", func(t *testing.T) {
		st := newServerTester(t)
		if got, want := st.srv.putq.dir, filepath.Join(st.srv.dir, putQueueDirName); got != want {
			t.Errorf("queue dir = %q, want %q", got, want)
		}
		if fi, err := os.Stat(st.srv.putq.dir); err != nil || !fi.IsDir() {
			t.Errorf("queue dir missing after start: %v", err)
		}
	})
}
