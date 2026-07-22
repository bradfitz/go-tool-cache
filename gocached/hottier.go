// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gocached

import (
	"container/list"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bradfitz/go-tool-cache/gocached/logger"
)

// hotIndex is an in-memory LRU index of the blob files resident in the hot
// tier directory. The hot tier holds a bounded copy of recently used blobs;
// the main blob directory is the source of truth, so hot files can be
// deleted at any time without data loss.
//
// It starts empty and is populated by an asynchronous startup scan
// ([hotIndex.scan]), then maintained incrementally as blobs are written,
// promoted, accessed, and evicted.
type hotIndex struct {
	usage atomic.Int64 // sum of sizes of all indexed files, in bytes

	// ready is set once the startup scan has completed. Until then the index
	// doesn't know the hot dir's usage, so nothing new may be written into
	// the hot tier (PUT tees and promotions are skipped, and eviction is a
	// no-op); reads are still served from hot files that already exist.
	ready atomic.Bool

	// scanStart is the unix-nano time the startup scan began, or 0 when the
	// scan isn't running. It feeds the gocached_hot_scan_running_seconds
	// gauge.
	scanStart atomic.Int64

	mu       sync.Mutex
	ll       *list.List               // front is most recently used; values are *hotEntry
	ents     map[string]*list.Element // base filename ("<hex>" or "<hex>.lz4") -> element in ll
	inflight map[string]struct{}      // base filenames with promotions in progress
}

// hotEntry describes one file in the hot tier.
type hotEntry struct {
	name string // base filename: "<hex>" or "<hex>.lz4"
	size int64  // file size in bytes
}

func newHotIndex() *hotIndex {
	return &hotIndex{
		ll:       list.New(),
		ents:     make(map[string]*list.Element),
		inflight: make(map[string]struct{}),
	}
}

// usageBytes reports the total size of all indexed hot files.
func (h *hotIndex) usageBytes() int64 { return h.usage.Load() }

// len reports the number of indexed hot files.
func (h *hotIndex) len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.ents)
}

// touch marks name as most recently used, if present.
func (h *hotIndex) touch(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.ents[name]; ok {
		h.ll.MoveToFront(e)
	}
}

// add records that name (of the given size) is now resident in the hot tier,
// marking it most recently used. Adding an existing name updates its size.
func (h *hotIndex) add(name string, size int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e, ok := h.ents[name]; ok {
		ent := e.Value.(*hotEntry)
		h.usage.Add(size - ent.size)
		ent.size = size
		h.ll.MoveToFront(e)
		return
	}
	h.ents[name] = h.ll.PushFront(&hotEntry{name: name, size: size})
	h.usage.Add(size)
}

// remove deletes name from the index, reporting its size and whether it was
// present. It does not remove the file on disk; that's the caller's job.
func (h *hotIndex) remove(name string) (size int64, ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.ents[name]
	if !ok {
		return 0, false
	}
	ent := e.Value.(*hotEntry)
	h.ll.Remove(e)
	delete(h.ents, name)
	h.usage.Add(-ent.size)
	return ent.size, true
}

// evictLRU removes the least recently used entries from the index until usage
// is at or below target, returning the removed entries. The caller is
// responsible for deleting the corresponding files on disk.
func (h *hotIndex) evictLRU(target int64) []hotEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	var evicted []hotEntry
	for h.usage.Load() > target {
		e := h.ll.Back()
		if e == nil {
			break
		}
		ent := e.Value.(*hotEntry)
		h.ll.Remove(e)
		delete(h.ents, ent.name)
		h.usage.Add(-ent.size)
		evicted = append(evicted, *ent)
	}
	return evicted
}

// tryStartPromotion reports whether the caller should promote name into the
// hot tier, registering the promotion as in flight if so. It returns false if
// the startup scan hasn't completed, name is already resident, or a promotion
// for it is already in flight. Every true return must be paired with a later
// [hotIndex.endPromotion] call.
func (h *hotIndex) tryStartPromotion(name string) bool {
	if !h.ready.Load() {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.ents[name]; ok {
		return false
	}
	if _, ok := h.inflight[name]; ok {
		return false
	}
	h.inflight[name] = struct{}{}
	return true
}

// endPromotion unregisters an in-flight promotion started by
// [hotIndex.tryStartPromotion].
func (h *hotIndex) endPromotion(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.inflight, name)
}

// staleHotTempAge is how old an "upload-*" temp file in the hot dir must be
// before the startup scan deletes it as an abandoned leftover from a crash.
const staleHotTempAge = time.Hour

// failsafeWriter wraps a writer whose failures should not abort the stream
// feeding it. The first underlying write error is recorded and all subsequent
// writes are discarded, but Write always reports success so that sibling
// writers in an [io.MultiWriter] keep receiving bytes.
type failsafeWriter struct {
	w   io.Writer
	err error // first underlying write error, if any
}

func (fw *failsafeWriter) Write(p []byte) (int, error) {
	if fw.err == nil {
		if _, err := fw.w.Write(p); err != nil {
			fw.err = err
		}
	}
	return len(p), nil
}

// scan walks dir and populates h with its blob files, ordered by file
// modification time so the oldest files are evicted first, then marks h
// ready. Stale "upload-*" temp files left behind by a crash are deleted;
// fresh ones are skipped, as they may belong to a concurrent writer.
//
// It runs asynchronously at server startup; until it completes, nothing new
// is written into the hot tier (see [hotIndex.ready]), so the directory is
// stable underneath the walk with one exception: the main cache's size/age
// eviction keeps running during the scan (only hot tier LRU eviction waits
// for it), and evicting a blob from the cache also deletes its hot copy via
// [Server.removeFromHot]. A deleted file the walk already recorded becomes a
// phantom index entry; that's harmless, as evicting it later is a no-op
// os.Remove.
func (h *hotIndex) scan(ctx context.Context, dir string, logf logger.Logf) error {
	type scanned struct {
		name  string
		size  int64
		mtime time.Time
	}
	var files []scanned
	start := time.Now()
	h.scanStart.Store(start.UnixNano())
	defer h.scanStart.Store(0)
	logf("hot tier: scanning %v ...", dir)
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			// Skip dot-directories such as the put-queue spool dir,
			// whose files are not hot tier entries.
			if strings.HasPrefix(d.Name(), ".") && path != dir {
				return fs.SkipDir
			}
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if strings.HasPrefix(d.Name(), "upload-") {
			if time.Since(fi.ModTime()) > staleHotTempAge {
				if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
					logf("hot tier: removing stale temp %v: %v", path, err)
				}
			}
			return nil
		}
		files = append(files, scanned{name: d.Name(), size: fi.Size(), mtime: fi.ModTime()})
		return nil
	})
	if err != nil {
		return err
	}
	slices.SortFunc(files, func(a, b scanned) int {
		return a.mtime.Compare(b.mtime)
	})
	for _, f := range files {
		h.add(f.name, f.size)
	}
	h.ready.Store(true)
	logf("hot tier: indexed %d files, %s in %v", len(files), bytesFmt(h.usageBytes()), time.Since(start).Round(time.Millisecond))
	return nil
}
