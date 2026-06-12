// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gocached

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradfitz/go-tool-cache/cachers"
	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/go-cmp/cmp"
	"github.com/pierrec/lz4/v4"
)

// sha256OfEmpty is the SHA-256 hash of an empty string, used as a well-known
// value in SQLite to store bytes, as it's common.
const sha256OfEmpty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// Package-level ECDSA P-256 keys reused across tests that need OIDC signing
// keys. Generating these is the single most expensive step in the JWT tests,
// so we share them rather than regenerating per test.
var (
	testKey1 = mustGenerateTestKey()
	testKey2 = mustGenerateTestKey()
	testKey3 = mustGenerateTestKey()
)

func mustGenerateTestKey() *ecdsa.PrivateKey {
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(fmt.Sprintf("generating test ECDSA key: %v", err))
	}
	return k
}

type tester struct {
	t   testing.TB
	srv *Server
	hs  *httptest.Server

	timeMu  sync.Mutex
	curTime time.Time
}

func (t *tester) Logf(format string, args ...any) {
	t.t.Logf(format, args...)
}

func (t *tester) now() time.Time {
	t.timeMu.Lock()
	defer t.timeMu.Unlock()
	return t.curTime
}

func (t *tester) advanceClock(d time.Duration) {
	t.timeMu.Lock()
	defer t.timeMu.Unlock()
	t.curTime = t.curTime.Add(d)
}

func (t *tester) mkClient() *cachers.HTTPClient {
	clientCacheDir := t.t.TempDir()
	return &cachers.HTTPClient{
		BaseURL: t.hs.URL,
		Disk: &cachers.DiskCache{
			Dir: clientCacheDir,
			Logf: func(format string, args ...any) {
				t.Logf("client-disk: "+format, args...)
			},
		},
	}
}

func (st *tester) usageStats() *usageStats {
	st.t.Helper()
	// Stats are maintained by an async per-shard stats loop in production. Tests
	// need a deterministic aggregate, so flush pending access-time bumps and
	// rotate every shard before reading.
	st.srv.flushAccessTimeBumps()
	if err := st.srv.scanAllShards(context.Background()); err != nil {
		st.t.Fatalf("scanAllShards: %v", err)
	}
	stats, err := st.srv.usageStats()
	if err != nil {
		st.t.Fatalf("usageStats: %v", err)
	}
	return stats
}

// cleanOldObjects runs cleanup until it's a no-op, so tests get a fully
// drained eviction in one call. Updates the aggregate (via a forced shard
// rescan) between batches so the cleanupTick's "are we over budget" check
// sees the post-eviction state, not stale pre-eviction shard data.
func (st *tester) cleanOldObjects() countAndSize {
	st.t.Helper()
	var total countAndSize
	ctx := context.Background()
	for {
		_ = st.usageStats()
		res, err := st.srv.cleanupTick(ctx)
		if err != nil {
			st.t.Fatalf("cleanupTick: %v", err)
		}
		if res.Count == 0 {
			return total
		}
		total.Count += res.Count
		total.Size += res.Size
	}
}

func (st *tester) diskFiles() []string {
	st.t.Helper()
	var ret []string

	err := filepath.Walk(st.srv.dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() || strings.HasPrefix(fi.Name(), ".") || strings.HasPrefix(fi.Name(), "gocached") {
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

// wantMetric is a helper to check an expvar.Int metric and reset it
// for future tests.
func (st *tester) wantMetric(m *expvar.Int, want int64) {
	st.t.Helper()
	if got := m.Value(); got != want {
		st.t.Errorf("metric = %d, want %d", got, want)
	}
	m.Set(0)
}

func (st *tester) wantPut(c *cachers.HTTPClient, actionID, outputID string, val string) {
	ctx := context.Background()
	st.t.Helper()
	clientDiskPath, err := c.Put(ctx, actionID, outputID, int64(len(val)), strings.NewReader(val))
	if err != nil {
		st.t.Fatalf("Put: %v", err)
	}
	if clientDiskPath == "" {
		st.t.Fatal("Put returned empty disk path")
	}
	st.wantMetric(&st.srv.m.Puts, 1)
	wrote, err := os.ReadFile(clientDiskPath)
	if err != nil {
		st.t.Fatalf("ReadFile: %v", err)
	}
	if string(wrote) != val {
		st.t.Errorf("ReadFile got %q, want %q", wrote, val)
	}
}

func (st *tester) wantGet(c *cachers.HTTPClient, actionID, outputID, wantVal string) {
	ctx := context.Background()
	st.t.Helper()
	gotOutputID, diskPath, err := c.Get(ctx, actionID)
	if err != nil {
		st.t.Fatalf("Get: %v", err)
	}
	if gotOutputID != outputID {
		st.t.Errorf("Get got outputID %q, want %q", gotOutputID, outputID)
	}
	if diskPath == "" {
		st.t.Fatal("Get returned empty disk path")
	}
	wrote, err := os.ReadFile(diskPath)
	if err != nil {
		st.t.Fatalf("ReadFile: %v", err)
	}
	if string(wrote) != wantVal {
		st.t.Errorf("ReadFile got %q, want %q", wrote, wantVal)
	}
}

func (st *tester) wantGetMiss(c *cachers.HTTPClient, actionID string) {
	ctx := context.Background()
	st.t.Helper()
	gotOutputID, diskPath, err := c.Get(ctx, actionID)
	if err != nil {
		st.t.Fatalf("Get: %v", err)
	}
	if gotOutputID != "" {
		st.t.Errorf("Get got outputID %q; want empty", gotOutputID)
	}
	if diskPath != "" {
		st.t.Fatalf("Get returned disk path %q; want empty", diskPath)
	}
}

// lz4Size returns the lz4-compressed size of data.
func lz4Size(t testing.TB, data []byte) int64 {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if err := w.Apply(lz4.SizeOption(uint64(len(data)))); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return int64(buf.Len())
}

func withClock(clk func() time.Time) ServerOption {
	return func(cfg *Server) {
		cfg.clock = clk
	}
}

// withoutBackgroundLoops disables every periodic goroutine that start would
// otherwise launch (stats loop, cleanup, checkpoint, DB-size metrics).
// Tests need this because the mocked clock makes scannedAt collide across
// concurrent scans, so a background scan can overwrite a freshly-PUT
// shard with a stale-snapshot empty result. Tests drive scanShard /
// cleanupTick / etc. synchronously instead.
func withoutBackgroundLoops() ServerOption {
	return func(cfg *Server) {
		cfg.disableBackgroundLoops = true
	}
}

func newServerTester(t testing.TB, extraOpts ...ServerOption) *tester {
	st := &tester{
		t:       t,
		curTime: time.Unix(1234, 0),
	}

	opts := []ServerOption{
		WithDir(t.TempDir()),
		WithLogf(t.Logf),
		WithVerbose(true),
		withClock(st.now),
		withoutBackgroundLoops(),
		// Default to 16 shards instead of production's 256 to keep
		// scanAllShards fast in tests (each shard is a separate SQL
		// round-trip; on near-empty tables overhead dominates). Tests
		// that need a specific shardPrefixLen pass WithShardPrefixLen
		// in extraOpts to override.
		WithShardPrefixLen(1),
	}
	srv, err := NewServer(append(opts, extraOpts...)...)
	if err != nil {
		t.Fatalf("starting gocached: %v", err)
	}
	st.srv = srv

	st.hs = httptest.NewServer(st.srv)
	t.Cleanup(func() { st.srv.Close() })
	t.Cleanup(st.hs.Close)

	return st
}

type jwtFunc func(claims jwt.MapClaims, signingKey *ecdsa.PrivateKey) string

// startOIDCServer starts a mock OIDC server that gocached can use for JWT auth.
// The provided publicKey is what JWT signatures will be validated against.
func startOIDCServer(t *testing.T, publicKey crypto.PublicKey) (iss string, jwtFunc jwtFunc) {
	t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	issuer := fmt.Sprintf("http://%s", srv.Listener.Addr().String())

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"issuer":   issuer,
			"jwks_uri": fmt.Sprintf("%s/jwks", issuer),
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"keys": []jose.JSONWebKey{
				{
					Key:       publicKey,
					KeyID:     "test-key",
					Algorithm: "ES256",
					Use:       "sig",
				},
			},
		})
	})

	return issuer, func(claims jwt.MapClaims, signingKey *ecdsa.PrivateKey) string {
		t.Helper()
		unsignedTk := &jwt.Token{
			Header: map[string]any{
				"typ": "JWT",
				"alg": jwt.SigningMethodES256.Alg(),
				"kid": "test-key",
			},
			Claims: claims,
			Method: jwt.SigningMethodES256,
		}
		tk, err := unsignedTk.SignedString(signingKey)
		if err != nil {
			t.Fatalf("error signing token: %v", err)
		}

		return tk
	}
}

func TestServer(t *testing.T) {
	st := newServerTester(t)

	ctx := context.Background()

	// Make two clients (imagine: two different builder VMs)
	c1 := st.mkClient()
	c2 := st.mkClient()

	const testActionID = "0001"
	const testActionIDMiss = "0002" // this one doesn't exist
	const testActionIDBig = "0bbb"  // non-inline object
	const testActionIDEmpty = "0000"
	const testOutputID = "9900"
	const testOutputIDBig = "9bbb"
	const testOutputIDEmpty = sha256OfEmpty
	const testObjectValue = "test data"
	testObjectValueBig := strings.Repeat("x", smallObjectSize+1)

	// Populate from the first client.
	st.wantPut(c1, testActionID, testOutputID, testObjectValue)
	st.wantPut(c1, testActionIDBig, testOutputIDBig, testObjectValueBig)
	st.wantPut(c1, testActionIDEmpty, testOutputIDEmpty, "")

	// Read from the second client.
	st.wantGet(c2, testActionID, testOutputID, testObjectValue)
	st.wantGet(c2, testActionIDBig, testOutputIDBig, testObjectValueBig)
	st.wantGet(c2, testActionIDEmpty, testOutputIDEmpty, "")

	// Check metrics
	st.wantMetric(&st.srv.m.Gets, 3)
	st.wantMetric(&st.srv.m.GetHits, 3)
	st.wantMetric(&st.srv.m.GetHitsInline, 1)

	// Do the same get again from the same client. This shouldn't hit the network.
	st.wantGet(c2, testActionID, testOutputID, testObjectValue)
	st.wantMetric(&st.srv.m.Gets, 0)

	// Cache miss. This should hit the network and fail.
	if _, _, err := c2.Get(ctx, testActionIDMiss); err != nil {
		t.Fatalf("miss Get: %v", err)
	}
	st.wantMetric(&st.srv.m.Gets, 1)
	st.wantMetric(&st.srv.m.GetHits, 0)

	// Check that access time gets updated.
	// Do it from a fresh client without a disk cache.
	st.wantMetric(&st.srv.m.GetAccessBumps, 0)
	st.advanceClock(relAtimeSeconds * 2 * time.Second) // advance clock by 2 days
	c3 := st.mkClient()
	st.wantGet(c3, testActionID, testOutputID, testObjectValue)
	st.wantMetric(&st.srv.m.GetAccessBumps, 1)

	// Get usage stats. The tester helper flushes pending access-time bumps
	// and rotates every shard so the aggregate is deterministic.
	stats := st.usageStats()
	bigStored := int64(len(testObjectValueBig)) // below lz4CompressThreshold, stored uncompressed
	totalSize := int64(9) + bigStored           // 9 (inline "test data") + big + 0 (empty)
	want := &usageStats{
		MissingBlobRows: 0,
		ActionsLE: map[time.Duration]countAndSize{
			24 * time.Hour:   {Count: 1, Size: 9},
			48 * time.Hour:   {Count: 1, Size: 9},
			96 * time.Hour:   {Count: 3, Size: totalSize},
			168 * time.Hour:  {Count: 3, Size: totalSize},
			336 * time.Hour:  {Count: 3, Size: totalSize},
			720 * time.Hour:  {Count: 3, Size: totalSize},
			2160 * time.Hour: {Count: 3, Size: totalSize},
			math.MaxInt64:    {Count: 3, Size: totalSize},
		},
	}
	if diff := cmp.Diff(stats, want); diff != "" {
		t.Errorf("usageStats mismatch (-got +want):\n%s", diff)
	}

	st.advanceClock(relAtimeSeconds * 2 * time.Second) // advance clock by 2 days
}

// TestEvictionQueryPlan is the lockdown test for the eviction query: it
// runs EXPLAIN QUERY PLAN and asserts SQLite uses idx_actions_access and
// never does a SCAN of the Actions table. INDEXED BY in the query already
// forces this at runtime, but the test catches schema or query drift early
// (and gives a concrete failure message instead of a runtime SQL error).
func TestEvictionQueryPlan(t *testing.T) {
	st := newServerTester(t)
	row := st.srv.db.QueryRow("EXPLAIN QUERY PLAN "+evictionCandidateQuery, 0, 1)
	var id, parent, notused int
	var detail string
	if err := row.Scan(&id, &parent, &notused, &detail); err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	t.Logf("plan: %s", detail)
	if !strings.Contains(detail, "idx_actions_access") {
		t.Errorf("plan does not use idx_actions_access: %q", detail)
	}
	if strings.Contains(detail, "SCAN Actions") || strings.Contains(detail, "SCAN TABLE Actions") {
		t.Errorf("plan does a table scan of Actions: %q", detail)
	}
}

func TestEvictOldestActions(t *testing.T) {
	st := newServerTester(t)
	ctx := context.Background()

	// Four unique blobs (different content => different SHA256 => different
	// BlobID), inserted 24h apart so AccessTime order matches insert order.
	c := st.mkClient()
	st.wantPut(c, "0001", "9901", "1")
	st.advanceClock(24 * time.Hour)
	st.wantPut(c, "0002", "9902", "22")
	st.advanceClock(24 * time.Hour)
	st.wantPut(c, "0003", "9903", "333")
	st.advanceClock(24 * time.Hour)
	st.wantPut(c, "0004", "9904", "4444")

	// All four should still be on disk.
	if got, want := len(st.diskFiles()), 0; got != want {
		// All four are <= smallObjectSize, so they're inline in the DB,
		// not on disk. Confirms the test isn't accidentally on the disk path.
		t.Errorf("diskFiles before evict: %d, want %d (all should be inline)", got, want)
	}

	// Cutoff lets only the two oldest (1 and 22) through. limit=10 is generous.
	res, err := st.srv.evictOldestActions(ctx, st.srv.now().Add(-25*time.Hour).Unix(), 10, math.MaxInt64)
	if err != nil {
		t.Fatalf("evictOldestActions: %v", err)
	}
	// 1:1 blobs => evicting Actions evicts Blobs too.
	if want := (countAndSize{Count: 2, Size: 3}); res != want {
		t.Errorf("evict result = %+v, want %+v", res, want)
	}

	// Now no cutoff (math.MaxInt64) but limit=1: just the next-oldest ("333").
	res, err = st.srv.evictOldestActions(ctx, math.MaxInt64, 1, math.MaxInt64)
	if err != nil {
		t.Fatalf("evictOldestActions: %v", err)
	}
	if want := (countAndSize{Count: 1, Size: 3}); res != want {
		t.Errorf("evict result = %+v, want %+v", res, want)
	}
}

// TestActionLRUSharedBlob locks in the Action-LRU semantic on shared blobs:
// when two Actions point at the same Blob, evicting the old Action leaves
// the Blob alive (because the fresh Action still references it).
func TestActionLRUSharedBlob(t *testing.T) {
	st := newServerTester(t)
	ctx := context.Background()

	c := st.mkClient()
	const content = "shared-blob-content"
	// Two distinct ActionIDs but identical content => identical SHA256 =>
	// the same BlobID. Action 0001 is inserted "two days ago", 0002 today.
	st.wantPut(c, "0001", "9901", content)
	st.advanceClock(48 * time.Hour)
	st.wantPut(c, "0002", "9902", content)

	// Evict everything older than 25h. Action-LRU: the old 0001 Action
	// must go, but the Blob stays because 0002 still references it.
	res, err := st.srv.evictOldestActions(ctx, st.srv.now().Add(-25*time.Hour).Unix(), 10, math.MaxInt64)
	if err != nil {
		t.Fatalf("evictOldestActions: %v", err)
	}
	if got, want := res.Count, int64(0); got != want {
		t.Errorf("evicted Blob count = %d, want %d (Blob still has a fresher Action)", got, want)
	}
	var actionCount int
	if err := st.srv.db.QueryRow(`SELECT COUNT(*) FROM Actions`).Scan(&actionCount); err != nil {
		t.Fatal(err)
	}
	if actionCount != 1 {
		t.Errorf("Actions count after evict = %d, want 1 (0001 evicted, 0002 alive)", actionCount)
	}
	var blobCount int
	if err := st.srv.db.QueryRow(`SELECT COUNT(*) FROM Blobs`).Scan(&blobCount); err != nil {
		t.Fatal(err)
	}
	if blobCount != 1 {
		t.Errorf("Blobs count after evict = %d, want 1 (Blob shared with 0002, stays)", blobCount)
	}
}

func TestShardDeltaPUT(t *testing.T) {
	st := newServerTester(t)

	// Before any PUT: delta is zero.
	if c, b := st.srv.sumShardDeltas(); c != 0 || b != 0 {
		t.Errorf("delta before PUT = (%d, %d), want (0, 0)", c, b)
	}

	// Two PUTs of unique content. Each is small enough to be stored inline
	// so StoredSize == len(content).
	c := st.mkClient()
	st.wantPut(c, "0001", "9901", "hello")
	st.wantPut(c, "0002", "9902", "world!!")

	if got, want := func() int64 { c, _ := st.srv.sumShardDeltas(); return c }(), int64(2); got != want {
		t.Errorf("delta count after 2 PUTs = %d, want %d", got, want)
	}
	if got, want := func() int64 { _, b := st.srv.sumShardDeltas(); return b }(), int64(5+7); got != want {
		t.Errorf("delta bytes after 2 PUTs = %d, want %d", got, want)
	}

	// Duplicate PUT (same actionID/outputID/content) goes through INSERT OR
	// IGNORE: affected=0, so the delta must NOT advance.
	st.wantPut(c, "0001", "9901", "hello")
	if got, _ := st.srv.sumShardDeltas(); got != 2 {
		t.Errorf("delta count after duplicate PUT = %d, want 2 (dup must not double-count)", got)
	}
}

func TestShardDeltaZeroedAfterScan(t *testing.T) {
	// The whole point of the preDelta dance in scanAndPersistShard is that
	// once a shard is rescanned, anything that was reflected in the new
	// persisted snapshot leaves the delta. Verify it: PUT, scanAllShards,
	// delta == 0.
	st := newServerTester(t)
	c := st.mkClient()
	st.wantPut(c, "0001", "9901", "hello")
	st.wantPut(c, "0002", "9902", "world!!")

	if cnt, b := st.srv.sumShardDeltas(); cnt == 0 && b == 0 {
		t.Fatalf("delta is already zero before scan; test isn't exercising the post-scan zeroing")
	}
	if err := st.srv.scanAllShards(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cnt, b := st.srv.sumShardDeltas(); cnt != 0 || b != 0 {
		t.Errorf("delta after scanAllShards = (%d, %d), want (0, 0)", cnt, b)
	}
}

func TestShardDeltaDrivesSizeCleanup(t *testing.T) {
	// Dead-reckoning's reason for existing: cleanup must react to PUTs that
	// happen between scans. Scenario: scan first so the persisted
	// aggregate is at zero, then PUT enough to go over the limit *without*
	// a scan, then assert cleanupTick fires.
	st := newServerTester(t)
	st.srv.maxSize = 8
	ctx := context.Background()

	// Empty cache: aggregate scanned, delta zero, cleanupTick no-op.
	if err := st.srv.scanAllShards(ctx); err != nil {
		t.Fatal(err)
	}
	if res, err := st.srv.cleanupTick(ctx); err != nil || res.Count != 0 {
		t.Fatalf("cleanupTick on empty: %+v, err=%v; want zero", res, err)
	}

	// PUT 10 bytes total. NO scanAllShards — the persisted aggregate still
	// says zero. cleanupTick must consult the delta and decide to evict.
	c := st.mkClient()
	st.wantPut(c, "0001", "9901", "1")
	st.advanceClock(time.Second)
	st.wantPut(c, "0002", "9902", "22")
	st.advanceClock(time.Second)
	st.wantPut(c, "0003", "9903", "333")
	st.advanceClock(time.Second)
	st.wantPut(c, "0004", "9904", "4444")
	st.advanceClock(time.Second)

	if us := st.srv.lastUsage.Load(); us.All().Size != 0 {
		t.Fatalf("persisted aggregate is non-zero (%v); the test premise (no scan yet) is broken", us.All())
	}
	if _, b := st.srv.sumShardDeltas(); b != 10 {
		t.Fatalf("delta bytes = %d, want 10 (10 = 1+2+3+4)", b)
	}
	res, err := st.srv.cleanupTick(ctx)
	if err != nil {
		t.Fatalf("cleanupTick: %v", err)
	}
	// maxSize=8, delta-only total=10, so we need at least 2 bytes freed.
	// The oldest are "1" (1B) and "22" (2B), summing to 3 ≥ 2 — stop after
	// evicting both. Same as TestCleanOldObjectsBySize.
	if want := (countAndSize{Count: 2, Size: 3}); res != want {
		t.Errorf("cleanupTick result = %+v, want %+v", res, want)
	}
}

func TestCleanOldObjectsByAge(t *testing.T) {
	st := newServerTester(t)
	st.srv.maxAge = 24 * time.Hour

	// Populate some data.
	c1 := st.mkClient()
	st.wantPut(c1, "0001", "9901", strings.Repeat("x", smallObjectSize+1))
	st.advanceClock(25 * time.Hour)
	st.wantPut(c1, "0002", "9902", strings.Repeat("x", smallObjectSize+2))
	st.wantPut(c1, "0003", "9903", "small")
	smallLen := int64(len("small"))

	stored1 := int64(smallObjectSize + 1)                                 // below lz4CompressThreshold, stored uncompressed
	stored2 := lz4Size(t, []byte(strings.Repeat("x", smallObjectSize+2))) // at lz4CompressThreshold, lz4-compressed

	st1 := st.usageStats()
	if all, want := st1.All(), (countAndSize{Count: 3, Size: stored1 + stored2 + smallLen}); all != want {
		t.Errorf("usageStats: %v; want %v", all, want)
	}
	// First file is uncompressed (no .lz4 suffix), second is lz4-compressed.
	if got, want := st.diskFiles(), []string{"333092a3daf718ed8f38a94e302df139edd4e3b5da4239a497995683942cf28c.lz4", "c6d8e9905300876046729949cc95c2385221270d389176f7234fe7ac00c4e430"}; !slices.Equal(got, want) {
		t.Errorf("diskFiles: %v; want %v", got, want)
	}

	clean1 := st.cleanOldObjects()
	if clean1.Count != 1 || clean1.Size != stored1 {
		t.Errorf("cleanOldObjects got %v, want {Count: 1, Size: %d}", clean1, stored1)
	}
	clean2 := st.cleanOldObjects()
	if clean2.Count != 0 || clean2.Size != 0 {
		t.Errorf("cleanOldObjects got %v, want {Count: 0, Size: 0}", clean2)
	}

	st2 := st.usageStats()
	if all, want := st2.All(), (countAndSize{Count: 2, Size: stored2 + smallLen}); all != want {
		t.Errorf("usageStats after clean: %v; want %v", all, want)
	}
	if got, want := st.diskFiles(), []string{"333092a3daf718ed8f38a94e302df139edd4e3b5da4239a497995683942cf28c.lz4"}; !slices.Equal(got, want) {
		t.Errorf("diskFiles after clean: %v; want %v", got, want)
	}
}

func TestCleanOldObjectsBySize(t *testing.T) {
	st := newServerTester(t)

	// Populate some data.
	c1 := st.mkClient()
	st.wantPut(c1, "0001", "9901", "1")
	st.advanceClock(time.Second)
	st.wantPut(c1, "0002", "9902", "22")
	st.advanceClock(time.Second)
	st.wantPut(c1, "0003", "9903", "333")
	st.advanceClock(time.Second)
	st.wantPut(c1, "0004", "9904", "4444")
	st.advanceClock(time.Second)

	st1 := st.usageStats()
	if all, want := st1.All(), (countAndSize{Count: 4, Size: 10}); all != want {
		t.Errorf("usageStats: %v; want %v", all, want)
	}

	clean1 := st.cleanOldObjects()
	if clean1.Count != 0 || clean1.Size != 0 {
		t.Errorf("cleanOldObjects got %v, want no clean", clean1)
	}

	st.srv.maxSize = 8 // the only way get to 8 or under is by deleting "1" and "22" (3 bytes)

	if got, want := st.cleanOldObjects(), (countAndSize{Count: 2, Size: 3}); got != want {
		t.Errorf("cleanOldObjects got %v, want %v", got, want)
	}
	if got, want := st.usageStats().All(), (countAndSize{Count: 2, Size: 7}); got != want {
		t.Errorf("usageStats: %v; want %v", got, want)
	}
}

func TestLZ4Storage(t *testing.T) {
	st := newServerTester(t)
	c := st.mkClient()

	type testCase struct {
		name     string
		size     int
		wantLZ4  bool // expect .lz4 file on disk
		wantDisk bool // expect any disk file (false = inline in DB)
	}
	tests := []testCase{
		{"empty", 0, false, false},
		{"tiny_1b", 1, false, false},
		{"inline_max", smallObjectSize, false, false},
		{"disk_no_lz4", smallObjectSize + 1, false, true},           // on disk but below lz4CompressThreshold
		{"disk_at_lz4_threshold", lz4CompressThreshold, true, true}, // smallest lz4-compressed disk blob
		{"disk_2k", 2048, true, true},
		{"disk_64k", 64 << 10, true, true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionID := fmt.Sprintf("%04x", i+0x10)
			outputID := fmt.Sprintf("%04x", i+0x90)

			data := make([]byte, tt.size)
			if len(data) > 0 {
				data[0] = 'X'
				data[len(data)-1] = 'X'
			}
			val := string(data)

			// PUT
			st.wantPut(c, actionID, outputID, val)

			// GET via client (sends Accept-Encoding: lz4) — verify round-trip.
			c2 := st.mkClient() // fresh client, no disk cache
			st.wantGet(c2, actionID, outputID, val)

			// Raw HTTP GET with Accept-Encoding: lz4
			req, _ := http.NewRequest("GET", st.hs.URL+"/action/"+actionID, nil)
			req.Header.Set("Want-Object", "1")
			req.Header.Set("Accept-Encoding", "lz4")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("raw GET: %v", err)
			}
			body, _ := io.ReadAll(res.Body)
			res.Body.Close()

			if tt.wantLZ4 {
				if got := res.Header.Get("Content-Encoding"); got != "lz4" {
					t.Errorf("Accept lz4: Content-Encoding = %q, want %q", got, "lz4")
				}
				if got := res.Header.Get("X-Uncompressed-Length"); got != fmt.Sprint(tt.size) {
					t.Errorf("Accept lz4: X-Uncompressed-Length = %q, want %q", got, fmt.Sprint(tt.size))
				}
				if got := res.Header.Get("Content-Length"); got == fmt.Sprint(tt.size) {
					t.Errorf("Accept lz4: Content-Length = %q, should be compressed (smaller)", got)
				}
			} else {
				if got := res.Header.Get("Content-Encoding"); got != "" {
					t.Errorf("Accept lz4: Content-Encoding = %q, want empty", got)
				}
				if got := res.Header.Get("X-Uncompressed-Length"); got != "" {
					t.Errorf("Accept lz4: X-Uncompressed-Length = %q, want empty", got)
				}
				// Uncompressed: body should be the raw data.
				if string(body) != val {
					t.Errorf("Accept lz4: body length = %d, want %d", len(body), len(val))
				}
			}

			// Raw HTTP GET without Accept-Encoding: lz4 — server must decompress.
			req2, _ := http.NewRequest("GET", st.hs.URL+"/action/"+actionID, nil)
			req2.Header.Set("Want-Object", "1")
			// Deliberately no Accept-Encoding.
			res2, err := http.DefaultClient.Do(req2)
			if err != nil {
				t.Fatalf("raw GET (no lz4): %v", err)
			}
			body2, _ := io.ReadAll(res2.Body)
			res2.Body.Close()

			if got := res2.Header.Get("Content-Encoding"); got != "" {
				t.Errorf("No Accept lz4: Content-Encoding = %q, want empty", got)
			}
			if got := res2.Header.Get("Content-Length"); got != fmt.Sprint(tt.size) {
				t.Errorf("No Accept lz4: Content-Length = %q, want %q", got, fmt.Sprint(tt.size))
			}
			if string(body2) != val {
				t.Errorf("No Accept lz4: body length = %d, want %d", len(body2), len(val))
			}
		})
	}

	// Verify disk state: expect exactly one plain file (the disk_no_lz4 case)
	// and the rest with .lz4 suffix.
	var plain, compressed int
	for _, f := range st.diskFiles() {
		if strings.HasSuffix(f, ".lz4") {
			compressed++
		} else {
			plain++
		}
	}
	if plain != 1 {
		t.Errorf("disk files: got %d plain (non-lz4), want 1", plain)
	}
	if compressed != 3 {
		t.Errorf("disk files: got %d .lz4, want 3", compressed)
	}
}

func TestShardPrefix(t *testing.T) {
	// Verify the %0*x zero-padding contract: the width comes from the *
	// argument and pads with leading zeros to that width. A typo (%0x,
	// %x, %2x) would silently break shard keys.
	cases := []struct {
		prefixLen int
		idx       int
		want      shardPrefix
	}{
		{1, 0, "0"},
		{1, 15, "f"},
		{2, 0, "00"},
		{2, 1, "01"},
		{2, 15, "0f"},
		{2, 16, "10"},
		{2, 255, "ff"},
		{3, 0, "000"},
		{3, 16, "010"},
		{3, 4095, "fff"},
		{4, 0, "0000"},
		{4, 65535, "ffff"},
	}
	for _, c := range cases {
		srv := &Server{shardPrefixLen: c.prefixLen}
		if got := srv.shardPrefix(c.idx); got != c.want {
			t.Errorf("shardPrefix(prefixLen=%d, idx=%d) = %q, want %q",
				c.prefixLen, c.idx, got, c.want)
		}
	}
}

func TestShardRange(t *testing.T) {
	// shardRange's last-shard sentinel ("g" chars) is the easiest place for a
	// future refactor to silently drop the final shard from the stats loop.
	cases := []struct {
		prefixLen      int
		idx            int
		wantLo, wantHi string
	}{
		// First shard: lo = all zeros prefix, hi = next prefix.
		{1, 0, "0", "1"},
		{2, 0, "00", "01"},
		// Middle shard.
		{2, 15, "0f", "10"},
		{2, 16, "10", "11"},
		// Last shard: hi is the "g"-sentinel that sorts after all hex.
		{1, 15, "f", "g"},
		{2, 255, "ff", "gg"},
		{3, 4095, "fff", "ggg"},
		{4, 65535, "ffff", "gggg"},
	}
	for _, c := range cases {
		srv := &Server{shardPrefixLen: c.prefixLen}
		gotLo, gotHi := srv.shardRange(c.idx)
		if gotLo != c.wantLo || gotHi != c.wantHi {
			t.Errorf("shardRange(prefixLen=%d, idx=%d) = (%q, %q), want (%q, %q)",
				c.prefixLen, c.idx, gotLo, gotHi, c.wantLo, c.wantHi)
		}
	}
}

func TestLoadShardStats(t *testing.T) {
	// Exercise the three branches that decide whether a persisted row gets
	// kept or dropped: prefix-length mismatch, cohort mismatch, and a clean
	// match. A dropped row stays in the table on disk but doesn't contribute
	// to the in-memory aggregate.
	// Override the tester default (shardPrefixLen=1) because this test
	// asserts the wrong-prefix-length and wrong-cohort branches using
	// 2-character prefixes.
	st := newServerTester(t, WithShardPrefixLen(2))
	ctx := context.Background()

	if got := st.srv.numShards(); got != 256 {
		t.Fatalf("numShards = %d, want 256", got)
	}

	// One clean shard scan so we have a row in BlobShardStats whose schema
	// matches the current server.
	if err := st.srv.scanAndPersistShard(ctx, 0); err != nil {
		t.Fatalf("scanAndPersistShard: %v", err)
	}

	// Hand-write two more rows that should be rejected on the next load.
	mustExec := func(query string, args ...any) {
		t.Helper()
		if _, err := st.srv.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("exec %q: %v", query, err)
		}
	}
	mustExec(`INSERT INTO BlobShardStats (Prefix, ScannedAt, StatsJSON) VALUES (?, ?, ?)`,
		"abc", 1, `{"ActionsLE":{}}`) // wrong prefix length
	mustExec(`INSERT INTO BlobShardStats (Prefix, ScannedAt, StatsJSON) VALUES (?, ?, ?)`,
		"01", 1, `{"ActionsLE":{"60000000000":{"Count":1,"Size":1}}}`) // wrong cohort set

	if err := st.srv.loadShardStats(ctx); err != nil {
		t.Fatalf("loadShardStats: %v", err)
	}
	if got, want := len(st.srv.shardStats), 1; got != want {
		t.Errorf("shardStats has %d entries, want %d (only the clean prefix=00 row should survive)", got, want)
	}
	if _, ok := st.srv.shardStats["00"]; !ok {
		t.Errorf("shardStats missing prefix=00")
	}
}

func TestShardScanDurationPercentiles(t *testing.T) {
	srv := &Server{
		shardStats: map[shardPrefix]*shardSnapshot{
			"00": {stats: &usageStats{QueryDuration: 10 * time.Millisecond}},
			"01": {stats: &usageStats{QueryDuration: 20 * time.Millisecond}},
			"02": {stats: &usageStats{QueryDuration: 30 * time.Millisecond}},
			"03": {stats: &usageStats{QueryDuration: 40 * time.Millisecond}},
			// Zero-duration shard is excluded so freshly-loaded-but-not-yet-
			// observed entries don't skew the low percentile to zero.
			"04": {stats: &usageStats{QueryDuration: 0}},
		},
	}
	p25, p50, p90, n := srv.shardScanDurationPercentiles()
	if n != 4 {
		t.Errorf("n = %d, want 4 (zero-duration shard should be excluded)", n)
	}
	// With sorted = [10,20,30,40] ms and rank-index picks, p25 -> idx 1,
	// p50 -> idx 2, p90 -> idx 3.
	if p25 != 20*time.Millisecond {
		t.Errorf("p25 = %v, want 20ms", p25)
	}
	if p50 != 30*time.Millisecond {
		t.Errorf("p50 = %v, want 30ms", p50)
	}
	if p90 != 40*time.Millisecond {
		t.Errorf("p90 = %v, want 40ms", p90)
	}

	empty := &Server{shardStats: map[shardPrefix]*shardSnapshot{}}
	if p25, p50, p90, n := empty.shardScanDurationPercentiles(); p25 != 0 || p50 != 0 || p90 != 0 || n != 0 {
		t.Errorf("empty server: got (%v, %v, %v, n=%d), want all zero", p25, p50, p90, n)
	}
}

func TestShardFreshness(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	at := func(secsAgo int64) time.Time { return now.Add(-time.Duration(secsAgo) * time.Second) }

	srv := &Server{
		shardStats: map[shardPrefix]*shardSnapshot{
			"00": {stats: &usageStats{}, scannedAt: at(10)},      // <= 1m
			"01": {stats: &usageStats{}, scannedAt: at(200)},     // <= 5m
			"02": {stats: &usageStats{}, scannedAt: at(800)},     // <= 15m
			"03": {stats: &usageStats{}, scannedAt: at(3000)},    // <= 1h
			"04": {stats: &usageStats{}, scannedAt: at(80_000)},  // <= 24h
			"05": {stats: &usageStats{}, scannedAt: at(200_000)}, // older than 24h
		},
	}
	counts, oldest, total := srv.shardFreshness(now)
	if total != 6 {
		t.Errorf("total = %d, want 6", total)
	}
	if got, want := oldest, at(200_000); !got.Equal(want) {
		t.Errorf("oldest = %v, want %v", got, want)
	}
	want := map[time.Duration]int{
		1 * time.Minute:  1, // just the 10s-old shard
		5 * time.Minute:  2, // adds the 200s-old
		15 * time.Minute: 3, // adds the 800s-old
		1 * time.Hour:    4, // adds the 3000s-old
		6 * time.Hour:    4, // unchanged (next is 80000s = ~22h)
		24 * time.Hour:   5, // adds the 22h-old; the 200000s (~2.3d) one is still excluded
	}
	for d, c := range want {
		if got := counts[d]; got != c {
			t.Errorf("counts[%v] = %d, want %d", d, got, c)
		}
	}
}

func TestShardHealthGauges(t *testing.T) {
	st := newServerTester(t)
	ctx := context.Background()

	// Before any scan: unscanned should equal numShards, oldest age 0.
	body := scrapeMetrics(t, st)
	if got, want := promGauge(t, body, "gocached_shard_stats_unscanned"), float64(st.srv.numShards()); got != want {
		t.Errorf("unscanned before scan = %v, want %v", got, want)
	}
	if got := promGauge(t, body, "gocached_shard_stats_oldest_age_seconds"); got != 0 {
		t.Errorf("oldest_age_seconds before scan = %v, want 0", got)
	}

	// Scan one shard at t=0, then advance the clock and re-scrape. The
	// oldest-age gauge should reflect the advance; unscanned should drop by 1.
	if err := st.srv.scanAndPersistShard(ctx, 0); err != nil {
		t.Fatalf("scanAndPersistShard: %v", err)
	}
	st.advanceClock(90 * time.Second)

	body = scrapeMetrics(t, st)
	if got, want := promGauge(t, body, "gocached_shard_stats_unscanned"), float64(st.srv.numShards()-1); got != want {
		t.Errorf("unscanned after 1 scan = %v, want %v", got, want)
	}
	if got, want := promGauge(t, body, "gocached_shard_stats_oldest_age_seconds"), float64(90); got != want {
		t.Errorf("oldest_age_seconds after 90s advance = %v, want %v", got, want)
	}
}

// scrapeMetrics returns the /metrics body served by the test server's
// metrics handler.
func scrapeMetrics(t *testing.T, st *tester) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	st.srv.metricsHandler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, body=%s", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

// promGauge returns the value of the named gauge in a Prometheus text
// exposition body, fatal-ing if the line isn't present. Matches lines like
// "<name> <value>" but not commented HELP/TYPE lines.
func promGauge(t *testing.T, body, name string) float64 {
	t.Helper()
	for line := range strings.Lines(body) {
		line = strings.TrimRight(line, "\n")
		if strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, " ")
		if !ok || k != name {
			continue
		}
		var f float64
		if _, err := fmt.Sscanf(v, "%g", &f); err != nil {
			t.Fatalf("parsing %q value %q: %v", name, v, err)
		}
		return f
	}
	t.Fatalf("metric %q not found in /metrics body:\n%s", name, body)
	return 0
}

func TestServeUsage(t *testing.T) {
	st := newServerTester(t)
	c := st.mkClient()
	st.wantPut(c, "0001", "9901", "hello")
	// Force a scan pass so the freshness section has data to render.
	if err := st.srv.scanAllShards(context.Background()); err != nil {
		t.Fatalf("scanAllShards: %v", err)
	}

	req := httptest.NewRequest("GET", "/usage", nil)
	rec := httptest.NewRecorder()
	st.srv.serveUsage(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	// Each new section should render at least its heading. Asserting on the
	// headings (rather than exact bytes) lets the formatting tweak without
	// breaking the test.
	for _, want := range []string{
		"Actions by access-time age",
		"Shard scan freshness",
		"Per-shard scan duration",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/usage body missing %q\nbody:\n%s", want, body)
		}
	}
}

func TestWALCheckpoint(t *testing.T) {
	st := newServerTester(t)
	ctx := context.Background()

	walPath := filepath.Join(st.srv.dir, fmt.Sprintf("gocached-v%d.db-wal", schemaVersion))

	// Generate enough write traffic for the WAL to be a few pages large.
	// The exact number isn't important; we just want it big enough that
	// "shrank to nearly empty" is a meaningful observation.
	for i := range 500 {
		if _, err := st.srv.db.ExecContext(ctx,
			`INSERT INTO Actions (NamespaceID, ActionID, BlobID, AltOutputID, CreateTime, AccessTime) VALUES (0, ?, 0, '', 0, 0)`,
			fmt.Sprintf("%032x", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL: %v", err)
	}
	t.Logf("WAL before checkpoint: %d bytes", before.Size())
	if before.Size() < 4096 {
		// 500 row inserts should leave at least one full WAL page even after
		// the periodic autocheckpoint reuses space in place. A tiny WAL here
		// means the test isn't actually exercising the truncation path.
		t.Fatalf("WAL only %d bytes before checkpoint; expected meaningful traffic", before.Size())
	}

	st.srv.updateDBSizeMetrics()
	if got := st.srv.m.SQLiteWALBytes.Value(); got != before.Size() {
		t.Errorf("sqlite_wal_bytes gauge before checkpoint: got %d, want %d", got, before.Size())
	}
	if got := st.srv.m.SQLiteDataBytes.Value(); got <= 0 {
		t.Errorf("sqlite_data_bytes gauge: got %d, want > 0", got)
	}

	busy, log, ckpt, err := st.srv.checkpointTruncate(ctx)
	if err != nil {
		t.Fatalf("checkpointTruncate: %v", err)
	}
	if busy != 0 || log != ckpt {
		t.Errorf("checkpoint not fully applied: busy=%d log=%d ckpt=%d", busy, log, ckpt)
	}

	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat WAL after checkpoint: %v", err)
	}
	t.Logf("WAL after checkpoint: %d bytes", after.Size())
	// TRUNCATE checkpoint with no concurrent readers truncates the WAL to 0.
	if after.Size() != 0 {
		t.Errorf("WAL not truncated: got %d bytes, want 0", after.Size())
	}

	st.srv.updateDBSizeMetrics()
	if got := st.srv.m.SQLiteWALBytes.Value(); got != 0 {
		t.Errorf("sqlite_wal_bytes gauge after checkpoint: got %d, want 0", got)
	}
}

func TestClientConnReuse(t *testing.T) {
	st := newServerTester(t)

	var numDials atomic.Int32
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		num := numDials.Add(1)
		t.Logf("DialContext #%d for %s %s", num, network, addr)
		var std net.Dialer
		return std.DialContext(ctx, network, addr)
	}
	t.Cleanup(func() { tr.CloseIdleConnections() })

	c1 := st.mkClient()
	c1.HTTPClient = &http.Client{Transport: tr}
	const missAction = "0001"
	st.wantGetMiss(c1, missAction)
	st.wantGetMiss(c1, missAction)
	st.wantGetMiss(c1, missAction)
	st.wantPut(c1, "0001", "9901", "1")
	st.wantGet(c1, "0001", "9901", "1")
	if got := numDials.Load(); got != 1 {
		t.Errorf("numDials = %d; want 1", got)
	}
}

func TestExchangeToken(t *testing.T) {
	privateKey := testKey1
	otherPrivateKey := testKey2

	for name, tc := range map[string]struct {
		mutateClaims   func(jwt.MapClaims)
		signingKey     *ecdsa.PrivateKey
		wantStatusCode int
	}{
		// Base case: no mutation.
		"valid": {
			wantStatusCode: http.StatusOK,
		},
		// Every other test makes one mutation from the base case that should cause failure.
		"missing_sub": {
			mutateClaims: func(cl jwt.MapClaims) {
				delete(cl, "sub")
			},
			wantStatusCode: http.StatusUnauthorized,
		},
		"invalid_sub": {
			mutateClaims: func(cl jwt.MapClaims) {
				cl["sub"] = "user456"
			},
			wantStatusCode: http.StatusUnauthorized,
		},
		"invalid_iss": {
			mutateClaims: func(cl jwt.MapClaims) {
				cl["iss"] = "invalid_issuer"
			},
			wantStatusCode: http.StatusUnauthorized,
		},
		"invalid_aud": {
			mutateClaims: func(cl jwt.MapClaims) {
				cl["aud"] = "invalid_audience"
			},
			wantStatusCode: http.StatusUnauthorized,
		},
		"not_yet_valid": {
			mutateClaims: func(cl jwt.MapClaims) {
				cl["nbf"] = jwt.NewNumericDate(time.Now().Add(10 * time.Minute))
			},
			wantStatusCode: http.StatusUnauthorized,
		},
		"expired": {
			mutateClaims: func(cl jwt.MapClaims) {
				cl["exp"] = jwt.NewNumericDate(time.Now().Add(-time.Minute))
			},
			wantStatusCode: http.StatusUnauthorized,
		},
		"invalid_signature": {
			signingKey:     otherPrivateKey,
			wantStatusCode: http.StatusUnauthorized,
		},
	} {
		t.Run(name, func(t *testing.T) {
			issuer, createJWT := startOIDCServer(t, privateKey.Public())
			st := newServerTester(t,
				WithJWTAuth(issuer),
				WithNamespaceMapping(func(claims map[string]any) (Namespace, error) {
					if claims["sub"] != "user123" {
						return "", fmt.Errorf("sub = %v, want user123", claims["sub"])
					}
					return GlobalNamespace, nil
				}),
			)

			// Generate JWT.
			tokenClaims := jwt.MapClaims{
				"sub": "user123",
				"num": 42,
				"iss": issuer,
				"aud": gocachedAudience,
				"nbf": jwt.NewNumericDate(time.Now().Add(-time.Minute)),
				"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
			}
			if tc.mutateClaims != nil {
				tc.mutateClaims(tokenClaims)
			}
			signingKey := privateKey
			if tc.signingKey != nil {
				signingKey = tc.signingKey
			}
			body, err := json.Marshal(map[string]any{
				"jwt": createJWT(tokenClaims, signingKey),
			})
			if err != nil {
				t.Fatalf("error marshaling request body: %v", err)
			}

			// Exchange JWT for access token.
			req, err := http.NewRequest("POST", st.hs.URL+"/auth/exchange-token", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("error creating request: %v", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("error making request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatusCode {
				t.Fatalf("unexpected status code: want %d, got %d", tc.wantStatusCode, resp.StatusCode)
			}
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("error reading response body: %v", err)
			}

			if tc.wantStatusCode != http.StatusOK {
				if string(body) != "unauthorized\n" {
					t.Fatalf("unexpected error body: %s", string(body))
				}

				// No access token to do further checks with; test finished.
				return
			}

			// Check returned access token.
			var d struct {
				AccessToken string `json:"access_token"`
			}
			if err := json.Unmarshal(body, &d); err != nil {
				t.Fatalf("error decoding response body: %v", err)
			}
			if d.AccessToken == "" {
				t.Fatalf("expected access_token in response, got %s", string(body))
			}

			cl := st.mkClient()
			if _, _, err := cl.Get(t.Context(), "abc123"); err == nil {
				t.Fatalf("Get without access token succeeded unexpectedly")
			}

			cl.AccessToken = d.AccessToken
			st.wantGetMiss(cl, "abc123")

			st.wantPut(cl, "abc123", "def456", "data789")
			st.wantGet(cl, "abc123", "def456", "data789")

			// Check session stats.
			reqStats, err := http.NewRequest("GET", st.hs.URL+"/session/stats", nil)
			if err != nil {
				t.Fatalf("error creating stats request: %v", err)
			}
			reqStats.Header.Set("Authorization", "Bearer "+d.AccessToken)
			respStats, err := http.DefaultClient.Do(reqStats)
			if err != nil {
				t.Fatalf("error making stats request: %v", err)
			}
			defer respStats.Body.Close()
			if respStats.StatusCode != http.StatusOK {
				t.Fatalf("unexpected stats status code: want %d, got %d", http.StatusOK, respStats.StatusCode)
			}
			bodyStats, err := io.ReadAll(respStats.Body)
			if err != nil {
				t.Fatalf("error reading stats response body: %v", err)
			}
			var stats stats
			if err := json.Unmarshal(bodyStats, &stats); err != nil {
				t.Fatalf("error decoding stats response body: %v", err)
			}
			t.Logf("stats: %v", stats)
			if stats.Gets == 0 {
				t.Errorf("expected non-zero gets in session stats")
			}
			if stats.Puts == 0 {
				t.Errorf("expected non-zero puts in session stats")
			}
		})
	}
}

func TestExchangeTokenNamespaceValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		namespace      string
		wantStatusCode int
	}{
		"simple":         {"user123", http.StatusOK},
		"structured_sub": {"repo:octo-org/octo-repo:environment:prod", http.StatusOK},
		"auth0_sub":      {"auth0|507f1f77bcf86cd799439020", http.StatusOK},
		"email":          {"alice+ci@example.com", http.StatusOK},
		"space":          {"has space", http.StatusUnauthorized},
		"non_ascii":      {"héllo", http.StatusUnauthorized},
		"control_char":   {"bad\tns", http.StatusUnauthorized},
		"html_meta":      {"<script>", http.StatusUnauthorized},
	} {
		t.Run(name, func(t *testing.T) {
			issuer, createJWT := startOIDCServer(t, testKey1.Public())
			st := newServerTester(t,
				WithJWTAuth(issuer),
				WithNamespaceMapping(func(claims map[string]any) (Namespace, error) {
					return Namespace(claims["sub"].(string)), nil
				}),
			)

			body, err := json.Marshal(map[string]any{
				"jwt": createJWT(baseClaims(issuer, tc.namespace), testKey1),
			})
			if err != nil {
				t.Fatalf("error marshaling request body: %v", err)
			}

			resp, err := http.Post(st.hs.URL+"/auth/exchange-token", "application/json", bytes.NewReader(body))
			if err != nil {
				t.Fatalf("error making request: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatusCode {
				t.Fatalf("namespace %q: got status %d, want %d", tc.namespace, resp.StatusCode, tc.wantStatusCode)
			}
		})
	}
}

func TestMultiIssuerAuth(t *testing.T) {
	keyA, keyB, keyC := testKey1, testKey2, testKey3

	issuerA, createJWTA := startOIDCServer(t, keyA.Public())
	issuerB, createJWTB := startOIDCServer(t, keyB.Public())
	issuerC, createJWTC := startOIDCServer(t, keyC.Public())

	namespaceFunc := func(claims map[string]any) (Namespace, error) {
		iss, _ := claims["iss"].(string)
		var requiredSub string
		switch iss {
		case issuerA:
			requiredSub = "userA"
		case issuerB:
			requiredSub = "userB"
		default:
			return "", fmt.Errorf("unknown issuer %q", iss)
		}
		if claims["sub"] != requiredSub {
			return "", fmt.Errorf("issuer %q: sub = %v, want %v", iss, claims["sub"], requiredSub)
		}
		if claims["ref"] == "refs/heads/main" {
			return GlobalNamespace, nil
		}
		return Namespace(requiredSub), nil
	}

	st := newServerTester(t,
		WithJWTAuth(issuerA, issuerB),
		WithNamespaceMapping(namespaceFunc),
	)

	// Issuer A: valid read-only token.
	t.Run("issuerA_read", func(t *testing.T) {
		claims := baseClaims(issuerA, "userA")
		resp, accessToken := exchangeToken(t, st.hs.URL, createJWTA(claims, keyA))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		if accessToken == "" {
			t.Fatal("expected access token")
		}

		cl := st.mkClient()
		cl.AccessToken = accessToken
		st.wantGetMiss(cl, "aabb01")
	})

	// Issuer A: valid write token.
	t.Run("issuerA_write", func(t *testing.T) {
		claims := baseClaims(issuerA, "userA")
		claims["ref"] = "refs/heads/main"
		resp, accessToken := exchangeToken(t, st.hs.URL, createJWTA(claims, keyA))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		cl := st.mkClient()
		cl.AccessToken = accessToken
		st.wantPut(cl, "aabb02", "ccdd02", "hello-from-A")
		st.wantGet(cl, "aabb02", "ccdd02", "hello-from-A")
	})

	// Issuer B: valid read-only token.
	t.Run("issuerB_read", func(t *testing.T) {
		claims := baseClaims(issuerB, "userB")
		resp, accessToken := exchangeToken(t, st.hs.URL, createJWTB(claims, keyB))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		if accessToken == "" {
			t.Fatal("expected access token")
		}
		cl := st.mkClient()
		cl.AccessToken = accessToken
		// Can read data written by issuer A.
		st.wantGet(cl, "aabb02", "ccdd02", "hello-from-A")
	})

	// Issuer B: valid write token.
	t.Run("issuerB_write", func(t *testing.T) {
		claims := baseClaims(issuerB, "userB")
		claims["ref"] = "refs/heads/main"
		resp, accessToken := exchangeToken(t, st.hs.URL, createJWTB(claims, keyB))
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("unexpected status code: want %d, got %d", http.StatusOK, resp.StatusCode)
		}
		cl := st.mkClient()
		cl.AccessToken = accessToken
		st.wantPut(cl, "aabb03", "ccdd03", "hello-from-B")
		st.wantGet(cl, "aabb03", "ccdd03", "hello-from-B")
	})

	// Issuer B: wrong required claims (sub doesn't match).
	t.Run("issuerB_wrong_sub", func(t *testing.T) {
		claims := baseClaims(issuerB, "userA") // issuer B requires sub=userB
		resp, _ := exchangeToken(t, st.hs.URL, createJWTB(claims, keyB))
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unexpected status code: want %d, got %d", http.StatusUnauthorized, resp.StatusCode)
		}
	})

	// Issuer C: not configured, should be rejected.
	t.Run("issuerC_rejected", func(t *testing.T) {
		claims := baseClaims(issuerC, "userC")
		resp, _ := exchangeToken(t, st.hs.URL, createJWTC(claims, keyC))
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unexpected status code: want %d, got %d", http.StatusUnauthorized, resp.StatusCode)
		}
	})
}

func TestNamespaces(t *testing.T) {
	privateKey := testKey1
	issuer, createJWT := startOIDCServer(t, privateKey.Public())

	// Namespace policy: ref=refs/heads/main grants global write; otherwise
	// the session writes to a namespace named after its sub claim.
	st := newServerTester(t,
		WithJWTAuth(issuer),
		WithNamespaceMapping(func(claims map[string]any) (Namespace, error) {
			if claims["ref"] == "refs/heads/main" {
				return GlobalNamespace, nil
			}
			sub, _ := claims["sub"].(string)
			if sub == "" {
				return "", fmt.Errorf("missing sub claim")
			}
			return Namespace(sub), nil
		}),
	)

	// Exchange tokens once, but mint a fresh client (with empty disk cache)
	// per operation so client-side disk hits don't short-circuit the
	// server-side namespace-routing assertions we want to make.
	mainClaims := baseClaims(issuer, "main-builder")
	mainClaims["ref"] = "refs/heads/main"
	_, aliceToken := exchangeToken(t, st.hs.URL, createJWT(baseClaims(issuer, "alice"), privateKey))
	_, bobToken := exchangeToken(t, st.hs.URL, createJWT(baseClaims(issuer, "bob"), privateKey))
	_, mainToken := exchangeToken(t, st.hs.URL, createJWT(mainClaims, privateKey))
	freshClient := func(token string) *cachers.HTTPClient {
		c := st.mkClient()
		c.AccessToken = token
		return c
	}
	alice := func() *cachers.HTTPClient { return freshClient(aliceToken) }
	bob := func() *cachers.HTTPClient { return freshClient(bobToken) }
	main := func() *cachers.HTTPClient { return freshClient(mainToken) }

	const (
		actionA = "0001"
		outA    = "9901"
		outG    = "9902"
		valA    = "alice bytes"
		valG    = "global bytes"
	)

	// alice writes to her own namespace.
	st.wantPut(alice(), actionA, outA, valA)

	// bob shares no namespace with alice and there's nothing in global yet.
	st.wantGetMiss(bob(), actionA)

	// alice can read what she just wrote from her own namespace.
	st.wantGet(alice(), actionA, outA, valA)

	// main-branch session writes to the global namespace.
	st.wantPut(main(), actionA, outG, valG)

	// bob now hits via global.
	st.wantGet(bob(), actionA, outG, valG)

	// alice prefers global over her own namespace, so the access-time bump
	// lands on the shared row.
	st.wantGet(alice(), actionA, outG, valG)
}

func BenchmarkFlushAccessTimes(b *testing.B) {
	st := newServerTester(b, WithVerbose(false))
	s := st.srv

	cl := st.mkClient()
	var actionIDs []string
	for n := range 5000 {
		aid := fmt.Sprintf("abcd%04x", n)
		st.wantPut(cl, aid, "def456", "data789")
		actionIDs = append(actionIDs, aid)
	}

	for b.Loop() {
		s.mu.Lock()
		s.accessDirty = make(map[actionKey]int64)
		for _, aid := range actionIDs {
			s.accessDirty[actionKey{ActionID: aid}] = 123
		}
		s.mu.Unlock()

		if err := s.flushAccessTimeBumpsWithErr(); err != nil {
			b.Fatalf("flushAccessTimeBumpsWithErr: %v", err)
		}
	}
}

func exchangeToken(t testing.TB, url string, jwt string) (resp *http.Response, accessToken string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jwt": jwt})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	resp, err = http.Post(url+"/auth/exchange-token", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("token exchange: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp, ""
	}
	var d struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp, d.AccessToken
}

func baseClaims(iss, sub string) jwt.MapClaims {
	return jwt.MapClaims{
		"sub": sub,
		"iss": iss,
		"aud": gocachedAudience,
		"nbf": jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		"exp": jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
}
