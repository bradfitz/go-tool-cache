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
	"database/sql"
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
	stats, err := st.srv.usageStats()
	if err != nil {
		st.t.Fatalf("usageStats: %v", err)
	}
	return stats
}

func (st *tester) cleanOldObjects() countAndSize {
	st.t.Helper()
	stats, err := st.srv.cleanOldObjects(st.usageStats())
	if err != nil {
		st.t.Fatalf("cleanOldObjects: %v", err)
	}
	return stats
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

	// Get usage stats.
	stats, err := st.srv.usageStats()
	if err != nil {
		t.Fatalf("usageStats: %v", err)
	}
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

func TestCleanCandidates(t *testing.T) {
	st := newServerTester(t)

	// Populate some data.
	c1 := st.mkClient()
	st.wantPut(c1, "0001", "9901", "1")
	st.advanceClock(24 * time.Hour)
	st.wantPut(c1, "0002", "9902", "22")
	st.advanceClock(24 * time.Hour)
	st.wantPut(c1, "0003", "9903", "333")
	st.advanceClock(24 * time.Hour)
	st.wantPut(c1, "0004", "9904", strings.Repeat("x", smallObjectSize+1))

	const day = 24 * time.Hour

	tests := []struct {
		maxAge time.Duration
		limit  int64
		want   []cleanCandidate
	}{
		{
			maxAge: 0,
			limit:  100,
			want: []cleanCandidate{
				{BlobID: 1, Age: 3 * day, StoredSize: 1},
				{BlobID: 2, Age: 2 * day, StoredSize: 2},
				{BlobID: 3, Age: 1 * day, StoredSize: 3},
				{BlobID: 4, Age: 0, StoredSize: smallObjectSize + 1}, // below lz4CompressThreshold, stored uncompressed
			},
		},
		{
			maxAge: 25 * time.Hour,
			limit:  100,
			want: []cleanCandidate{
				{BlobID: 1, Age: 3 * day, StoredSize: 1},
				{BlobID: 2, Age: 2 * day, StoredSize: 2},
			},
		},
		{
			maxAge: 0,
			limit:  2,
			want: []cleanCandidate{
				{BlobID: 1, Age: 3 * day, StoredSize: 1},
				{BlobID: 2, Age: 2 * day, StoredSize: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("maxAge=%v,limit=%d", tt.maxAge, tt.limit), func(t *testing.T) {
			candidates, err := st.srv.cleanCandidates(tt.maxAge, tt.limit)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(candidates, tt.want); diff != "" {
				t.Errorf("cleanCandidates mismatch (-got +want):\n%s", diff)

			}
		})
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
	// Generate private keys outside of the loop for speed.
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating OIDC server private key: %v", err)
	}
	otherPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating OIDC server private key: %v", err)
	}
	wantClaims := map[string]string{
		"sub": "user123",
	}
	wantGlobalClaims := map[string]string{
		"sub": "user123",
		"ref": "refs/heads/main",
	}

	for name, tc := range map[string]struct {
		mutateClaims   func(jwt.MapClaims)
		signingKey     *ecdsa.PrivateKey
		wantStatusCode int
		wantWrite      bool
	}{
		// Base case: no mutation.
		"valid_read": {
			wantStatusCode: http.StatusOK,
			wantWrite:      false,
		},
		// Additional claim needed for write scope.
		"valid_write": {
			mutateClaims: func(cl jwt.MapClaims) {
				cl["ref"] = "refs/heads/main"
			},
			wantStatusCode: http.StatusOK,
			wantWrite:      true,
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
				WithJWTAuth(issuer, wantClaims),
				WithGlobalNamespaceJWTClaims(wantGlobalClaims),
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

			if tc.wantWrite {
				st.wantPut(cl, "abc123", "def456", "data789")
				st.wantGet(cl, "abc123", "def456", "data789")
			} else {
				if _, err := cl.Put(t.Context(), "abc123", "def456", 0, nil); err == nil {
					t.Fatalf("Put without write scope succeeded unexpectedly")
				}
			}

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
			if stats.Puts == 0 && tc.wantWrite {
				t.Errorf("expected non-zero puts in session stats")
			}
		})
	}
}

// exchangeToken is a test helper that exchanges a JWT for an access token.
func (st *tester) exchangeToken(t *testing.T, createJWT jwtFunc, signingKey *ecdsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jwt": createJWT(claims, signingKey),
	})
	if err != nil {
		t.Fatalf("error marshaling request body: %v", err)
	}
	req, err := http.NewRequest("POST", st.hs.URL+"/auth/exchange-token", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("error creating request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("error making request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: want %d, got %d", http.StatusOK, resp.StatusCode)
	}
	var d struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatalf("error decoding response: %v", err)
	}
	return d.AccessToken
}

// getSessionStats fetches /session/stats for the given access token and optional group_by.
func (st *tester) getSessionStats(t *testing.T, accessToken, groupBy string) stats {
	t.Helper()
	u := st.hs.URL + "/session/stats"
	if groupBy != "" {
		u += "?group_by=" + groupBy
	}
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("error creating stats request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("error making stats request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected stats status code: want %d, got %d; body: %s", http.StatusOK, resp.StatusCode, body)
	}
	var s stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatalf("error decoding stats: %v", err)
	}
	return s
}

func TestSessionID(t *testing.T) {
	claims := map[string]any{
		"aud":        "gocached",
		"repository": "foo/bar",
		"run_id":     "123",
		"sha":        "abc",
	}

	// Deterministic: same claims, same result.
	id1 := sessionIDFromClaims(claims, defaultGroupingClaims)
	id2 := sessionIDFromClaims(claims, defaultGroupingClaims)
	if id1 != id2 {
		t.Fatalf("sessionIDFromClaims not deterministic: %s != %s", id1, id2)
	}

	// 64-char hex string.
	if len(id1) != 64 {
		t.Fatalf("expected 64-char hex, got %d chars: %s", len(id1), id1)
	}

	// Missing claims are skipped, producing a different ID.
	claimsMissing := map[string]any{
		"aud":        "gocached",
		"repository": "foo/bar",
	}
	id3 := sessionIDFromClaims(claimsMissing, defaultGroupingClaims)
	if id1 == id3 {
		t.Fatalf("different claims produced the same session ID")
	}

	// Order-independent: different order of groupBy claims, same result.
	groupBy1 := []string{"aud", "repository", "sha"}
	groupBy2 := []string{"sha", "aud", "repository"}
	id4 := sessionIDFromClaims(claims, groupBy1)
	id5 := sessionIDFromClaims(claims, groupBy2)
	if id4 != id5 {
		t.Fatalf("order of groupBy should not matter: %s != %s", id4, id5)
	}

	// Empty claims produce a valid ID.
	id6 := sessionIDFromClaims(map[string]any{}, defaultGroupingClaims)
	if len(id6) != 64 {
		t.Fatalf("expected 64-char hex for empty claims, got %d chars", len(id6))
	}
}

func TestAddStats(t *testing.T) {
	a := stats{
		LastUsed:       time.Unix(100, 0),
		Gets:           5,
		GetBytes:       100,
		GetHits:        3,
		GetAccessBumps: 1,
		GetHitsInline:  2,
		GetNanos:       500,
		GetErrs:        0,
		Puts:           4,
		PutErrs:        1,
		PutsDup:        0,
		PutsBytes:      200,
		PutsInline:     1,
		PutsNanos:      300,
	}
	b := stats{
		LastUsed:       time.Unix(200, 0),
		Gets:           10,
		GetBytes:       200,
		GetHits:        7,
		GetAccessBumps: 2,
		GetHitsInline:  5,
		GetNanos:       1000,
		GetErrs:        1,
		Puts:           6,
		PutErrs:        0,
		PutsDup:        2,
		PutsBytes:      400,
		PutsInline:     3,
		PutsNanos:      600,
	}

	got := addStats(a, b)
	want := stats{
		LastUsed:       time.Unix(200, 0), // max
		Gets:           15,
		GetBytes:       300,
		GetHits:        10,
		GetAccessBumps: 3,
		GetHitsInline:  7,
		GetNanos:       1500,
		GetErrs:        1,
		Puts:           10,
		PutErrs:        1,
		PutsDup:        2,
		PutsBytes:      600,
		PutsInline:     4,
		PutsNanos:      900,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("addStats mismatch (-want +got):\n%s", diff)
	}

	// LastUsed max: a is later.
	a.LastUsed = time.Unix(300, 0)
	got2 := addStats(a, b)
	if !got2.LastUsed.Equal(time.Unix(300, 0)) {
		t.Fatalf("LastUsed should be max; got %v", got2.LastUsed)
	}
}

func TestSessionPersistenceAcrossRestart(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating key: %v", err)
	}
	issuer, createJWT := startOIDCServer(t, privateKey.Public())
	wantClaims := map[string]string{"sub": "user123"}
	wantGlobalClaims := map[string]string{"sub": "user123", "ref": "refs/heads/main"}

	dir := t.TempDir()

	// Start server 1.
	st := &tester{t: t, curTime: time.Unix(1234, 0)}
	srv1, err := NewServer(
		WithDir(dir),
		WithLogf(t.Logf),
		WithVerbose(true),
		withClock(st.now),
		WithJWTAuth(issuer, wantClaims),
		WithGlobalNamespaceJWTClaims(wantGlobalClaims),
	)
	if err != nil {
		t.Fatalf("starting server 1: %v", err)
	}
	hs1 := httptest.NewServer(srv1)
	st.srv = srv1
	st.hs = hs1

	tokenClaims := jwt.MapClaims{
		"sub":        "user123",
		"ref":        "refs/heads/main",
		"repository": "foo/bar",
		"run_id":     "42",
		"iss":        issuer,
		"aud":        gocachedAudience,
		"nbf":        jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		"exp":        jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	accessToken1 := st.exchangeToken(t, createJWT, privateKey, tokenClaims)

	// Do some work.
	cl := st.mkClient()
	cl.AccessToken = accessToken1
	st.wantPut(cl, "abc123", "def456", "data789")
	// Use a separate client for GET so it hits the server (not local disk cache).
	cl1b := st.mkClient()
	cl1b.AccessToken = accessToken1
	st.wantGet(cl1b, "abc123", "def456", "data789")

	stats1 := st.getSessionStats(t, accessToken1, "")
	if stats1.Puts != 1 {
		t.Fatalf("expected 1 put, got %d", stats1.Puts)
	}
	if stats1.Gets != 1 {
		t.Fatalf("expected 1 get, got %d", stats1.Gets)
	}

	// Close server 1 (should flush to DB).
	hs1.Close()
	if err := srv1.Close(); err != nil {
		t.Fatalf("close server 1: %v", err)
	}

	// Start server 2 on the same directory.
	st2 := &tester{t: t, curTime: time.Unix(2345, 0)}
	srv2, err := NewServer(
		WithDir(dir),
		WithLogf(t.Logf),
		WithVerbose(true),
		withClock(st2.now),
		WithJWTAuth(issuer, wantClaims),
		WithGlobalNamespaceJWTClaims(wantGlobalClaims),
	)
	if err != nil {
		t.Fatalf("starting server 2: %v", err)
	}
	hs2 := httptest.NewServer(srv2)
	st2.srv = srv2
	st2.hs = hs2
	defer hs2.Close()
	defer srv2.Close()

	// New auth with same claims.
	accessToken2 := st2.exchangeToken(t, createJWT, privateKey, tokenClaims)

	// Do more work with a fresh client.
	cl2 := st2.mkClient()
	cl2.AccessToken = accessToken2
	st2.wantGet(cl2, "abc123", "def456", "data789")

	stats2 := st2.getSessionStats(t, accessToken2, "")
	// Should include persisted stats from server 1 + new stats from server 2.
	// Server 1: 1 put + 1 get. Server 2: 1 get.
	if stats2.Puts != 1 {
		t.Fatalf("expected accumulated 1 put, got %d", stats2.Puts)
	}
	if stats2.Gets != 2 {
		t.Fatalf("expected 2 accumulated gets, got %d", stats2.Gets)
	}
}

func TestSessionPersistenceOnTokenExpiry(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating key: %v", err)
	}
	issuer, createJWT := startOIDCServer(t, privateKey.Public())
	wantClaims := map[string]string{"sub": "user123"}
	wantGlobalClaims := map[string]string{"sub": "user123", "ref": "refs/heads/main"}

	st := newServerTester(t,
		WithJWTAuth(issuer, wantClaims),
		WithGlobalNamespaceJWTClaims(wantGlobalClaims),
	)

	tokenClaims := jwt.MapClaims{
		"sub":        "user123",
		"ref":        "refs/heads/main",
		"repository": "foo/bar",
		"run_id":     "42",
		"iss":        issuer,
		"aud":        gocachedAudience,
		"nbf":        jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		"exp":        jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	accessToken1 := st.exchangeToken(t, createJWT, privateKey, tokenClaims)

	cl := st.mkClient()
	cl.AccessToken = accessToken1
	st.wantPut(cl, "abc123", "def456", "data789")
	// Use separate client for GET so it hits the server.
	clGet := st.mkClient()
	clGet.AccessToken = accessToken1
	st.wantGet(clGet, "abc123", "def456", "data789")

	initialStats := st.getSessionStats(t, accessToken1, "")
	if initialStats.Puts != 1 {
		t.Fatalf("expected 1 put, got %d", initialStats.Puts)
	}
	if initialStats.Gets != 1 {
		t.Fatalf("expected 1 get, got %d", initialStats.Gets)
	}

	// Advance clock past token expiry so cleanup will delete the session.
	st.advanceClock(2 * time.Hour)

	// Manually trigger cleanup (simulates the runCleanSessionsLoop iteration).
	st.srv.mu.Lock()
	var expired []*sessionData
	for token, sd := range st.srv.sessions {
		if st.srv.now().After(sd.expiry) {
			expired = append(expired, sd)
			delete(st.srv.sessions, token)
			st.srv.m.Sessions.Add(-1)
		}
	}
	st.srv.mu.Unlock()
	for _, sd := range expired {
		st.srv.flushSessionStats(sd)
	}

	// Re-auth with the same claims.
	accessToken2 := st.exchangeToken(t, createJWT, privateKey, tokenClaims)

	cl2 := st.mkClient()
	cl2.AccessToken = accessToken2
	st.wantGet(cl2, "abc123", "def456", "data789")

	stats2 := st.getSessionStats(t, accessToken2, "")
	// Accumulated: 1 put + 1 get (flushed from expired) + 1 get (new session).
	if stats2.Puts != 1 {
		t.Fatalf("expected accumulated 1 put after re-auth, got %d", stats2.Puts)
	}
	if stats2.Gets != 2 {
		t.Fatalf("expected 2 accumulated gets after re-auth, got %d", stats2.Gets)
	}
}

func TestSessionStatsGrouping(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating key: %v", err)
	}
	issuer, createJWT := startOIDCServer(t, privateKey.Public())
	wantClaims := map[string]string{"sub": "user123"}
	wantGlobalClaims := map[string]string{"sub": "user123", "ref": "refs/heads/main"}

	st := newServerTester(t,
		WithJWTAuth(issuer, wantClaims),
		WithGlobalNamespaceJWTClaims(wantGlobalClaims),
	)

	// Two tokens with different check_run_id but same repository and run_id.
	baseClaims := func(checkRunID string) jwt.MapClaims {
		return jwt.MapClaims{
			"sub":          "user123",
			"ref":          "refs/heads/main",
			"repository":   "foo/bar",
			"run_id":       "42",
			"check_run_id": checkRunID,
			"iss":          issuer,
			"aud":          gocachedAudience,
			"nbf":          jwt.NewNumericDate(time.Now().Add(-time.Minute)),
			"exp":          jwt.NewNumericDate(time.Now().Add(time.Hour)),
		}
	}

	token1 := st.exchangeToken(t, createJWT, privateKey, baseClaims("cr1"))
	token2 := st.exchangeToken(t, createJWT, privateKey, baseClaims("cr2"))

	cl1 := st.mkClient()
	cl1.AccessToken = token1
	st.wantPut(cl1, "abc123", "def456", "data789")

	cl2 := st.mkClient()
	cl2.AccessToken = token2
	st.wantPut(cl2, "bbc123", "eef456", "data789")

	// Default grouping: different check_run_id -> separate sessions.
	stats1 := st.getSessionStats(t, token1, "")
	stats2 := st.getSessionStats(t, token2, "")
	if stats1.Puts != 1 {
		t.Fatalf("token1: expected 1 put, got %d", stats1.Puts)
	}
	if stats2.Puts != 1 {
		t.Fatalf("token2: expected 1 put, got %d", stats2.Puts)
	}

	// Custom group_by (repository,run_id): both tokens should merge.
	combined := st.getSessionStats(t, token1, "repository,run_id")
	if combined.Puts != 2 {
		t.Fatalf("combined grouping: expected 2 puts, got %d", combined.Puts)
	}
	// Should work from either token's perspective.
	combined2 := st.getSessionStats(t, token2, "repository,run_id")
	if combined2.Puts != 2 {
		t.Fatalf("combined grouping from token2: expected 2 puts, got %d", combined2.Puts)
	}
}

func TestSessionsDebugPage(t *testing.T) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("error generating key: %v", err)
	}
	issuer, createJWT := startOIDCServer(t, privateKey.Public())
	wantClaims := map[string]string{"sub": "user123"}
	wantGlobalClaims := map[string]string{"sub": "user123", "ref": "refs/heads/main"}

	st := newServerTester(t,
		WithJWTAuth(issuer, wantClaims),
		WithGlobalNamespaceJWTClaims(wantGlobalClaims),
	)

	debugHS := httptest.NewServer(http.HandlerFunc(st.srv.ServeHTTPDebug))
	defer debugHS.Close()

	// Create two sessions with different claims.
	claims1 := jwt.MapClaims{
		"sub":        "user123",
		"ref":        "refs/heads/main",
		"repository": "foo/bar",
		"run_id":     "42",
		"iss":        issuer,
		"aud":        gocachedAudience,
		"nbf":        jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		"exp":        jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	claims2 := jwt.MapClaims{
		"sub":        "user123",
		"ref":        "refs/heads/main",
		"repository": "baz/qux",
		"run_id":     "99",
		"iss":        issuer,
		"aud":        gocachedAudience,
		"nbf":        jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		"exp":        jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}

	token1 := st.exchangeToken(t, createJWT, privateKey, claims1)
	st.exchangeToken(t, createJWT, privateKey, claims2)

	cl := st.mkClient()
	cl.AccessToken = token1
	st.wantPut(cl, "abc123", "def456", "data789")

	// Fetch sessions page.
	resp, err := http.Get(debugHS.URL + "/sessions")
	if err != nil {
		t.Fatalf("error fetching sessions: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("error reading body: %v", err)
	}
	html := string(body)

	// Should contain both repos.
	if !strings.Contains(html, "foo/bar") {
		t.Errorf("expected HTML to contain foo/bar")
	}
	if !strings.Contains(html, "baz/qux") {
		t.Errorf("expected HTML to contain baz/qux")
	}
	if !strings.Contains(html, "grouped sessions") {
		t.Errorf("expected HTML to contain 'grouped sessions' heading")
	}
}

func TestMigrateV4ToV5(t *testing.T) {
	dir := t.TempDir()

	// Create a v4 database manually with the v4 schema (no Sessions table).
	v4Path := filepath.Join(dir, "gocached-v4.db")
	v4DB, err := sql.Open("sqlite", "file:"+v4Path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("error opening v4 db: %v", err)
	}
	// v4 schema: Actions, Blobs, Namespaces (no Sessions).
	v4Schema := `
	PRAGMA journal_mode=WAL;
	CREATE TABLE IF NOT EXISTS Actions (
	  NamespaceID  INTEGER NOT NULL,
	  ActionID     TEXT    NOT NULL,
	  BlobID       INTEGER NOT NULL,
	  AltOutputID  TEXT NOT NULL DEFAULT '',
	  CreateTime   INTEGER NOT NULL,
	  AccessTime   INTEGER NOT NULL,
	  PRIMARY KEY (NamespaceID, ActionID),
	  CHECK (ActionID = lower(ActionID)),
	  CHECK (ActionID GLOB '[0-9a-f]*'),
	  CHECK (CreateTime >= 0),
	  CHECK (AccessTime >= 0)
	) STRICT;
	CREATE INDEX IF NOT EXISTS idx_actions_access ON Actions(AccessTime);
	CREATE INDEX IF NOT EXISTS idx_actions_blobid ON Actions(BlobID);
	CREATE TABLE IF NOT EXISTS Blobs (
	  BlobID            INTEGER PRIMARY KEY AUTOINCREMENT,
	  SHA256            TEXT NOT NULL,
	  StoredSize        INTEGER NOT NULL,
	  UncompressedSize  INTEGER NOT NULL,
	  SmallData         BLOB,
	  CHECK (SmallData IS NULL OR length(SmallData) = StoredSize)
	) STRICT;
	CREATE UNIQUE INDEX IF NOT EXISTS idx_blobs_sha256 ON Blobs(SHA256);
	CREATE TABLE IF NOT EXISTS Namespaces (
	  NamespaceID INTEGER PRIMARY KEY AUTOINCREMENT,
	  Namespace   TEXT NOT NULL UNIQUE CHECK (Namespace = lower(Namespace))
	) STRICT;
	`
	if _, err := v4DB.Exec(v4Schema); err != nil {
		t.Fatalf("error creating v4 schema: %v", err)
	}
	// Insert some data.
	_, err = v4DB.Exec(`INSERT INTO Blobs (SHA256, StoredSize, UncompressedSize) VALUES (?, 5, 5)`, sha256OfEmpty)
	if err != nil {
		t.Fatalf("error inserting blob: %v", err)
	}
	_, err = v4DB.Exec(`INSERT INTO Actions (NamespaceID, ActionID, BlobID, CreateTime, AccessTime) VALUES (0, 'aabb', 1, 1000, 1000)`)
	if err != nil {
		t.Fatalf("error inserting action: %v", err)
	}
	if _, err := v4DB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatalf("error checkpointing: %v", err)
	}
	v4DB.Close()

	// Run migration.
	v5Path := filepath.Join(dir, "gocached-v5.db")
	if err := migrateV4ToV5(dir, v4Path, v5Path); err != nil {
		t.Fatalf("migrateV4ToV5 error: %v", err)
	}

	// Verify the v5 database.
	v5DB, err := sql.Open("sqlite", "file:"+v5Path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("error opening v5 db: %v", err)
	}
	defer v5DB.Close()

	// Sessions table should exist.
	var count int
	if err := v5DB.QueryRow("SELECT count(*) FROM Sessions").Scan(&count); err != nil {
		t.Fatalf("Sessions table doesn't exist: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 sessions, got %d", count)
	}

	// v4 data should be intact.
	var actionID string
	if err := v5DB.QueryRow("SELECT ActionID FROM Actions WHERE NamespaceID=0").Scan(&actionID); err != nil {
		t.Fatalf("error reading action: %v", err)
	}
	if actionID != "aabb" {
		t.Fatalf("expected action aabb, got %s", actionID)
	}
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
