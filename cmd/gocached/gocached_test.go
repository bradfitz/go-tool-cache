package main

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
	ijwt "github.com/bradfitz/go-tool-cache/cmd/gocached/internal/jwt"
	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/go-cmp/cmp"
)

// sha256OfEmpty is the SHA-256 hash of an empty string, used as a well-known
// value in SQLite to store bytes, as it's common.
const sha256OfEmpty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

type tester struct {
	t   testing.TB
	srv *server
	hs  *httptest.Server

	issuer    string
	audience  string
	createJWT func(claims jwt.MapClaims, signingKey *ecdsa.PrivateKey) string

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
	st.wantMetric(&st.srv.Puts, 1)
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

func newServerTester(t testing.TB) *tester {
	st := &tester{
		t:       t,
		curTime: time.Unix(1234, 0),
	}

	var err error
	dir := t.TempDir()
	st.srv, err = newServer(dir)
	if err != nil {
		t.Fatalf("newServer: %v", err)
	}
	st.srv.logf = t.Logf
	st.srv.verbose = true
	st.srv.clock = st.now

	st.hs = httptest.NewServer(st.srv)
	t.Cleanup(st.hs.Close)

	return st
}

// startOIDCServer starts a mock OIDC server and configures gocached to use it
// for JWT validation. The provided publicKey is what JWT signatures will be
// validated against. Use st.createJWT to create signed JWTs.
func (st *tester) startOIDCServer(publicKey crypto.PublicKey) {
	st.t.Helper()
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	st.t.Cleanup(srv.Close)

	st.issuer = fmt.Sprintf("http://%s", srv.Listener.Addr().String())
	st.audience = gocachedAudience

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"issuer":   st.issuer,
			"jwks_uri": fmt.Sprintf("%s/jwks", st.issuer),
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

	st.srv.jwtValidator = ijwt.NewJWTValidator(st.issuer, st.audience)
	st.srv.jwtValidator.Logf = st.Logf
	if err := st.srv.jwtValidator.RunUpdateJWKSLoop(st.srv.shutdownCtx); err != nil {
		st.t.Fatalf("failed to start JWKS loop for JWT validator: %v", err)
	}

	st.createJWT = func(claims jwt.MapClaims, signingKey *ecdsa.PrivateKey) string {
		st.t.Helper()
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
			st.t.Fatalf("error signing token: %v", err)
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
	st.wantMetric(&st.srv.Gets, 3)
	st.wantMetric(&st.srv.GetHits, 3)
	st.wantMetric(&st.srv.GetHitsInline, 1)

	// Do the same get again from the same client. This shouldn't hit the network.
	st.wantGet(c2, testActionID, testOutputID, testObjectValue)
	st.wantMetric(&st.srv.Gets, 0)

	// Cache miss. This should hit the network and fail.
	if _, _, err := c2.Get(ctx, testActionIDMiss); err != nil {
		t.Fatalf("miss Get: %v", err)
	}
	st.wantMetric(&st.srv.Gets, 1)
	st.wantMetric(&st.srv.GetHits, 0)

	// Check that access time gets updated.
	// Do it from a fresh client without a disk cache.
	st.wantMetric(&st.srv.GetAccessBumps, 0)
	st.advanceClock(relAtimeSeconds * 2 * time.Second) // advance clock by 2 days
	c3 := st.mkClient()
	st.wantGet(c3, testActionID, testOutputID, testObjectValue)
	st.wantMetric(&st.srv.GetAccessBumps, 1)

	// Get usage stats.
	stats, err := st.srv.usageStats()
	if err != nil {
		t.Fatalf("usageStats: %v", err)
	}
	want := &usageStats{
		MissingBlobRows: 0,
		ActionsLE: map[time.Duration]countAndSize{
			24 * time.Hour:   {Count: 1, Size: 9},
			48 * time.Hour:   {Count: 1, Size: 9},
			96 * time.Hour:   {Count: 3, Size: 1034},
			168 * time.Hour:  {Count: 3, Size: 1034},
			336 * time.Hour:  {Count: 3, Size: 1034},
			720 * time.Hour:  {Count: 3, Size: 1034},
			2160 * time.Hour: {Count: 3, Size: 1034},
			math.MaxInt64:    {Count: 3, Size: 1034},
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
				{BlobID: 1, Age: 3 * day, BlobSize: 1},
				{BlobID: 2, Age: 2 * day, BlobSize: 2},
				{BlobID: 3, Age: 1 * day, BlobSize: 3},
				{BlobID: 4, Age: 0, BlobSize: smallObjectSize + 1},
			},
		},
		{
			maxAge: 25 * time.Hour,
			limit:  100,
			want: []cleanCandidate{
				{BlobID: 1, Age: 3 * day, BlobSize: 1},
				{BlobID: 2, Age: 2 * day, BlobSize: 2},
			},
		},
		{
			maxAge: 0,
			limit:  2,
			want: []cleanCandidate{
				{BlobID: 1, Age: 3 * day, BlobSize: 1},
				{BlobID: 2, Age: 2 * day, BlobSize: 2},
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

	st1 := st.usageStats()
	if all, want := st1.All(), (countAndSize{Count: 3, Size: smallObjectSize*2 + 3 + smallLen}); all != want {
		t.Errorf("usageStats: %v; want %v", all, want)
	}
	if got, want := st.diskFiles(), []string{"333092a3daf718ed8f38a94e302df139edd4e3b5da4239a497995683942cf28c", "c6d8e9905300876046729949cc95c2385221270d389176f7234fe7ac00c4e430"}; !slices.Equal(got, want) {
		t.Errorf("diskFiles: %v; want %v", got, want)
	}

	clean1 := st.cleanOldObjects()
	if clean1.Count != 1 || clean1.Size != smallObjectSize+1 {
		t.Errorf("cleanOldObjects got %v, want {Count: 1, Size: %d}", clean1, smallObjectSize+1)
	}
	clean2 := st.cleanOldObjects()
	if clean2.Count != 0 || clean2.Size != 0 {
		t.Errorf("cleanOldObjects got %v, want {Count: 0, Size: 0}", clean2)
	}

	st2 := st.usageStats()
	if all, want := st2.All(), (countAndSize{Count: 2, Size: smallObjectSize + 2 + smallLen}); all != want {
		t.Errorf("usageStats after clean: %v; want %v", all, want)
	}
	if got, want := st.diskFiles(), []string{"333092a3daf718ed8f38a94e302df139edd4e3b5da4239a497995683942cf28c"}; !slices.Equal(got, want) {
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
			st := newServerTester(t)
			st.startOIDCServer(privateKey.Public())
			st.srv.jwtClaims = map[string]string{
				"sub": "user123",
			}
			st.srv.globalJWTClaims = map[string]string{
				"sub": "user123",
				"ref": "refs/heads/main",
			}

			// Generate JWT.
			tokenClaims := jwt.MapClaims{
				"sub": "user123",
				"iss": st.issuer,
				"aud": st.audience,
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
				"jwt": st.createJWT(tokenClaims, signingKey),
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

func TestJWTClaimFlag(t *testing.T) {
	for name, tc := range map[string]struct {
		input []string
		want  jwtClaimValue
	}{
		"single_claim": {
			input: []string{"role=builder"},
			want:  jwtClaimValue{"role": "builder"},
		},
		"multiple_claims": {
			input: []string{"env=prod", "team=devops"},
			want:  jwtClaimValue{"env": "prod", "team": "devops"},
		},
		"duplicate_keys": {
			input: []string{"key=value1", "key=value2"},
			want:  jwtClaimValue{"key": "value2"},
		},
		"value_with_equals": {
			input: []string{"data=a=b=c"},
			want:  jwtClaimValue{"data": "a=b=c"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			claim := make(jwtClaimValue)
			for _, v := range tc.input {
				if err := claim.Set(v); err != nil {
					t.Fatalf("Set failed: %v", err)
				}
			}
			if diff := cmp.Diff(claim, tc.want); diff != "" {
				t.Errorf("jwtClaimValue mismatch (-got +want):\n%s", diff)
			}
		})
	}
}

func TestInvalidJWTClaimFlags(t *testing.T) {
	for _, s := range []string{
		"invalidclaim",
		"=nokey",
		"novalue=",
	} {
		claim := make(jwtClaimValue)
		if err := claim.Set(s); err == nil {
			t.Fatalf("Set with invalid claim %q did not return error", s)
		}
	}
}
