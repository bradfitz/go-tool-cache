// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// The gocached daemon is an HTTP server daemon that go-cacher can hit. It does
// cache tiering and evicts old large things from disk, and can fetch metadata
// and object contents from peer cache servers.
//
// It uses sqlite (the pure Go modernc.org/sqlite driver) to store metadata and
// indexes.
//
/*

It speaks the same protocol as go-cacher-server, but requires
the "Want-Object: 1" header variant on the GET request.

	GET /action/<actionID-hex>
	Want-Object: 1

	200 OK
	Content-Type: application/octet-stream
	Content-Length: 1234
	Go-Output-Id: xxxxxxxxxxx

	<object-id-contents>

And to insert an object:

	PUT /<actionID>/<outputID>
	Content-Length: 1234

	<bytes>

*/
package gocached

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"maps"
	"math"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ijwt "github.com/bradfitz/go-tool-cache/gocached/internal/jwt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	_ "modernc.org/sqlite"
)

const (
	// smallObjectSize is the maximum size of an object that we store inline in the
	// database, rather than on disk. Empirically, about half of objects are 1KB or
	// smaller.
	smallObjectSize = 1 << 10

	// tokenPrefix is the prefix for all gocached access tokens.
	tokenPrefix = "gocached-token-"

	// gocachedAudience is the audience we require JWTs to have. Could be
	// configurable in future, but for now just needs to be specific to gocached.
	gocachedAudience = "gocached"
)

const schemaVersion = 3

const schema = `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS Actions (
  NamespaceID  INTEGER NOT NULL, -- 0 for global trusted namespace
  ActionID     TEXT    NOT NULL,
  BlobID       INTEGER NOT NULL,
  AltOutputID  TEXT NOT NULL DEFAULT '', -- if non-empty, the alternate object ID to use for this action; NULL means the blob's sha256
  CreateTime   INTEGER NOT NULL, -- unix sec when inserted (locally or on a peer)
  AccessTime   INTEGER NOT NULL, -- unix sec of last access

  PRIMARY KEY (NamespaceID, ActionID),

  CHECK (ActionID = lower(ActionID)),
  CHECK (ActionID GLOB '[0-9a-f]*'),
  CHECK (CreateTime >= 0),
  CHECK (AccessTime >= 0)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_actions_access ON Actions(AccessTime);
CREATE INDEX IF NOT EXISTS idx_actions_blobid ON Actions(BlobID);

CREATE TABLE IF NOT EXISTS Blobs (
  BlobID       INTEGER PRIMARY KEY AUTOINCREMENT,
  SHA256       TEXT NOT NULL,
  BlobSize     INTEGER NOT NULL, -- size in bytes, either inline or on disk
  SmallData    BLOB, -- NULL if stored on disk

  CHECK (SmalLData IS NULL OR length(SmallData) = BlobSize)
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_blobs_sha256 ON Blobs(SHA256);

CREATE TABLE IF NOT EXISTS Namespaces (
  NamespaceID INTEGER PRIMARY KEY AUTOINCREMENT,
  Namespace   TEXT NOT NULL UNIQUE CHECK (Namespace = lower(Namespace))
) STRICT;
`

func openDB(dbDir string) (*sql.DB, error) {
	dbPath := filepath.Join(dbDir, fmt.Sprintf("gocached-v%d.db", schemaVersion))
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(0) // no limit
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

func (srv *Server) Start(ctx context.Context) error {
	db, err := openDB(srv.Dir)
	if err != nil {
		return fmt.Errorf("openDB: %w", err)
	}
	srv.db = db
	srv.sessions = make(map[string]*sessionData)
	srv.shutdownCtx, srv.shutdownCancel = context.WithCancel(ctx)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
	srv.registerMetrics(reg)

	srv.metricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog: log.Default(),
	})

	log.Printf("gocached: scanning usage & cleaning as needed...")
	us, err := srv.usageStats()
	if err != nil {
		return fmt.Errorf("getting usage stats: %w", err)
	}

	log.Printf("gocached: current usage: %v of limit %v", us.All(), bytesFmt(srv.MaxSize))
	if res, err := srv.cleanOldObjects(us); err != nil {
		return fmt.Errorf("clean old objects: %w", err)
	} else if res.Count > 0 {
		log.Printf("gocached: cleaned %v", res)
	}

	if srv.JWTIssuer != "" {
		srv.jwtValidator = ijwt.NewJWTValidator(srv.JWTIssuer, gocachedAudience)
		if err := srv.jwtValidator.RunUpdateJWKSLoop(srv.shutdownCtx); err != nil {
			return fmt.Errorf("failed to fetch JWKS for JWT validator: %w", err)
		}

		log.Printf("gocached: using JWT issuer %q with claims %v, global claims %v", srv.JWTIssuer, srv.JWTClaims, srv.GlobalJWTClaims)

		go srv.runCleanSessionsLoop()
	}

	go srv.runCleanLoop()

	return nil
}

func (s *Server) registerMetrics(reg *prometheus.Registry) {
	rv := reflect.ValueOf(s).Elem()
	t := reflect.TypeOf(s).Elem()
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.Type == reflect.TypeFor[expvar.Int]() {
			expvarInt := rv.Field(i).Addr().Interface().(*expvar.Int)
			typ := sf.Tag.Get("type")
			name := sf.Tag.Get("name")
			if typ == "" {
				panic("missing type tag for " + sf.Name)
			}
			if name == "" {
				panic("missing name tag for " + sf.Name)
			}
			help := sf.Tag.Get("help")
			metricName := "gocached_" + name

			if tag := sf.Tag.Get("type"); tag != "" {
				if tag == "gauge" {
					reg.MustRegister(singleMetricCollector{&expvarGaugeMetric{
						desc: prometheus.NewDesc(metricName, help, nil, nil),
						v:    expvarInt,
					}})
				} else if tag == "counter" {
					reg.MustRegister(singleMetricCollector{&expvarCounterMetric{
						desc: prometheus.NewDesc(metricName, help, nil, nil),
						v:    expvarInt,
					}})
				}
			}
		}
	}
}

type Server struct {
	Dir             string // for SQLite DB + large blobs
	Verbose         bool
	Logf            func(format string, args ...any)
	MaxSize         int64             // maximum size of the cache in bytes; 0 means no limit
	MaxAge          time.Duration     // maximum age of objects; 0 means no limit
	JWTIssuer       string            // issuer URL for JWTs
	JWTClaims       map[string]string // claims required for any JWT to start a session
	GlobalJWTClaims map[string]string // additional claims required to write to global namespace

	db             *sql.DB
	clock          func() time.Time // if non-nil, alternate time.Now for testing
	metricsHandler http.Handler
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	jwtValidator *ijwt.Validator         // nil unless -jwt-issuer flag is set
	sessionsMu   sync.RWMutex            // guards sessions
	sessions     map[string]*sessionData // maps access token -> session data.

	// sqliteWriteMu serializes access to SQLite. In theory the SQLite driver
	// should serialize access with our 5000ms busy timeout, but empirically we
	// sometimes seen DB busy errors. Just serialize it explicitly out of
	// laziness for now.
	sqliteWriteMu sync.Mutex

	lastUsage atomic.Pointer[usageStats]

	// Metrics
	ActiveGets     expvar.Int `type:"gauge" name:"active_gets" help:"currently pending get requests; should usually be zero"`
	ActivePuts     expvar.Int `type:"gauge" name:"active_puts" help:"currently pending put requests; should usually be zero"`
	Gets           expvar.Int `type:"counter" name:"gets" help:"total number of gocache get requests"` // gets = getHits + getErrs + implicit misses
	GetBytes       expvar.Int `type:"counter" name:"get_bytes" help:"total bytes fetched from gocache gets that were cache hits"`
	GetHits        expvar.Int `type:"counter" name:"get_hits" help:"total number of successful gocache get requests"`
	GetAccessBumps expvar.Int `type:"counter" name:"get_access_bumps" help:"number of times a get request updated the access time of object"`
	GetHitsInline  expvar.Int `type:"counter" name:"get_hits_inline" help:"cache hits served from inline database storage (small objects)"`
	GetErrs        expvar.Int `type:"counter" name:"get_errs" help:"number of gocache get request errors"`
	Puts           expvar.Int `type:"counter" name:"puts" help:"total number of gocache put requests"`
	PutErrs        expvar.Int `type:"counter" name:"put_errs" help:"number of gocache put request errors"`
	PutsDup        expvar.Int `type:"counter" name:"puts_dup" help:"total number of gocache put requests that are duplicates of a mapping we already had"`
	PutsBytes      expvar.Int `type:"counter" name:"put_bytes" help:"total bytes added from gocache puts"`
	PutsInline     expvar.Int `type:"counter" name:"put_inline" help:"subset of gocached_puts that were stored inline (small objects)"`
	BlobCount      expvar.Int `type:"gauge" name:"blob_count" help:"number of blobs currently stored in the cache"`
	BlobBytes      expvar.Int `type:"gauge" name:"blob_bytes" help:"sum of blob sizes currently stored in the cache"`
	EvictedBlobs   expvar.Int `type:"counter" name:"evicted_blobs" help:"number of blobs evicted from the cache"`
	EvictedBytes   expvar.Int `type:"counter" name:"evicted_bytes" help:"number of bytes evicted from the cache"`
	Sessions       expvar.Int `type:"gauge" name:"sessions" help:"number of active authenticated sessions"`
	Auths          expvar.Int `type:"counter" name:"auth_attempts" help:"number of successful token exchanges"`
	AuthErrs       expvar.Int `type:"counter" name:"auth_errs" help:"number of failed token exchanges"`
}

// sessionData corresponds to a specific access token, and is only used if JWT
// auth is enabled.
type sessionData struct {
	expiry        time.Time      // Session valid until.
	globalNSWrite bool           // Whether this session can write to the cache's global namespace.
	claims        map[string]any // Claims from the JWT used to create this session, stored for debug.

	mu    sync.Mutex // Guards stats.
	stats stats
}

// stats holds per-request or per-session stats which get rolled up into server
// stats. See [server] struct for detailed definitions.
type stats struct {
	LastUsed       time.Time // Only applies to session stats. Last time the access token for this session was used.
	Gets           int64
	GetBytes       int64
	GetHits        int64
	GetAccessBumps int64
	GetHitsInline  int64
	GetNanos       int64
	GetErrs        int64
	Puts           int64
	PutErrs        int64
	PutsDup        int64
	PutsBytes      int64
	PutsInline     int64
	PutsNanos      int64
}

func (srv *Server) now() time.Time {
	if srv.clock != nil {
		return srv.clock()
	}
	return time.Now()
}

func (srv *Server) ServeHTTPDebug(w http.ResponseWriter, r *http.Request) {
	if srv.Verbose {
		srv.Logf("ServeHTTPDebug: %s %s", r.Method, r.RequestURI)
	}
	switch {
	case r.URL.Path == "/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, "<h1>gocached</h1>")
		io.WriteString(w, "<p>This is a shared Go build cache server, hit by GOCACHEPROG clients.</p>")
		io.WriteString(w, "<p>See <a href='/usage'>/usage</a> for usage stats.</p>")
		io.WriteString(w, "<p>See <a href='/sessions'>/sessions</a> for session data</p>")
		io.WriteString(w, "<p>See <a href='/metrics'>/metrics</a> for Prometheus metrics.</p>")
		io.WriteString(w, "<p>See <a href='/debug/pprof/'>/debug/pprof/</a> for pprof</p>")
		io.WriteString(w, "<p>See <a href='/debug/pprof/goroutine?debug=2'>/debug/pprof/goroutine?debug=2</a> - full goroutines</p>")
	case r.URL.Path == "/usage":
		srv.serveUsage(w, r)
	case r.URL.Path == "/sessions":
		srv.serveSessions(w, r)
	case r.URL.Path == "/metrics":
		srv.metricsHandler.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/profile"):
		pprof.Profile(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/cmdline"):
		pprof.Cmdline(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/symbol"):
		pprof.Symbol(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/trace"):
		pprof.Trace(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof/"):
		pprof.Index(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if srv.Verbose {
		srv.Logf("ServeHTTP: %s %s", r.Method, r.RequestURI)
	}

	var sessionData *sessionData // remains nil for unauthenticated requests.
	reqStats := &stats{}
	defer func() {
		// Call inside func to capture maybe-updated sessionData pointer.
		srv.processRequestStats(reqStats, sessionData)
	}()

	// Handle session auth first if enabled.
	if srv.jwtValidator != nil {
		// If JWT auth enabled, this is the only unauthenticated (non-debug) endpoint.
		if r.Method == "POST" && r.URL.Path == "/auth/exchange-token" {
			srv.handleTokenExchange(w, r)
			return
		}

		// Check for session data and error if none.
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !strings.HasPrefix(token, tokenPrefix) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		var ok bool
		sessionData, ok = srv.getSessionData(token)
		if !ok || srv.now().After(sessionData.expiry) {
			if srv.Verbose {
				reason := fmt.Sprintf("exists: %v", ok)
				if sessionData != nil {
					reason += fmt.Sprintf(", expiry: %v", sessionData.expiry)
				}
				srv.Logf("unauthorized; %s", reason)
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if r.Method == "PUT" {
		if sessionData != nil && !sessionData.globalNSWrite {
			// TODO(tomhjp): support per-namespace writes.
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		srv.handlePut(w, r, reqStats)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/action/") {
		srv.handleGetAction(w, r, reqStats)
		return
	}
	if sessionData != nil && r.URL.Path == "/session/stats" {
		srv.handleSessionStats(w, sessionData)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func (srv *Server) getSessionData(token string) (*sessionData, bool) {
	srv.sessionsMu.Lock()
	defer srv.sessionsMu.Unlock()
	sessionData, ok := srv.sessions[token]
	return sessionData, ok
}

func (srv *Server) addSessionData(token string, sessionData *sessionData) {
	srv.sessionsMu.Lock()
	defer srv.sessionsMu.Unlock()
	srv.sessions[token] = sessionData
	srv.Sessions.Add(1)
}

func (srv *Server) processRequestStats(req *stats, sessionData *sessionData) {
	srv.Gets.Add(req.Gets)
	srv.GetBytes.Add(req.GetBytes)
	srv.GetHits.Add(req.GetHits)
	srv.GetAccessBumps.Add(req.GetAccessBumps)
	srv.GetHitsInline.Add(req.GetHitsInline)
	srv.GetErrs.Add(req.GetErrs)
	srv.Puts.Add(req.Puts)
	srv.PutErrs.Add(req.PutErrs)
	srv.PutsDup.Add(req.PutsDup)
	srv.PutsBytes.Add(req.PutsBytes)
	srv.PutsInline.Add(req.PutsInline)

	if sessionData != nil {
		sessionData.mu.Lock()
		defer sessionData.mu.Unlock()

		sessionData.stats.LastUsed = srv.now().UTC()
		sessionData.stats.Gets += req.Gets
		sessionData.stats.GetBytes += req.GetBytes
		sessionData.stats.GetHits += req.GetHits
		sessionData.stats.GetAccessBumps += req.GetAccessBumps
		sessionData.stats.GetHitsInline += req.GetHitsInline
		sessionData.stats.GetErrs += req.GetErrs
		sessionData.stats.GetNanos += req.GetNanos
		sessionData.stats.Puts += req.Puts
		sessionData.stats.PutErrs += req.PutErrs
		sessionData.stats.PutsDup += req.PutsDup
		sessionData.stats.PutsBytes += req.PutsBytes
		sessionData.stats.PutsInline += req.PutsInline
		sessionData.stats.PutsNanos += req.PutsNanos
	}
}

func getHexSuffix(r *http.Request, prefix string) (hexSuffix string, ok bool) {
	hexSuffix, _ = strings.CutPrefix(r.RequestURI, prefix)
	if !validHex(hexSuffix) {
		return "", false
	}
	return hexSuffix, true
}

func validHex(x string) bool {
	if len(x) < 4 || len(x) > 1000 || len(x)%2 == 1 {
		return false
	}
	for i := range x {
		b := x[i]
		if b >= '0' && b <= '9' || b >= 'a' && b <= 'f' {
			continue
		}
		return false
	}
	return true
}

// relAtimeSeconds is how old an access time needs to be before
// we do a DB write to update it.
const relAtimeSeconds = 60 * 60 * 24 // 1 day

func (srv *Server) handleGetAction(w http.ResponseWriter, r *http.Request, stats *stats) {
	srv.ActiveGets.Add(1)
	defer srv.ActiveGets.Add(-1)

	start := srv.now()
	defer func() {
		stats.GetNanos += srv.now().Sub(start).Nanoseconds()
	}()
	stats.Gets++
	ctx := r.Context()

	httpErr := func(msg string, code int) {
		http.Error(w, msg, code)
		stats.GetErrs++
	}

	actionID, ok := getHexSuffix(r, "/action/")
	if !ok {
		httpErr("bad request", http.StatusBadRequest)
		return
	}
	if r.Header.Get("Want-Object") != "1" {
		httpErr("bad request: missing Want-Object header", http.StatusBadRequest)
		return
	}

	var sha256hex string
	var size int64
	var smallData sql.NullString
	var altObjectID string
	var accessTime int64
	namespaceID := 0 // global for now; TODO(bradfitz): support namespaces
	err := srv.db.QueryRow(
		"SELECT b.SHA256, b.BlobSize, b.SmallData, a.AltOutputID, a.AccessTime FROM Actions a, Blobs b WHERE a.NameSpaceID = ? AND a.ActionID = ? AND a.BlobID = b.BlobID",
		namespaceID, actionID).Scan(
		&sha256hex, &size, &smallData, &altObjectID, &accessTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		srv.Logf("QueryRow error: %v", err)
		httpErr("QueryRow error", http.StatusInternalServerError)
		return
	}

	// If it's been more than a day since the last access, update the access time.
	// This is similar to the Linux "relatime" behavior.
	now := srv.now().Unix()
	if accessTime < now-relAtimeSeconds {
		// TODO(bradfitz): do this async? not worth blocking the caller.
		// But we need a mechanism for tests to wait on async work.
		srv.sqliteWriteMu.Lock()
		_, err := srv.db.Exec("UPDATE Actions SET AccessTime = ? WHERE ActionID = ?", now, actionID)
		srv.sqliteWriteMu.Unlock()
		if err != nil {
			srv.Logf("Update AccessTime error: %v", err)
			httpErr("internal server error", http.StatusInternalServerError)
			return
		}
		stats.GetAccessBumps++
	}

	stats.GetHits++

	outputID := cmp.Or(altObjectID, sha256hex)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(size))
	w.Header().Set("Go-Output-Id", outputID)

	if r.Method == "HEAD" || size == 0 {
		return
	}

	if smallData.Valid {
		// For small outputs stored inline in the database, we can return them directly.
		stats.GetHitsInline++
		stats.GetBytes += size
		io.WriteString(w, smallData.String)
		return
	}

	// Otherwise, for large objects that we know about, we can try to get them
	// from our local disk or a peer.

	rc, err := srv.getObjectFromDiskOrPeer(ctx, sha256hex)
	if err != nil {
		srv.Logf("Get object error: %v", actionID, err)
		httpErr("Get object error", http.StatusInternalServerError)
		return
	}
	if rc == nil {
		// Our database suggested we should've had this object,
		// but maybe somebody delete it by hand from the filesystem.
		// Just treat it as a cache miss. The background cleanup
		// will eventually remove the Action row from the DB
		// after identifying it as a dangling reference.
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	stats.GetBytes += size
	defer rc.Close()
	io.Copy(w, rc)
}

// getObjectFromDiskOrPeer retrieves the object for the given actionID, either
// from disk or a peer. This is used after a local DB lookup discovers the content
// exists but is not stored in SQLite.
//
// It returns (nil, nil) on miss.
func (srv *Server) getObjectFromDiskOrPeer(_ context.Context, sha256hex string) (rc io.ReadCloser, err error) {
	if len(sha256hex) != sha256.Size*2 {
		return nil, fmt.Errorf("invalid sha256hex %q", sha256hex)
	}
	diskPath := filepath.Join(srv.Dir, sha256hex[:2], sha256hex)
	f, err := os.Open(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			// TODO(bradfitz): search peers, S3, etc.
			// For now, just return nil, nil on miss.
			return nil, nil
		}
		return nil, err
	}
	return f, nil
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, stats *stats) {
	s.ActivePuts.Add(1)
	defer s.ActivePuts.Add(-1)

	start := s.now()
	defer func() {
		stats.PutsNanos += s.now().Sub(start).Nanoseconds()
	}()

	if r.Method != "PUT" {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	actionID, outputID, ok := strings.Cut(r.RequestURI[len("/"):], "/")
	if !ok || !validHex(actionID) || !validHex(outputID) {
		http.Error(w, "bad URI", http.StatusBadRequest)
		return
	}
	if r.ContentLength == -1 {
		http.Error(w, "missing Content-Length", http.StatusBadRequest)
		return
	}

	hasher := sha256.New()
	hashingBody := io.TeeReader(r.Body, hasher)

	var smallData []byte
	if r.ContentLength <= smallObjectSize {
		// Store small objects inline in the database.
		var err error
		smallData, err = io.ReadAll(hashingBody)
		if err != nil {
			s.Logf("Read content error: %v", err)
			http.Error(w, "Read content error", http.StatusInternalServerError)
			return
		}
		if int64(len(smallData)) != r.ContentLength {
			// This check is redundant with net/http's validation, but
			// for extra clarity.
			http.Error(w, "bad content length", http.StatusInternalServerError)
			return
		}
	} else {
		// For larger objects, we store them on disk.
		if err := s.writeDiskBlob(r.ContentLength, hashingBody); err != nil {
			s.Logf("Write disk blob error: %v", err)
			http.Error(w, "Write disk blob error", http.StatusInternalServerError)
			return
		}
	}

	sha256hex := fmt.Sprintf("%x", hasher.Sum(nil))
	blobSize := r.ContentLength

	s.sqliteWriteMu.Lock()
	defer s.sqliteWriteMu.Unlock()

	var blobID int64
	err := s.db.QueryRow(`INSERT INTO Blobs (SHA256, BlobSize, SmallData)
		VALUES (?, ?, ?)
		ON CONFLICT(SHA256) DO UPDATE SET SHA256=excluded.SHA256
		RETURNING BlobID;
`, sha256hex, blobSize, smallData).Scan(&blobID)
	if err != nil {
		s.Logf("Blobs insert error: %v", err)
		stats.PutErrs++
		http.Error(w, "Blobs insert error", http.StatusInternalServerError)
		return
	}

	// Insert or update the action in the database.
	nowUnix := s.now().Unix()
	altObjectID := ""
	namespace := 0 // global for now; TODO(bradfitz): support namespaces
	if sha256hex != outputID {
		altObjectID = outputID
	}
	res, err := s.db.Exec(`INSERT OR IGNORE INTO Actions (NamespaceID, ActionID, BlobID, AltOutputID, CreateTime, AccessTime)
	VALUES (?, ?, ?, ?, ?, ?)`,
		namespace,
		actionID,
		blobID,
		altObjectID,
		nowUnix,
		nowUnix,
	)
	if err != nil {
		s.Logf("Actions insert error: %v", err)
		stats.PutErrs++
		http.Error(w, "Actions insert error", http.StatusInternalServerError)
		return
	}

	affected, err := res.RowsAffected()
	if err != nil {
		s.Logf("Actions rows affected error: %v", err)
		stats.PutErrs++
		http.Error(w, "Actions rows affected error", http.StatusInternalServerError)
		return
	}

	if affected == 0 {
		stats.PutsDup++
	}

	stats.Puts++
	stats.PutsBytes += r.ContentLength
	if smallData != nil {
		stats.PutsInline++
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleTokenExchange handles POST /auth/exchange-token requests to exchange
// a JWT for an access token. Each access token represents a session that will
// last for one hour and have cache stats associated with it.
func (srv *Server) handleTokenExchange(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JWT string `json:"jwt"`
	}
	// JWTs are often sent in HTTP headers, so 4KiB should ~always be enough.
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		srv.AuthErrs.Add(1)
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	jwtClaims, err := srv.jwtValidator.Validate(r.Context(), req.JWT)
	if err != nil {
		srv.AuthErrs.Add(1)
		if srv.Verbose {
			srv.Logf("token exchange: JWT validation error: %v", err)
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	globalNSWrite, err := srv.evaluateClaims(jwtClaims)
	if err != nil {
		srv.AuthErrs.Add(1)
		if srv.Verbose {
			srv.Logf("token exchange: %v", err)
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	const ttl = time.Hour
	// 52 base32 characters, 256 bits of entropy.
	accessToken := tokenPrefix + strings.ToLower(rand.Text()+rand.Text())
	srv.addSessionData(accessToken, &sessionData{
		expiry:        srv.now().UTC().Add(ttl),
		globalNSWrite: globalNSWrite,
		claims:        jwtClaims,
	})

	resp := map[string]any{
		"access_token": accessToken,
		"token_type":   "Bearer",
		"expires_in":   ttl.Seconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		srv.AuthErrs.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	srv.Auths.Add(1)
}

func (srv *Server) evaluateClaims(claims map[string]any) (globalNSWrite bool, _ error) {
	if missing := findMissingClaims(srv.JWTClaims, claims); len(missing) > 0 {
		return false, fmt.Errorf("got claims %v; missing required claims: %v", claims, missing)
	}

	if missing := findMissingClaims(srv.GlobalJWTClaims, claims); len(missing) == 0 {
		return true, nil
	} else if srv.Verbose {
		srv.Logf("token exchange: missing global namespace write claims: %v", missing)
	}

	return false, nil
}

func findMissingClaims(wantClaims map[string]string, gotClaims map[string]any) map[string]any {
	missing := make(map[string]any)
	for k, want := range wantClaims {
		if got, ok := gotClaims[k]; !ok || got != want {
			missing[k] = want
		}
	}
	return missing
}

func (srv *Server) handleSessionStats(w http.ResponseWriter, sessionData *sessionData) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(sessionData.stats); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) sha256Filepath(hash [sha256.Size]byte) string {
	hex := fmt.Sprintf("%x", hash)
	return filepath.Join(s.Dir, hex[:2], hex)
}

func (s *Server) writeDiskBlob(size int64, r io.Reader) (err error) {
	nowUnix := s.now().Unix()
	tf, err := os.CreateTemp(s.Dir, fmt.Sprintf("upload-%d-*", nowUnix))
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			return
		}
		tf.Close()
		os.Remove(tf.Name())
	}()
	hasher := sha256.New()
	n, err := io.Copy(tf, io.LimitReader(io.TeeReader(r, hasher), size+1))
	if err != nil {
		return err
	}
	if n != size {
		return fmt.Errorf("wrote %d bytes; wanted %d", n, size)
	}
	if err := tf.Close(); err != nil {
		return err
	}
	var hash [sha256.Size]byte
	hasher.Sum(hash[:0])

	target := s.sha256Filepath(hash)
	if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
		return err
	}
	return os.Rename(tf.Name(), target)
}

type countAndSize struct {
	Count int64 // number of actions
	Size  int64 // total size of all actions' blobs (even if shared by other actions)
}

func (cs countAndSize) String() string {
	if cs.Count == 0 {
		return "0 objects, 0 bytes"
	}
	return fmt.Sprintf("%d objects, %s", cs.Count, bytesFmt(cs.Size))
}

type usageStats struct {
	// ActionsLE is a histogram of the actions in the DB by their access time.
	//
	// The key is a Prometheus-style histogram "less than" value. That is, if
	// there are map keys for 24h and 48h, the latter includes the sum of the
	// 24h values as well.
	//
	// The map keys are day-granularity, as the access time is only updated once
	// it's over a day old.
	//
	// So the map keys are 24h, 48h, 96h, 168h (7d), 336h (14d), 720h
	// (30d), and 2160h (90d) and math.MaxInt64 for infinity.
	ActionsLE map[time.Duration]countAndSize

	// MissingBlobRows is the number of rows in the Actions table that
	// reference a BlobID that doesn't exist in the Blobs table.
	// This should always be zero in a healthy system.
	MissingBlobRows int
}

func (us *usageStats) All() countAndSize { return us.ActionsLE[math.MaxInt64] }

const day = 24 * time.Hour

var standardDurs = []time.Duration{
	1 * day,
	2 * day,
	4 * day,
	7 * day,
	14 * day,
	30 * day,
	90 * day,
	math.MaxInt64,
}

func (s *Server) usageStats() (_ *usageStats, err error) {
	defer func() {
		if err != nil {
			s.Logf("usageStats error: %v", err)
		}
	}()

	st := &usageStats{
		ActionsLE: make(map[time.Duration]countAndSize),
	}

	// Build the durations to use for the histogram.
	// The math.MaxInt64 value is always included.
	// If s.maxAge is set, we ignore sizes above that, except
	// for the math.MaxInt64 value.
	var durs []time.Duration
	if s.MaxAge == 0 {
		durs = standardDurs
	} else {
		durs = make([]time.Duration, 0, len(standardDurs)+1)
		durs = append(durs, s.MaxAge)
		for _, d := range standardDurs {
			if d < s.MaxAge || d == math.MaxInt64 {
				durs = append(durs, d)
			}
		}
		slices.Sort(durs)
	}

	now := s.now().Unix()
	rows, err := s.db.Query(
		"SELECT a.BlobID, a.AccessTime, b.BlobSize FROM Actions a LEFT JOIN Blobs b ON a.BlobID = b.BlobID")
	if err != nil {
		return nil, fmt.Errorf("query Actions: %w", err)
	}
	var blobID int64
	var accessTime int64
	var blobSize sql.NullInt64
	for rows.Next() {
		if err := rows.Scan(&blobID, &accessTime, &blobSize); err != nil {
			return nil, fmt.Errorf("rows.Scan: %w", err)
		}
		if !blobSize.Valid {
			st.MissingBlobRows++
			continue
		}

		dur := time.Duration(now-accessTime) * time.Second
		if dur < 0 {
			dur = 0
		}
		for _, d := range durs {
			if dur < d {
				was := st.ActionsLE[d]
				was.Count++
				was.Size += blobSize.Int64
				st.ActionsLE[d] = was
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Next: %w", err)
	}

	s.lastUsage.Store(st)
	all := st.All()
	s.BlobCount.Set(all.Count)
	s.BlobBytes.Set(all.Size)
	return st, nil
}

type cleanCandidate struct {
	BlobID   int64
	Age      time.Duration
	BlobSize int64 // size of the blob, in bytes
}

func (s *Server) cleanCandidates(olderThan time.Duration, limit int64) ([]cleanCandidate, error) {
	now := s.now()
	nowUnix := now.Unix()
	cutoff := now.Add(-olderThan).Unix()

	rows, err := s.db.Query(`
		SELECT b.BlobID, MAX(a.AccessTime), b.BlobSize
		FROM Blobs b LEFT JOIN Actions a ON b.BlobID = a.BlobID
		GROUP BY b.BlobID
		HAVING MAX(a.AccessTime) <= ?
		ORDER BY MAX(a.AccessTime)
		LIMIT ?`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("query clean candidates: %w", err)
	}
	defer rows.Close()

	var candidates []cleanCandidate
	var accessTime int64
	for rows.Next() {
		var c cleanCandidate
		if err := rows.Scan(&c.BlobID, &accessTime, &c.BlobSize); err != nil {
			return nil, fmt.Errorf("rows.Scan: %w", err)
		}
		c.Age = time.Duration(nowUnix-accessTime) * time.Second
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Next: %w", err)
	}

	return candidates, nil
}

func (srv *Server) deleteBlobs(blobIDs ...int64) error {
	srv.sqliteWriteMu.Lock()
	defer srv.sqliteWriteMu.Unlock()

	tx, err := srv.db.Begin()
	if err != nil {
		return fmt.Errorf("delete blob Begin: %w", err)
	}
	defer tx.Rollback()

	var sumBytes int64
	for _, blobID := range blobIDs {
		var sha256Hex string
		var blobSize int64
		if err := tx.QueryRow("SELECT SHA256, BlobSize FROM Blobs WHERE BlobID = ?", blobID).Scan(&sha256Hex, &blobSize); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("querying blob SHA256: %w", err)
		}
		sumBytes += blobSize
		if _, err := tx.Exec("DELETE FROM Blobs WHERE BlobID = ?", blobID); err != nil {
			return fmt.Errorf("deleting blob: %w", err)
		}
		if _, err := tx.Exec("DELETE FROM Actions WHERE BlobID = ?", blobID); err != nil {
			return fmt.Errorf("deleting actions: %w", err)
		}
		var hash [sha256.Size]byte
		if _, err := hex.Decode(hash[:], []byte(sha256Hex)); err == nil {
			if err := os.Remove(srv.sha256Filepath(hash)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("removing disk file: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	srv.EvictedBlobs.Add(int64(len(blobIDs)))
	srv.EvictedBytes.Add(sumBytes)

	return nil
}

func (srv *Server) cleanOldObjects(us *usageStats) (countAndSize, error) {
	var zero countAndSize
	var ret countAndSize

	all := us.ActionsLE[math.MaxInt64]
	if srv.Verbose {
		srv.Logf("current usage stats: %v", all)
		last := all
		for _, d := range slices.Sorted(maps.Keys(us.ActionsLE)) {
			if d == math.MaxInt64 {
				continue // skip infinity
			}
			c := us.ActionsLE[d]
			srv.Logf("  <=%v: %v", durFmt(d), c)
			if last == c {
				break
			}
			last = c
		}
	}

	// First clean things that are just too old.
	if srv.MaxAge > 0 {
		if toDelete := all.Count - us.ActionsLE[srv.MaxAge].Count; toDelete > 0 {
			srv.Logf("Cleaning %d objects older than %v ...", toDelete, durFmt(srv.MaxAge))
			candidates, err := srv.cleanCandidates(srv.MaxAge, toDelete+1)
			if err != nil {
				return zero, fmt.Errorf("getting clean candidates: %v", err)
			}
			blobIDs := make([]int64, 0, len(candidates))
			var sumSize int64
			for _, c := range candidates {
				blobIDs = append(blobIDs, c.BlobID)
				sumSize += c.BlobSize
			}
			if err := srv.deleteBlobs(blobIDs...); err != nil {
				return zero, fmt.Errorf("deleting old blobs: %v", err)
			}
			all.Count -= int64(len(candidates))
			all.Size -= sumSize
			ret.Count += int64(len(candidates))
			ret.Size += sumSize
		}
	}

	for srv.MaxSize > 0 && all.Size > srv.MaxSize {
		toClean := all.Size - srv.MaxSize
		if srv.Verbose {
			srv.Logf("need to clean %v to get under max size of %v ...",
				bytesFmt(toClean), bytesFmt(srv.MaxSize))
		}

		var batchBytes int64
		var blobIDs []int64
		candidates, err := srv.cleanCandidates(0, 10000)
		if err != nil {
			return zero, fmt.Errorf("getting clean candidates: %v", err)
		}
		for _, c := range candidates {
			blobIDs = append(blobIDs, c.BlobID)
			batchBytes += c.BlobSize
			if batchBytes >= toClean {
				break
			}
		}
		if err := srv.deleteBlobs(blobIDs...); err != nil {
			return zero, fmt.Errorf("deleting old blobs: %v", err)
		}

		ret.Count += int64(len(blobIDs))
		ret.Size += batchBytes
		all.Count -= int64(len(blobIDs))
		all.Size -= batchBytes

		if len(blobIDs) == len(candidates) {
			// We didn't find enough candidates to delete.
			// Just stop here.
			srv.Logf("[unexpected] didn't find enough candidates to delete")
			break
		}
	}

	return ret, nil
}

func (srv *Server) runCleanLoop() {
	for {
		select {
		case <-srv.shutdownCtx.Done():
			return
		case <-time.After(5 * time.Minute):
		}

		us, err := srv.usageStats()
		if err != nil {
			srv.Logf("error getting usage stats: %v", err)
			continue
		}

		res, err := srv.cleanOldObjects(us)
		if err != nil {
			srv.Logf("error cleaning old objects: %v", err)
			continue
		}
		if res.Count > 0 {
			srv.Logf("cleaned %v", res)
			srv.usageStats() // for side effect of updating lastUsage
		}

	}
}

func (srv *Server) runCleanSessionsLoop() {
	for {
		select {
		case <-srv.shutdownCtx.Done():
			return
		case <-time.After(time.Hour):
		}

		srv.sessionsMu.Lock()
		count := len(srv.sessions)
		var deleted int
		for token, metadata := range srv.sessions {
			if time.Now().After(metadata.expiry) {
				delete(srv.sessions, token)
				deleted++
				srv.Sessions.Add(-1)
			}
		}
		srv.sessionsMu.Unlock()
		srv.Logf("cleaned up %d/%d access tokens", deleted, count)
	}
}

func durFmt(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days > 0 {
		return fmt.Sprintf("%dd", days)
	}
	return d.String()
}

func bytesFmt(n int64) string {
	if n >= 1<<30 {
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	}
	if n >= 1<<20 {
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
	if n >= 1<<10 {
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", n)
}

func (srv *Server) serveUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		// For side effect of updating lastUsage.
		_, err := srv.usageStats()
		if err != nil {
			http.Error(w, "error getting usage stats: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	us := srv.lastUsage.Load()
	if us == nil {
		http.Error(w, "no usage stats available", http.StatusInternalServerError)
		return
	}

	// Print out an HTML table of the usage stats, sorted by age.
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<html><body><h1>gocached usage stats</h1>\n")
	fmt.Fprintf(w, "<p>Current usage: %v of limit %v</p>\n",
		us.All(), bytesFmt(srv.MaxSize))

	fmt.Fprintf(w, "<table border='1' cellpadding=5>\n")
	fmt.Fprintf(w, "<tr><th>Age</th><th>Count</th><th>Size</th></tr>\n")
	for _, d := range slices.Sorted(maps.Keys(us.ActionsLE)) {
		var title string
		if d == math.MaxInt64 {
			title = "all"
		} else {
			title = "&lt;= " + durFmt(d)
		}
		c := us.ActionsLE[d]
		fmt.Fprintf(w, "<tr><td>%s</td><td>%d</td><td>%s</td></tr>\n",
			title, c.Count, bytesFmt(c.Size))
	}
	fmt.Fprintf(w, "</table>\n")
}

func (srv *Server) serveSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}

	srv.sessionsMu.RLock()
	// Make a copy of all session data (excluding the mutex).
	sessions := make([]*sessionData, 0, len(srv.sessions))
	for _, v := range srv.sessions {
		v.mu.Lock()
		sessions = append(sessions, &sessionData{
			expiry:        v.expiry,
			globalNSWrite: v.globalNSWrite,
			claims:        v.claims,
			stats:         v.stats,
		})
		v.mu.Unlock()
	}
	srv.sessionsMu.RUnlock()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<html><body><h1>gocached sessions</h1>\n")
	fmt.Fprintf(w, "<p>JWT issuer: %s</p>\n", srv.JWTIssuer)
	fmt.Fprintf(w, "<p>JWT claims required: %v</p>\n", srv.JWTClaims)
	fmt.Fprintf(w, "<p>JWT global write claims required: %v</p>\n", srv.GlobalJWTClaims)
	fmt.Fprintf(w, "<p>Number of sessions: %d</p>\n", len(sessions))

	fmt.Fprintf(w, "<table border='1' cellpadding=5>\n")
	fmt.Fprintf(w, "<tr><th>Last used</th><th>Expiry time</th><th>Global write</th><th>Stats</th><th>Claims</th></tr>\n")
	slices.SortFunc(sessions, func(a, b *sessionData) int {
		return a.stats.LastUsed.Compare(b.stats.LastUsed)
	})
	for _, d := range slices.Backward(sessions) {
		lastUsed := "never"
		if !d.stats.LastUsed.IsZero() {
			lastUsed = durFmt(time.Since(d.stats.LastUsed)) + " ago"
		}
		statsJSON, _ := json.MarshalIndent(d.stats, "", "  ")
		claimsJSON, _ := json.MarshalIndent(d.claims, "", "  ")
		fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td><td>%v</td><td><pre>%s</pre></td><td><pre>%s</pre></td></tr>\n",
			lastUsed, d.expiry.Format(time.RFC3339), d.globalNSWrite, statsJSON, claimsJSON)
	}
	fmt.Fprintf(w, "</table>\n")
}

// expvarCounterMetric is a Prometheus counter metric backed by an expvar.Int.
type expvarCounterMetric struct {
	desc *prometheus.Desc
	v    *expvar.Int
}

var _ prometheus.Metric = (*expvarCounterMetric)(nil)

func (m *expvarCounterMetric) Desc() *prometheus.Desc { return m.desc }

func (m *expvarCounterMetric) Write(out *dto.Metric) error {
	val := float64(m.v.Value())
	out.Counter = &dto.Counter{Value: &val}
	return nil
}

// expvarGaugeMetric is a Prometheus gauge metric backed by an expvar.Int.
type expvarGaugeMetric struct {
	desc *prometheus.Desc
	v    *expvar.Int
}

var _ prometheus.Metric = (*expvarGaugeMetric)(nil)

func (m *expvarGaugeMetric) Desc() *prometheus.Desc { return m.desc }

func (m *expvarGaugeMetric) Write(out *dto.Metric) error {
	val := float64(m.v.Value())
	out.Gauge = &dto.Gauge{Value: &val}
	return nil
}

// singleMetricCollector is a Prometheus collector that collects a single metric.
type singleMetricCollector struct {
	metric prometheus.Metric
}

var _ prometheus.Collector = singleMetricCollector{}

func (c singleMetricCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.metric.Desc() }
func (c singleMetricCollector) Collect(ch chan<- prometheus.Metric) { ch <- c.metric }
