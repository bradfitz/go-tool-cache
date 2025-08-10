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
package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	_ "modernc.org/sqlite"
)

// smallObjectSize is the maximum size of an object that we store inline in the
// database, rather than on disk. Empirically, about half of objects are 1KB or
// smaller.
const smallObjectSize = 1 << 10

var (
	dir     = flag.String("cache-dir", "", "cache directory, if empty defaults to <UserCacheDir>/gocached")
	verbose = flag.Bool("verbose", false, "be verbose")
	listen  = flag.String("listen", ":31364", "listen address")
)

func main() {
	flag.Parse()
	if *dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "gocached")
		log.Printf("Defaulting to cache dir %v ...", d)
		*dir = d
	}
	if err := os.MkdirAll(*dir, 0755); err != nil {
		log.Fatal(err)
	}

	srv, err := newServer(*dir)
	if err != nil {
		log.Fatalf("newServer: %v", err)
	}
	srv.verbose = *verbose

	log.Printf("gocached listening on %s ...", *listen)
	log.Fatal(http.ListenAndServe(*listen, srv))
}

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
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

func newServer(dir string) (*server, error) {
	db, err := openDB(dir)
	if err != nil {
		return nil, fmt.Errorf("openDB: %w", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)

	srv := &server{
		db:   db,
		dir:  dir,
		logf: log.Printf,
	}
	srv.registerMetrics(reg)

	srv.metricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog: log.Default(),
	})

	return srv, nil
}

func (s *server) registerMetrics(reg *prometheus.Registry) {
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

type server struct {
	db             *sql.DB
	dir            string // for SQLite DB + large blobs
	verbose        bool
	logf           func(format string, args ...any)
	clock          func() time.Time // if non-nil, alternate time.Now for testing
	metricsHandler http.Handler

	// Metrics
	ActiveGets     expvar.Int `type:"gauge" name:"active_gets" help:"currently pending get requests; should usually be zero"`
	Gets           expvar.Int `type:"counter" name:"gets" help:"total number of gocache get requests"` // gets = getHits + getErrs + implicit misses
	GetBytes       expvar.Int `type:"counter" name:"get_bytes" help:"total bytes fetched from gocache gets that were cache hits"`
	GetHits        expvar.Int `type:"counter" name:"get_hits" help:"total number of successful gocache get requests"`
	GetAccessBumps expvar.Int `type:"counter" name:"get_access_bumps" help:"number of times a get request updated the access time of object"`
	GetHitsInline  expvar.Int `type:"counter" name:"get_hits_inline" help:"cache hits served from inline database storage (small objects)"`
	GetErrs        expvar.Int `type:"counter" name:"get_errs" help:"number of gocache get request errors"`
	Puts           expvar.Int `type:"counter" name:"puts" help:"total number of gocache put requests"`
	PutsBytes      expvar.Int `type:"counter" name:"put_bytes" help:"total bytes added from gocache puts"`
	PutsInline     expvar.Int `type:"counter" name:"put_inline" help:"subset of gocached_puts that were stored inline (small objects)"`
}

func (s *server) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.verbose {
		s.logf("ServeHTTP: %s %s", r.Method, r.RequestURI)
	}
	if r.Method == "PUT" {
		s.handlePut(w, r)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/action/"):
		s.handleGetAction(w, r)
	case r.URL.Path == "/":
		io.WriteString(w, "gocached")
	case r.URL.Path == "/metrics":
		s.metricsHandler.ServeHTTP(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
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

func (s *server) handleGetAction(w http.ResponseWriter, r *http.Request) {
	s.ActiveGets.Add(1)
	defer s.ActiveGets.Add(-1)

	s.Gets.Add(1)
	ctx := r.Context()

	httpErr := func(msg string, code int) {
		http.Error(w, msg, code)
		s.GetErrs.Add(1)
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
	err := s.db.QueryRow(
		"SELECT b.SHA256, b.BlobSize, b.SmallData, a.AltOutputID, a.AccessTime FROM Actions a, Blobs b WHERE a.NameSpaceID = ? AND a.ActionID = ? AND a.BlobID = b.BlobID",
		namespaceID, actionID).Scan(
		&sha256hex, &size, &smallData, &altObjectID, &accessTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s.logf("QueryRow error: %v", err)
		httpErr("Query: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// If it's been more than a day since the last access, update the access time.
	// This is similar to the Linux "relatime" behavior.
	now := s.now().Unix()
	if accessTime < now-relAtimeSeconds {
		_, err := s.db.Exec("UPDATE Actions SET AccessTime = ? WHERE ActionID = ?", now, actionID)
		if err != nil {
			s.logf("Update AccessTime error: %v", err)
			httpErr("internal server error", http.StatusInternalServerError)
			return
		}
		s.GetAccessBumps.Add(1)
	}

	s.GetHits.Add(1)

	outputID := cmp.Or(altObjectID, sha256hex)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(size))
	w.Header().Set("Go-Output-Id", outputID)

	if r.Method == "HEAD" || size == 0 {
		return
	}

	if smallData.Valid {
		// For small outputs stored inline in the database, we can return them directly.
		s.GetHitsInline.Add(1)
		s.GetBytes.Add(size)
		io.WriteString(w, smallData.String)
		return
	}

	// Otherwise, for large objects that we know about, we can try to get them
	// from our local disk or a peer.

	rc, err := s.getObjectFromDiskOrPeer(ctx, sha256hex)
	if err != nil {
		httpErr(err.Error(), http.StatusInternalServerError)
		return
	}
	if rc == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.GetBytes.Add(size)
	defer rc.Close()
	io.Copy(w, rc)
}

// getObjectFromDiskOrPeer retrieves the object for the given actionID, either
// from disk or a peer. This is used after a local DB lookup discovers the content
// exists but is not stored in SQLite.
//
// It returns (nil, nil) on miss.
func (s *server) getObjectFromDiskOrPeer(ctx context.Context, sha256hex string) (rc io.ReadCloser, err error) {
	if len(sha256hex) != sha256.Size*2 {
		return nil, fmt.Errorf("invalid sha256hex %q", sha256hex)
	}
	diskPath := filepath.Join(s.dir, sha256hex[:2], sha256hex)
	f, err := os.Open(diskPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	if err == nil {
		return f, nil
	}

	// TODO(bradfitz): search peers, S3, etc.
	// For now, just return nil, nil on miss.
	return nil, nil
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request) {
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sha256hex := fmt.Sprintf("%x", hasher.Sum(nil))
	blobSize := r.ContentLength

	var blobID int64
	err := s.db.QueryRow(`INSERT INTO Blobs (SHA256, BlobSize, SmallData)
		VALUES (?, ?, ?)
		ON CONFLICT(SHA256) DO UPDATE SET SHA256=excluded.SHA256
		RETURNING BlobID;
`, sha256hex, blobSize, smallData).Scan(&blobID)
	if err != nil {
		s.logf("Blobs insert error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert or update the action in the database.
	nowUnix := s.now().Unix()
	altObjectID := ""
	namespace := 0 // global for now; TODO(bradfitz): support namespaces
	if sha256hex != outputID {
		altObjectID = outputID
	}
	_, err = s.db.Exec(`INSERT OR IGNORE INTO Actions (NamespaceID, ActionID, BlobID, AltOutputID, CreateTime, AccessTime)
	VALUES (?, ?, ?, ?, ?, ?)`,
		namespace,
		actionID,
		blobID,
		altObjectID,
		nowUnix,
		nowUnix,
	)
	if err != nil {
		s.logf("Actions insert error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.Puts.Add(1)
	s.PutsBytes.Add(r.ContentLength)
	if smallData != nil {
		s.PutsInline.Add(1)
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) writeDiskBlob(size int64, r io.Reader) (err error) {
	nowUnix := s.now().Unix()
	tf, err := os.CreateTemp(s.dir, fmt.Sprintf("upload-%d-*", nowUnix))
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
	hex := fmt.Sprintf("%02x", hasher.Sum(nil))
	dir := filepath.Join(s.dir, hex[:2])
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	target := filepath.Join(dir, hex)
	return os.Rename(tf.Name(), target)
}

// sha256OfEmpty is the SHA-256 hash of an empty string, used as a well-known
// value in SQLite to store bytes, as it's common. We store it in SQLite
// as "wk0" (for "well known 0") to save space, as it's common.
const sha256OfEmpty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

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
