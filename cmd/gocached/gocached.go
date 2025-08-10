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
	"context"
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
	"strings"
	"time"

	"github.com/bradfitz/go-tool-cache/cachers"
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

	log.Fatal(http.ListenAndServe(*listen, srv))
}

func newServer(dir string) (*server, error) {
	db, err := openDB(dir)
	if err != nil {
		return nil, fmt.Errorf("openDB: %w", err)
	}
	dc := &cachers.DiskCache{Dir: dir}
	return &server{
		db:   db,
		disk: dc,
		logf: log.Printf,
	}, nil
}

const schemaVersion = 1

const schema = `
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS Actions (
  ActionID     TEXT    NOT NULL PRIMARY KEY,
  OutputID     TEXT    NOT NULL,
  OutputSize   INTEGER NOT NULL, -- bytes of the output (even if stored off-DB)
  CreateTime   INTEGER NOT NULL, -- unix sec when inserted (locally or on a peer)
  AccessTime   INTEGER NOT NULL, -- unix sec of last access
  InlineOutput BLOB, -- optional inline output value (e.g. for small output)

  CHECK (ActionID = lower(ActionID)),
  CHECK (OutputID = lower(OutputID)),
  CHECK (ActionID GLOB '[0-9a-f]*'),
  CHECK (OutputID GLOB '[0-9a-f]*'),
  CHECK (OutputSize >= 0),
  CHECK (CreateTime >= 0),
  CHECK (AccessTime >= 0),
  CHECK (InlineOutput IS NULL OR length(InlineOutput) = OutputSize)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_actions_access ON Actions(AccessTime, OutputSize);
`

func openDB(dbDir string) (*sql.DB, error) {
	dbPath := filepath.Join(dbDir, fmt.Sprintf("gocached-v%d.db", schemaVersion))
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

type server struct {
	db      *sql.DB
	disk    *cachers.DiskCache // for large outputs only
	verbose bool
	logf    func(format string, args ...any)
	clock   func() time.Time // if non-nil, alternate time.Now for testing

	// Metrics
	gets           expvar.Int // gets = getHits + getErrs + implicit misses
	getBytes       expvar.Int
	getHits        expvar.Int
	getAccessBumps expvar.Int // number of times we updated the AccessTime
	getHitsInline  expvar.Int // includes getHits; subset of getHits stored in SQLite
	getErrs        expvar.Int // errors from GET requests
	puts           expvar.Int
	putsBytes      expvar.Int
	putsInline     expvar.Int
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
	if r.Method != "GET" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}
	switch {
	case strings.HasPrefix(r.URL.Path, "/action/"):
		s.handleGetAction(w, r)
	case r.URL.Path == "/":
		io.WriteString(w, "gocached")
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
	s.gets.Add(1)
	ctx := r.Context()

	httpErr := func(msg string, code int) {
		http.Error(w, msg, code)
		s.getErrs.Add(1)
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

	var outputID string
	var size int64
	var inlineOutput sql.NullString
	var accessTime int64
	err := s.db.QueryRow("SELECT OutputID, OutputSize, InlineOutput, AccessTime FROM Actions WHERE ActionID = ?", actionID).Scan(&outputID, &size, &inlineOutput, &accessTime)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		httpErr("bad request: missing Want-Object header", http.StatusBadRequest)
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
		s.getAccessBumps.Add(1)
	}

	s.getHits.Add(1)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(size))
	w.Header().Set("Go-Output-Id", outputID)

	if r.Method == "HEAD" || size == 0 {
		return
	}

	if inlineOutput.Valid {
		// For small outputs stored inline in the database, we can return them directly.
		s.getHitsInline.Add(1)
		s.getBytes.Add(size)
		io.WriteString(w, inlineOutput.String)
		return
	}

	// Otherwise, for large objects that we know about, we can try to get them
	// from our local disk or a peer.
	rc, err := s.getObjectFromDiskOrPeer(ctx, actionID)
	if err != nil {
		httpErr(err.Error(), http.StatusInternalServerError)
		return
	}
	if rc == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	s.getBytes.Add(size)
	defer rc.Close()
	io.Copy(w, rc)
}

// getObjectFromDiskOrPeer retrieves the object for the given actionID, either
// from disk or a peer. This is used after a local DB lookup discovers the content
// exists but is not stored in SQLite.
//
// It returns (nil, nil) on miss.
func (s *server) getObjectFromDiskOrPeer(ctx context.Context, actionID string) (rc io.ReadCloser, err error) {
	_, diskPath, diskErr := s.disk.Get(ctx, actionID)
	if diskErr != nil {
		return nil, diskErr
	}
	if diskPath != "" {
		f, err := os.Open(diskPath)
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	// TODO(bradfitz): search peers, S3, etc.
	// For now, just return nil, nil on miss.
	return nil, nil
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	var inline []byte
	if r.ContentLength <= smallObjectSize {
		// Store small objects inline in the database.
		var err error
		inline, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		// For larger objects, we store them on disk.
		_, err := s.disk.Put(ctx, actionID, outputID, r.ContentLength, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Insert or update the action in the database.
	nowUnix := s.now().Unix()
	_, err := s.db.Exec(`
INSERT OR IGNORE INTO Actions (ActionID, OutputID, OutputSize, CreateTime, AccessTime, InlineOutput)
VALUES (?, ?, ?, ?, ?, ?)`,
		actionID, outputID, r.ContentLength, nowUnix, nowUnix, inline)
	if err != nil {
		s.logf("INSERT error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.puts.Add(1)
	s.putsBytes.Add(r.ContentLength)
	if inline != nil {
		s.putsInline.Add(1)
	}

	w.WriteHeader(http.StatusNoContent)
}
