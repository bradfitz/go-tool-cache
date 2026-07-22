// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

// The gocached package provides an HTTP server daemon that go-cacher can hit.
// It does cache tiering and evicts old large things from disk, and can fetch
// metadata and object contents from peer cache servers.
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
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ijwt "github.com/bradfitz/go-tool-cache/gocached/internal/jwt"
	"github.com/bradfitz/go-tool-cache/gocached/logger"
	"github.com/pierrec/lz4/v4"
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

	// lz4CompressThreshold is the minimum blob size at which we lz4-compress
	// before storing on disk. Intentionally two bytes above smallObjectSize so
	// there's a real gap (disk blobs that aren't lz4-compressed), preventing
	// assumptions that the two thresholds are equal while keeping them close.
	lz4CompressThreshold = smallObjectSize + 2

	// tokenPrefix is the prefix for all gocached access tokens.
	tokenPrefix = "gocached-token-"

	// gocachedAudience is the audience we require JWTs to have. Could be
	// configurable in future, but for now just needs to be specific to gocached.
	gocachedAudience = "gocached"
)

const schemaVersion = 4

// walJournalSizeLimit caps the on-disk WAL size after a successful
// checkpoint. It is set as a per-connection PRAGMA so that even SQLite's
// built-in PASSIVE autocheckpoint, which would otherwise leave the file at
// its high-water mark, truncates the WAL down to this size. Without it the
// WAL can grow without bound under continuous write traffic and never
// shrink, even when frames are being checkpointed in place.
const walJournalSizeLimit = 1 << 30 // 1 GiB

// checkpointInterval is how often [Server.runCheckpointLoop] runs a TRUNCATE
// checkpoint in the background. SQLite's autocheckpoint only runs PASSIVE
// checkpoints (which reuse WAL space in place but never shrink the file on
// disk past walJournalSizeLimit); a periodic explicit TRUNCATE is what
// actually keeps the file small in steady state.
const checkpointInterval = time.Minute

// dbSizeMetricsInterval is how often [Server.runDBSizeMetricsLoop] re-stats
// the SQLite files to update the size gauges. It is intentionally shorter
// than checkpointInterval so the gauge sees the WAL grow between checkpoints,
// not just snap back to zero each minute.
const dbSizeMetricsInterval = 15 * time.Second

// defaultShardPrefixLen is the [Server.shardPrefixLen] used when the option
// is not set (zero value). 2 yields 256 shards ("00".."ff"), which keeps
// individual scans fast even at hundreds of millions of rows.
const defaultShardPrefixLen = 2

// minShardPrefixLen is the minimum [Server.shardPrefixLen]. 1 yields 16
// shards. Going to 0 (1 shard) would defeat the purpose of sharded
// statistics, so we reject it.
const minShardPrefixLen = 1

// maxShardPrefixLen is the maximum [Server.shardPrefixLen]. 4 hex chars ->
// 65,536 shards is more than we ever expect to need; higher values would
// make the stats loop spend most of its time on shards with little or no
// data, and would also blow up the size of the BlobShardStats table.
const maxShardPrefixLen = 4

// shardStatsMinInterval sets a defensive floor on how long [Server.runShardStatsLoop] will
// sleep between iterations to ensure it never spins in a tight loop. [shardStalenessTarget]
// dictates the interval in steady state.
const shardStatsMinInterval = 2 * time.Second

// cleanupTickInterval is how often [Server.runCleanLoop] wakes up to check
// whether the cache is over its limits and, if so, evict one batch of
// Actions. It's tight on purpose: each tick does at most cleanupBatchSize
// deletes so the write transaction is short.
const cleanupTickInterval = 1 * time.Second

// cleanupBatchSize is the maximum number of Actions deleted in a single
// [Server.evictOldestActions] call. Small enough that the write
// transaction (including any file removals) holds the SQLite write lock
// for only a brief moment, large enough that steady-state eviction keeps
// up with realistic write traffic at ~cleanupBatchSize/cleanupTickInterval.
const cleanupBatchSize = 200

// shardStalenessTarget bounds how old any single shard's cohort histogram is
// allowed to get. The stats loop sleeps until the oldest shard reaches this
// age, then scans it; on a quiet cache that means scans naturally settle to
// ~numShards/shardStalenessTarget per second instead of running continuously.
// 10m drift on the 1d cohort is well under 1% error, which is negligible
// compared to the day-granularity cohort cutoffs we record.
const shardStalenessTarget = 10 * time.Minute

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
  BlobID            INTEGER PRIMARY KEY AUTOINCREMENT,
  SHA256            TEXT NOT NULL,
  StoredSize        INTEGER NOT NULL, -- bytes stored: len(SmallData) if inline, file size on disk (possibly lz4)
  UncompressedSize  INTEGER NOT NULL, -- original uncompressed content size
  SmallData         BLOB,

  CHECK (SmallData IS NULL OR length(SmallData) = StoredSize)
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_blobs_sha256 ON Blobs(SHA256);

CREATE TABLE IF NOT EXISTS Namespaces (
  NamespaceID INTEGER PRIMARY KEY AUTOINCREMENT,
  Namespace   TEXT NOT NULL UNIQUE
) STRICT;

-- BlobShardStats persists per-shard usage histograms across restarts so
-- the server can serve PUTs with accurate global usage from right at startup,
-- without blocking for minutes computing stats before serving can begin.
-- There's one row per SHA256 prefix shard, refreshes one at a time in the background.
CREATE TABLE IF NOT EXISTS BlobShardStats (
  Prefix     TEXT PRIMARY KEY, -- "01".."ff" by default for shardPrefixLen=2; must be lowercase hex
  ScannedAt  INTEGER NOT NULL, -- unix seconds
  StatsJSON  TEXT NOT NULL     -- JSON-encoded [usageStats] of that shard
) STRICT;
`

func openDB(dbDir string) (*sql.DB, error) {
	dbPath := filepath.Join(dbDir, fmt.Sprintf("gocached-v%d.db", schemaVersion))

	// If the v4 DB doesn't exist but v3 does, migrate it.
	if schemaVersion == 4 {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			v3Path := filepath.Join(dbDir, "gocached-v3.db")
			if _, err := os.Stat(v3Path); err == nil {
				if err := migrateV3ToV4(dbDir, v3Path, dbPath); err != nil {
					return nil, fmt.Errorf("migrating v3→v4: %w", err)
				}
			}
		}
	}

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_size_limit(%d)",
		dbPath, walJournalSizeLimit)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	numConns := min(runtime.NumCPU(), 4)
	db.SetMaxOpenConns(numConns)
	db.SetMaxIdleConns(numConns)
	db.SetConnMaxLifetime(0) // no limit
	var ddl string
	err = db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='Namespaces'`).Scan(&ddl)
	if err == nil && strings.Contains(ddl, "lower(Namespace)") {
		// Drop the Namespaces table _only_ if it has the lowercase constraint so it
		// can be recreated without it.
		if _, err := db.Exec(`DROP TABLE Namespaces`); err != nil {
			return nil, fmt.Errorf("dropping Namespaces lowercase constraint: %w", err)
		}
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

// migrateV3ToV4 copies the v3 database to a temp file, runs the v3→v4
// migration on it, then atomically renames it to the v4 path. This is
// crash-safe: if anything fails, the original v3 DB is untouched.
func migrateV3ToV4(dbDir, v3Path, v4Path string) error {
	// Checkpoint the v3 WAL so the main .db file is self-contained before
	// we copy it. Without this, data sitting in gocached-v3.db-wal would
	// be silently lost.
	v3DB, err := sql.Open("sqlite", "file:"+v3Path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("opening v3 for checkpoint: %w", err)
	}
	_, err = v3DB.Exec("PRAGMA wal_checkpoint(TRUNCATE)")
	v3DB.Close()
	if err != nil {
		return fmt.Errorf("checkpointing v3 WAL: %w", err)
	}

	// Copy v3 to a temp file in the same directory (for same-filesystem rename).
	tmpFile, err := os.CreateTemp(dbDir, "gocached-v4-migrate-*.db")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	defer func() {
		// Clean up temp file on failure.
		os.Remove(tmpPath)
	}()

	src, err := os.Open(v3Path)
	if err != nil {
		tmpFile.Close()
		return err
	}
	if _, err := io.Copy(tmpFile, src); err != nil {
		src.Close()
		tmpFile.Close()
		return err
	}
	src.Close()
	if err := tmpFile.Close(); err != nil {
		return err
	}

	// Open the temp DB and run the migration.
	tmpDB, err := sql.Open("sqlite", "file:"+tmpPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	for _, stmt := range []string{
		`ALTER TABLE Blobs RENAME COLUMN BlobSize TO StoredSize`,
		`ALTER TABLE Blobs ADD COLUMN UncompressedSize INTEGER NOT NULL DEFAULT 0`,
		`UPDATE Blobs SET UncompressedSize = StoredSize WHERE UncompressedSize = 0`,
		`PRAGMA wal_checkpoint(TRUNCATE)`,
	} {
		if _, err := tmpDB.Exec(stmt); err != nil {
			tmpDB.Close()
			return fmt.Errorf("migration statement %q: %w", stmt, err)
		}
	}
	if err := tmpDB.Close(); err != nil {
		return err
	}

	// Atomic rename on same filesystem.
	if err := os.Rename(tmpPath, v4Path); err != nil {
		return err
	}
	return nil
}

// start initializes the server, including defaults and background goroutines.
func (srv *Server) start() error {
	srv.shutdownCtx, srv.shutdownCancel = context.WithCancel(srv.shutdownCtx)
	if srv.dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			return fmt.Errorf("getting user cache dir: %w", err)
		}
		srv.dir = filepath.Join(d, "gocached")
		srv.logf("Defaulting to cache dir %v ...", srv.dir)
	}
	if err := os.MkdirAll(srv.dir, 0750); err != nil {
		return fmt.Errorf("creating cache dir: %w", err)
	}
	if srv.sqliteDir == "" {
		srv.sqliteDir = srv.dir
	} else if err := os.MkdirAll(srv.sqliteDir, 0750); err != nil {
		return fmt.Errorf("creating sqlite dir: %w", err)
	}
	if srv.hotDir != "" {
		if srv.hotDir == srv.dir {
			return fmt.Errorf("hot dir %q must differ from cache dir", srv.hotDir)
		}
		if srv.hotCap <= 0 {
			return fmt.Errorf("hot capacity must be positive when hot dir is set")
		}
		if err := os.MkdirAll(srv.hotDir, 0750); err != nil {
			return fmt.Errorf("creating hot dir: %w", err)
		}
		srv.hot = newHotIndex()
	}

	// Pending PUT metadata is memory-only, so any spool files left over
	// from a previous process are unreachable; delete them. This must
	// happen before the hot tier scan starts walking hotDir, so the scan
	// never sees the directory mid-removal.
	queueDir := filepath.Join(srv.dir, putQueueDirName)
	if srv.hotDir != "" {
		queueDir = filepath.Join(srv.hotDir, putQueueDirName)
	}
	if err := os.RemoveAll(queueDir); err != nil {
		return fmt.Errorf("cleaning put-queue dir: %w", err)
	}
	if err := os.MkdirAll(queueDir, 0750); err != nil {
		return fmt.Errorf("creating put-queue dir: %w", err)
	}
	srv.putq = newPutQueue(srv, queueDir)

	if srv.hot != nil {
		// Populate the index asynchronously: scanning millions of files can
		// take a while, and reads can be served from existing hot files right
		// away. Until the scan completes and marks the index ready, nothing
		// new is written into the hot tier.
		go func() {
			if err := srv.hot.scan(srv.shutdownCtx, srv.hotDir, srv.logf); err != nil {
				srv.logf("hot tier: scan failed; hot tier writes remain disabled: %v", err)
				return
			}
			srv.evictHotIfOver()
		}()
	}
	db, err := openDB(srv.sqliteDir)
	if err != nil {
		return fmt.Errorf("openDB: %w", err)
	}
	srv.db = db

	// Run a TRUNCATE checkpoint up front, before any other reader can pin a
	// snapshot. If the WAL on disk is large (e.g. from a prior version of
	// gocached that lacked the periodic checkpointer), this is what actually
	// shrinks it.
	ckCtx, ckCancel := context.WithTimeout(srv.shutdownCtx, 2*time.Minute)
	if busy, log, ckpt, err := srv.checkpointTruncate(ckCtx); err != nil {
		srv.logf("startup wal_checkpoint(TRUNCATE) error: %v", err)
	} else {
		srv.logf("startup wal_checkpoint(TRUNCATE): busy=%d log=%d ckpt=%d", busy, log, ckpt)
	}
	ckCancel()

	if srv.shardPrefixLen == 0 {
		srv.shardPrefixLen = defaultShardPrefixLen
	}
	if srv.shardPrefixLen < minShardPrefixLen || srv.shardPrefixLen > maxShardPrefixLen {
		return fmt.Errorf("shardPrefixLen %d out of range [%d, %d]",
			srv.shardPrefixLen, minShardPrefixLen, maxShardPrefixLen)
	}

	// Compute the cohort cutoffs once so every shard scan agrees. maxAge is
	// added to the set if it's not already a standard duration so cleanup can
	// read its bucket directly out of the aggregate.
	srv.durs = computeDurs(srv.maxAge)

	// Allocate the dead-reckoning delta state. One entry per shard, indexed
	// by the integer value of the SHA256 hex prefix.
	srv.shardDeltas = make([]shardDelta, srv.numShards())

	// Create the shardScanDuration histogram before loadShardStats so any
	// QueryDuration values persisted by a previous run get replayed into
	// the histogram immediately on restart, rather than waiting for the
	// stats loop to repopulate it from fresh scans.
	srv.shardScanDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "gocached_shard_scan_duration_seconds",
		Help:    "wall time of each per-shard usage-stats SQL scan",
		Buckets: prometheus.DefBuckets,
	})

	// Buckets span roughly 1ms to 2s; cache hits should land in the low
	// milliseconds, large PUTs and disk-backed GETs land higher up.
	reqLatencyBuckets := []float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2}
	srv.getDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gocached_get_duration_seconds",
		Help:    "wall time of each cache get request, labeled by storage path: hot (hot tier disk), disk, inline, pending (put-queue), none (HEAD request), or error; and type: get (hit), miss (404), or error",
		Buckets: reqLatencyBuckets,
	}, []string{"storage", "type"})
	srv.putDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gocached_put_duration_seconds",
		Help:    "wall time of each cache put request, labeled by storage path: disk, inline, or error; and type: put (success), dup, or error",
		Buckets: reqLatencyBuckets,
	}, []string{"storage", "type"})
	blobSizeBuckets := []float64{1}
	for i := 6; i <= 30; i += 2 {
		bucket := 1 << i
		blobSizeBuckets = append(blobSizeBuckets, float64(bucket))
	}
	srv.blobSize = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gocached_blob_size_bytes",
		Help:    "object size in bytes transmitted on the wire (whether compressed or uncompressed) for successful GETs and PUTs, labeled by storage: hot, disk, inline, or error; and type: get, put, or dup",
		Buckets: blobSizeBuckets,
	}, []string{"storage", "type"})

	// Fill the namespace ID cache.
	rows, err := srv.db.Query("SELECT NamespaceID, Namespace FROM Namespaces")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var ns string
		if err := rows.Scan(&id, &ns); err != nil {
			return err
		}
		srv.namespaces[Namespace(ns)] = id
	}
	if err := rows.Err(); err != nil {
		return err
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
	srv.registerMetrics(reg)
	reg.MustRegister(srv.shardScanDuration, srv.getDuration, srv.putDuration, srv.blobSize)

	// Per-scrape gauges for shard stats loop health. GaugeFunc recomputes on
	// every Prometheus scrape, so the values stay fresh between scans
	// (an expvar.Int gauge would only update once per shard scan, and
	// "oldest age" would lag the wall clock between scans).
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gocached_shard_stats_unscanned",
			Help: "number of shards with no cached usage stats; nonzero means the stats loop has not yet covered the full shard space (e.g. just after first deploy of this code)",
		},
		func() float64 {
			srv.shardStatsMu.Lock()
			defer srv.shardStatsMu.Unlock()
			return float64(srv.numShards() - len(srv.shardStats))
		},
	))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gocached_shard_stats_oldest_age_seconds",
			Help: "age in seconds of the oldest cached shard scan, or 0 if no shard has been scanned yet; alert when this exceeds shardStalenessTarget by some margin to catch the stats loop falling behind",
		},
		func() float64 {
			_, oldest, _ := srv.shardFreshness(srv.now())
			if oldest.IsZero() {
				return 0
			}
			return srv.now().Sub(oldest).Seconds()
		},
	))
	// Dead-reckoning gauges: PUTs/evictions adjust these between shard
	// scans, so they show how much the live state has drifted from the
	// persisted aggregate. Signed: a sustained negative delta means evictions
	// are running ahead of writes, which is fine; a sustained large positive
	// delta hints the stats loop is falling behind write traffic.
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gocached_pending_blob_count",
			Help: "signed pending Action-count delta since the last shard scan; combined with gocached_blob_count this gives the live total",
		},
		func() float64 {
			c, _ := srv.sumShardDeltas()
			return float64(c)
		},
	))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gocached_pending_blob_bytes",
			Help: "signed pending aggregate-bytes delta since the last shard scan; combined with gocached_blob_bytes this gives the live total used to gate maxSize cleanup",
		},
		func() float64 {
			_, b := srv.sumShardDeltas()
			return float64(b)
		},
	))

	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gocached_put_queue_pending",
			Help: "PUTs accepted but whose metadata hasn't been committed to SQLite yet; should return to zero shortly after a write burst",
		},
		func() float64 {
			c, _ := srv.putq.pendingStats()
			return float64(c)
		},
	))
	reg.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "gocached_put_queue_pending_bytes",
			Help: "sum of the reserved sizes of pending PUTs; bounds spool disk usage and reflects backpressure toward its cap",
		},
		func() float64 {
			_, b := srv.putq.pendingStats()
			return float64(b)
		},
	))

	if srv.hot != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "gocached_hot_bytes",
				Help: "bytes currently stored in the hot tier directory; bounded by the configured hot capacity",
			},
			func() float64 { return float64(srv.hot.usageBytes()) },
		))
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "gocached_hot_files",
				Help: "number of blob files currently stored in the hot tier directory",
			},
			func() float64 { return float64(srv.hot.len()) },
		))
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "gocached_hot_ready",
				Help: "1 once the hot tier startup scan has completed and new blobs may be written into the hot tier, else 0; alert if this stays 0 well past startup",
			},
			func() float64 {
				if srv.hot.ready.Load() {
					return 1
				}
				return 0
			},
		))
		reg.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "gocached_hot_scan_running_seconds",
				Help: "how long the hot tier startup scan has currently been running, in seconds; 0 when the scan is not running",
			},
			func() float64 {
				ns := srv.hot.scanStart.Load()
				if ns == 0 {
					return 0
				}
				return time.Since(time.Unix(0, ns)).Seconds()
			},
		))
	}

	srv.metricsHandler = promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog: log.Default(),
	})

	// Seed shardStats + the aggregate from BlobShardStats so usageStats is
	// accurate from the first HTTP request after restart, without a full-table
	// scan. Stale shards are refreshed by runShardStatsLoop in the
	// background; the cleanup loop already runs periodically and will catch
	// up any over-limit state once enough shards have been rescanned.
	if err := srv.loadShardStats(srv.shutdownCtx); err != nil {
		srv.logf("loading persisted shard stats: %v", err)
	}
	srv.logf("gocached: %d/%d shards loaded; usage: %v of limit %v",
		len(srv.shardStats), srv.numShards(), srv.lastUsage.Load().All(), bytesFmt(srv.maxSize))

	if len(srv.jwtIssuers) > 0 {
		srv.jwtValidator = ijwt.NewJWTValidator(srv.logf, gocachedAudience, srv.jwtIssuers)
		if err := srv.jwtValidator.RunUpdateJWKSLoop(srv.shutdownCtx); err != nil {
			return fmt.Errorf("failed to fetch JWKS for JWT validator: %w", err)
		}

		for _, iss := range srv.jwtIssuers {
			srv.logf("gocached: using JWT issuer %q", iss)
		}

		go srv.runCleanSessionsLoop()
	}

	if !srv.disableBackgroundLoops {
		srv.putq.start(srv.shutdownCtx)
		go srv.runCleanLoop()
		go srv.runCheckpointLoop()
		go srv.runDBSizeMetricsLoop()
		go srv.runShardStatsLoop()
	}

	return nil
}

func (srv *Server) registerMetrics(reg *prometheus.Registry) {
	rv := reflect.ValueOf(&srv.m).Elem()
	t := reflect.TypeOf(&srv.m).Elem()
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

// ServerOption configures a gocached Server.
type ServerOption func(*Server)

// WithShutdownCtx sets the context used to signal server shutdown. Defaults to
// context.Background().
func WithShutdownCtx(ctx context.Context) ServerOption {
	return func(srv *Server) {
		srv.shutdownCtx = ctx
	}
}

// WithDir sets the directory where the server stores its data. Defaults to the
// OS user cache directory under $XDG_CACHE_HOME/gocached or equivalent.
func WithDir(dir string) ServerOption {
	return func(srv *Server) {
		srv.dir = dir
	}
}

// WithSQLiteDir sets the directory where the server stores its SQLite
// database. Defaults to the blob directory (see [WithDir]).
func WithSQLiteDir(dir string) ServerOption {
	return func(srv *Server) {
		srv.sqliteDir = dir
	}
}

// WithHotDir enables storage tiering, using dir as a fast storage tier (e.g.
// local NVMe) holding a bounded copy of recently used blobs. The main blob
// directory (see [WithDir]) remains the source of truth containing all blobs;
// files in the hot directory can be deleted at any time. If set,
// [WithHotCapacity] must also be set. Defaults to empty, meaning no tiering.
func WithHotDir(dir string) ServerOption {
	return func(srv *Server) {
		srv.hotDir = dir
	}
}

// WithHotCapacity sets the maximum number of bytes stored in the hot tier
// directory (see [WithHotDir]). When usage exceeds this capacity, the least
// recently used hot files are deleted.
func WithHotCapacity(bytes int64) ServerOption {
	return func(srv *Server) {
		srv.hotCap = bytes
	}
}

// WithVerbose enables verbose logging for the server. Defaults to false.
func WithVerbose(verbose bool) ServerOption {
	return func(srv *Server) {
		srv.verbose = verbose
	}
}

// WithLogf sets a custom logging function for the server. Defaults to
// [log.Printf].
func WithLogf(logf logger.Logf) ServerOption {
	return func(srv *Server) {
		srv.logf = logf
	}
}

// WithMaxSize sets the maximum size of the cache in bytes. Defaults to 0, which
// means no limit.
func WithMaxSize(maxSize int64) ServerOption {
	return func(srv *Server) {
		srv.maxSize = maxSize
	}
}

// WithMaxAge sets the maximum age of objects in the cache. Objects older than
// this duration will be cleaned periodically. Defaults to 0, which means no
// limit.
func WithMaxAge(maxAge time.Duration) ServerOption {
	return func(srv *Server) {
		srv.maxAge = maxAge
	}
}

// WithShardPrefixLen sets the number of hex characters used as each shard key
// in BlobShardStats. The shard count is 16^n. Valid values are 1..4 inclusive
// (16, 256, 4096, or 65536 shards). Passing 0 means "use the default" (2;
// 256 shards), which keeps individual shard scans fast at hundreds of
// millions of rows. Tune up only if a single shard scan becomes too slow
// under continued growth; any other value causes [NewServer] to fail.
func WithShardPrefixLen(n int) ServerOption {
	return func(srv *Server) {
		srv.shardPrefixLen = n
	}
}

// Namespace identifies a logical partition of the cache where each peer is
// equally trusted. Every session is associated with exactly one Namespace to
// which it can read and write; sessions for non-global namespaces also read
// from [GlobalNamespace]. See [WithNamespaceMapping]. It may only contain
// characters from the set [a-zA-Z0-9._~:/@+|=-].
type Namespace string

// GlobalNamespace is a trusted namespace that all sessions can read from. Only
// sessions explicitly mapped to GlobalNamespace can write to it.
const GlobalNamespace Namespace = ""

// WithJWTAuth enables JWT-based authentication for the server. Each issuer
// must be a reachable HTTP(S) server that serves its JWKS via a URL
// discoverable at /.well-known/openid-configuration. JWTs presented for token
// exchange must pass the standard signature/issuer/audience/expiry checks
// against one of these issuers. If [WithNamespaceMapping] is provided, then
// it may still be rejected if the mapping function returns an error for its
// claims. No requests other than token exchange are allowed without
// authentication. It may be called multiple times; issuers accumulate.
func WithJWTAuth(issuers ...string) ServerOption {
	return func(srv *Server) {
		srv.jwtIssuers = append(srv.jwtIssuers, issuers...)
	}
}

// WithNamespaceMapping sets the function that makes policy decisions based on
// a JWT's claims. It is called once per token exchange after the JWT's
// signature and standard claims have been validated. It should return an error
// if the claims are not authorized, and otherwise return which [Namespace] the
// session is allowed to read and write in. See [Namespace] for character set
// constraints. All authorized sessions are allowed to read from the
// [GlobalNamespace] regardless of the namespace returned. Check claims["iss"]
// to switch on per-issuer rules. If JWT auth is enabled but no mapping
// function is provided, all sessions will read and write in the
// [GlobalNamespace].
func WithNamespaceMapping(fn func(claims map[string]any) (Namespace, error)) ServerOption {
	return func(srv *Server) {
		srv.namespaceMapping = fn
	}
}

// NewServer creates and starts a new gocached [Server] that is ready to serve
// requests. It defaults to requiring no authentication and storing its data in
// the OS user cache directory under $XDG_CACHE_HOME/gocached or equivalent.
func NewServer(opts ...ServerOption) (*Server, error) {
	srv := &Server{
		shutdownCtx: context.Background(),
		logf:        log.Printf,
		sessions:    make(map[string]*sessionData),
		namespaces:  make(map[Namespace]int64),
		clock:       time.Now,
	}
	for _, opt := range opts {
		opt(srv)
	}

	if len(srv.jwtIssuers) > 0 && srv.namespaceMapping == nil {
		// If JWT auth is enabled, but not namespace mapping, every session is in
		// the global namespace.
		srv.namespaceMapping = func(claims map[string]any) (Namespace, error) {
			return GlobalNamespace, nil
		}
	}

	err := srv.start()
	if err != nil {
		return nil, err
	}

	return srv, nil
}

// Close shuts down the server, stopping background goroutines and closing the
// database.
func (srv *Server) Close() error {
	srv.shutdownCancel()

	// Settle any PUTs the background pipeline hadn't finished: their
	// metadata exists only in memory until flushed.
	var err error
	if drainErr := srv.drainPendingPuts(); drainErr != nil {
		err = fmt.Errorf("draining pending puts: %w", drainErr)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if srv.updateAccessTimeStmt != nil {
		err = errors.Join(err, srv.updateAccessTimeStmt.Close())
	}
	if srv.writeConn != nil {
		err = errors.Join(err, srv.writeConn.Close())
	}

	// Final TRUNCATE checkpoint so the WAL doesn't linger on disk past
	// shutdown. Use context.Background because srv.shutdownCtx has already
	// been canceled.
	ckCtx, ckCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if _, _, _, ckErr := srv.checkpointTruncate(ckCtx); ckErr != nil {
		err = errors.Join(err, fmt.Errorf("final wal_checkpoint: %w", ckErr))
	}
	ckCancel()

	return errors.Join(err, srv.db.Close())
}

// Server implements a gocached server. Use [NewServer] to create and start a
// valid instance.
type Server struct {
	db             *sql.DB
	dir            string // for large blobs (and the SQLite DB, unless sqliteDir is set)
	sqliteDir      string // for the SQLite DB; if empty, defaults to dir
	hotDir         string // if non-empty, fast storage tier for a bounded copy of hot blobs
	hotCap         int64  // maximum bytes in hotDir; must be positive if hotDir is set
	hot            *hotIndex
	putq           *putQueue
	verbose        bool
	logf           logger.Logf
	clock          func() time.Time // if non-nil, alternate time.Now for testing
	metricsHandler http.Handler
	maxSize        int64         // maximum size of the cache in bytes; 0 means no limit
	maxAge         time.Duration // maximum age of objects; 0 means no limit
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	jwtValidator     *ijwt.Validator                                // nil unless jwtIssuers is non-empty
	jwtIssuers       []string                                       // accepted issuer URLs
	namespaceMapping func(claims map[string]any) (Namespace, error) // required when jwtIssuers is non-empty

	mu               sync.RWMutex            // guards following fields in this block
	sessions         map[string]*sessionData // maps access token -> session data.
	accessDirty      map[actionKey]int64     // action -> accessTime
	accessFlushTimer *time.Timer             // nil if no flush is scheduled
	namespaces       map[Namespace]int64     // cached namespace string -> NamespaceID

	// sqliteWriteMu serializes access to SQLite. In theory the SQLite driver
	// should serialize access with our 5000ms busy timeout, but empirically we
	// sometimes seen DB busy errors. Just serialize it explicitly out of
	// laziness for now.
	//
	// Lock ordering: sqliteWriteMu before mu. putQueue.mu is a leaf:
	// nothing else is acquired while holding it.
	sqliteWriteMu        sync.Mutex
	writeConn            *sql.Conn // nil until first used; single connection for writes
	updateAccessTimeStmt *sql.Stmt // nil until first used; for updating access times on writeConn

	// lastUsage holds the most recent aggregate of per-shard stats, recomputed
	// after every shard scan. It is the source of truth for /usage,
	// BlobCount/BlobBytes gauges, and cleanup decisions; nothing on the hot
	// path runs a full-table scan.
	lastUsage atomic.Pointer[usageStats]

	// durs is the set of cohort cutoffs (in standardDurs order, plus maxAge if
	// set and not already standard) used by all per-shard scans. Computed once
	// in start. Persisted shard stats whose cohort set doesn't match these are
	// discarded at load time and re-scanned by the stats loop.
	durs []time.Duration

	// shardPrefixLen is the number of hex characters in each shard key. Set
	// via [WithShardPrefixLen]; defaults to [defaultShardPrefixLen] when 0,
	// and must be in [minShardPrefixLen, maxShardPrefixLen] inclusive once
	// [Server.start] has run. The derived shard count is 16^shardPrefixLen,
	// exposed via [Server.numShards].
	shardPrefixLen int

	// disableBackgroundLoops, when true, skips the start of every periodic
	// goroutine (shard stats, cleanup, checkpoint, DB-size metrics).
	// Test-only via withoutBackgroundLoops because the mocked clock collides
	// scannedAt across concurrent scans; production never sets this.
	disableBackgroundLoops bool

	// shardStatsMu guards shardStats and serializes recomputeAggregateLocked.
	shardStatsMu sync.Mutex
	shardStats   map[shardPrefix]*shardSnapshot

	// shardDeltas is dead-reckoning state: each entry tracks (count, bytes)
	// added or removed for that shard since its last persisted scan, so
	// cleanupTick can react to write bursts faster than shardStalenessTarget.
	// PUTs increment; evictions decrement; scanAndPersistShard subtracts the
	// (newStats - oldStats) diff so the delta only carries changes that
	// aren't already in the persisted shard. Allocated once in start
	// (len == numShards()) so per-shard updates are an addressable struct
	// with its own mutex; no allocation per PUT.
	shardDeltas []shardDelta

	// shardScanDuration observes the wall time of each per-shard SQL scan.
	// Combined with the count of shards, the histogram shows whether any one
	// shard scan is pathologically slow (e.g. a hot range with millions of
	// entries while others are nearly empty). Registered manually in start
	// because it isn't an expvar.Int and so doesn't fit the m-struct
	// reflection.
	shardScanDuration prometheus.Histogram

	// getDuration, putDuration, and blobSize observe the wall time and transfer
	// size of each /action GET and PUT request handler, labeled by storage and
	// type. type is one of "get" (hit), "miss" (404), "put", "dup", or "error".
	// storage is one of "disk", "inline", "none", or "error".
	getDuration *prometheus.HistogramVec
	putDuration *prometheus.HistogramVec
	blobSize    *prometheus.HistogramVec

	// Metrics. Exported fields for reflection, but within a private struct
	// field to control the gocached Server API surface.
	m struct {
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
		EvictedActions expvar.Int `type:"counter" name:"evicted_actions" help:"number of Actions evicted from the cache by the cleanup loop"`
		EvictedBlobs   expvar.Int `type:"counter" name:"evicted_blobs" help:"number of Blobs evicted from the cache; a Blob is evicted when its last referencing Action is evicted"`
		EvictedBytes   expvar.Int `type:"counter" name:"evicted_bytes" help:"number of bytes reclaimed by evicting Blobs from the cache"`
		Sessions       expvar.Int `type:"gauge" name:"sessions" help:"number of active authenticated sessions"`
		Auths          expvar.Int `type:"counter" name:"auth_attempts" help:"number of successful token exchanges"`
		AuthErrs       expvar.Int `type:"counter" name:"auth_errs" help:"number of failed token exchanges"`

		SQLiteDataBytes expvar.Int `type:"gauge" name:"sqlite_data_bytes" help:"size in bytes of the SQLite main database file on disk"`
		SQLiteWALBytes  expvar.Int `type:"gauge" name:"sqlite_wal_bytes" help:"size in bytes of the SQLite WAL file on disk; should stay bounded near walJournalSizeLimit"`

		HotHits          expvar.Int `type:"counter" name:"hot_hits" help:"GETs served from the hot tier"`
		HotMisses        expvar.Int `type:"counter" name:"hot_misses" help:"GETs that were not served from the hot tier"`
		HotReadErrs      expvar.Int `type:"counter" name:"hot_read_errs" help:"hot tier opens that failed for a reason other than the file not existing; reads fall back to the main blob directory"`
		HotPromotions    expvar.Int `type:"counter" name:"hot_promotions" help:"blobs copied into the hot tier after a hot tier miss"`
		HotPromotionErrs expvar.Int `type:"counter" name:"hot_promotion_errs" help:"failed attempts to copy a blob into the hot tier"`
		HotWriteErrs     expvar.Int `type:"counter" name:"hot_write_errs" help:"failed hot tier writes during PUT; the PUT itself still succeeds"`
		HotEvicted       expvar.Int `type:"counter" name:"hot_evicted" help:"files evicted from the hot tier to stay under its capacity"`
		HotEvictedBytes  expvar.Int `type:"counter" name:"hot_evicted_bytes" help:"bytes reclaimed by evicting files from the hot tier"`

		PutQueueBlocked      expvar.Int `type:"counter" name:"put_queue_blocked" help:"PUT requests that had to wait for put-queue backpressure before being admitted"`
		PutQueueCopyErrs     expvar.Int `type:"counter" name:"put_queue_copy_errs" help:"failed attempts to copy a spooled blob into the main blob directory; retried before the PUT is dropped"`
		PutQueueDropped      expvar.Int `type:"counter" name:"put_queue_dropped" help:"pending PUTs abandoned after repeated copy or flush failures; the client saw success but the object was lost"`
		PutQueueFlushes      expvar.Int `type:"counter" name:"put_queue_flushes" help:"metadata batch transactions committed by the put-queue flusher"`
		PutQueueFlushedItems expvar.Int `type:"counter" name:"put_queue_flushed_items" help:"pending PUTs whose metadata was committed by the put-queue flusher"`
		PutQueueFlushDups    expvar.Int `type:"counter" name:"put_queue_flush_dups" help:"subset of put_queue_flushed_items that were duplicates of an already-stored action"`
	}
}

// sessionData corresponds to a specific access token, and is only used if JWT
// auth is enabled.
type sessionData struct {
	expiry      time.Time      // Session valid until.
	namespaceID int64          // The namespace this session writes to. 0 means GlobalNamespace; non-zero sessions also read from 0.
	namespace   Namespace      // Namespace this session writes to, stored for debug.
	claims      map[string]any // Claims from the JWT used to create this session, stored for debug.

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
	return srv.clock()
}

// ServeHTTPDebug serves debug HTTP endpoints. It is unauthenticated, so should
// only be used on a separate debug listener.
func (srv *Server) ServeHTTPDebug(w http.ResponseWriter, r *http.Request) {
	if srv.verbose {
		srv.logf("ServeHTTPDebug: %s %s", r.Method, r.RequestURI)
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

// ServeHTTP implements gocached's API via [http.Handler].
func (srv *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if srv.verbose {
		srv.logf("ServeHTTP: %s %s", r.Method, r.RequestURI)
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
			if srv.verbose {
				reason := fmt.Sprintf("exists: %v", ok)
				if sessionData != nil {
					reason += fmt.Sprintf(", expiry: %v", sessionData.expiry)
				}
				srv.logf("unauthorized; %s", reason)
			}
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if r.Method == "PUT" {
		var writeNS int64
		if sessionData != nil {
			writeNS = sessionData.namespaceID
		}
		srv.handlePut(w, r, reqStats, writeNS)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		http.Error(w, "bad method", http.StatusBadRequest)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/action/") {
		srv.handleGetAction(w, r, reqStats, sessionData)
		return
	}
	if sessionData != nil && r.URL.Path == "/session/stats" {
		srv.handleSessionStats(w, sessionData)
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func (srv *Server) getSessionData(token string) (*sessionData, bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	sessionData, ok := srv.sessions[token]
	return sessionData, ok
}

func (srv *Server) addSessionData(token string, sessionData *sessionData) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.sessions[token] = sessionData
	srv.m.Sessions.Add(1)
}

func (srv *Server) processRequestStats(req *stats, sessionData *sessionData) {
	srv.m.Gets.Add(req.Gets)
	srv.m.GetBytes.Add(req.GetBytes)
	srv.m.GetHits.Add(req.GetHits)
	srv.m.GetAccessBumps.Add(req.GetAccessBumps)
	srv.m.GetHitsInline.Add(req.GetHitsInline)
	srv.m.GetErrs.Add(req.GetErrs)
	srv.m.Puts.Add(req.Puts)
	srv.m.PutErrs.Add(req.PutErrs)
	srv.m.PutsDup.Add(req.PutsDup)
	srv.m.PutsBytes.Add(req.PutsBytes)
	srv.m.PutsInline.Add(req.PutsInline)

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

// actionKey is the comparable value type for the (NamespaceID, ActionID)
// primary key tuple used in the SQLite Actions table.
type actionKey struct {
	NamespaceID int64 // 0 for global
	ActionID    string
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

const getFromGlobalNamespace = `
SELECT b.SHA256, b.StoredSize, b.UncompressedSize, b.SmallData, a.AltOutputID, a.AccessTime, a.NamespaceID
FROM Actions a, Blobs b
WHERE a.NamespaceID = 0
  AND a.ActionID = ?
  AND a.BlobID = b.BlobID
`

// Get hits from the global namespace first so shared cache mtime is bumped
// with higher priority than namespaced cache.
const getFromSessionNamespace = `
SELECT b.SHA256, b.StoredSize, b.UncompressedSize, b.SmallData, a.AltOutputID, a.AccessTime, a.NamespaceID
FROM Actions a, Blobs b
WHERE a.NamespaceID IN (0, ?)
  AND a.ActionID = ?
  AND a.BlobID = b.BlobID
ORDER BY CASE a.NamespaceID WHEN 0 THEN 0 ELSE 1 END
LIMIT 1
`

func (srv *Server) handleGetAction(w http.ResponseWriter, r *http.Request, stats *stats, sessionData *sessionData) {
	srv.m.ActiveGets.Add(1)
	defer srv.m.ActiveGets.Add(-1)

	start := srv.now()
	labels := writeObjectResponseLabels{storage: "error", result: "error"}
	defer func() {
		d := srv.now().Sub(start)
		stats.GetNanos += d.Nanoseconds()
		srv.getDuration.WithLabelValues(labels.storage, labels.result).Observe(d.Seconds())
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

	// Serve from the put-queue first: a just-written PUT is readable here
	// before its metadata reaches SQLite. Like the SQL queries below, the
	// global namespace is preferred over the session's.
	pp, havePending := srv.putq.lookup(actionKey{ActionID: actionID})
	if !havePending && sessionData != nil && sessionData.namespaceID != 0 {
		pp, havePending = srv.putq.lookup(actionKey{NamespaceID: sessionData.namespaceID, ActionID: actionID})
	}
	if havePending {
		// Open the spool file before committing to this path: if the entry
		// was retired between the lookup and now, its file is gone but its
		// metadata is committed, so fall through to the SQL path below.
		var rc io.ReadCloser
		ok := true
		if pp.smallData == nil {
			f, err := os.Open(pp.queueFile)
			if err != nil {
				ok = false
			} else {
				rc = f
			}
		}
		if ok {
			stats.GetHits++
			opened := false
			defer func() {
				if !opened && rc != nil {
					rc.Close()
				}
			}()
			labels = srv.writeObjectResponse(w, r, stats, objectSource{
				sha256hex:        pp.sha256hex,
				storedSize:       pp.storedSize,
				uncompressedSize: pp.uncompressedSize,
				altOutputID:      pp.altOutputID,
				inline:           pp.smallData != nil,
				smallData:        pp.smallData,
				open: func() (io.ReadCloser, bool, error) {
					opened = true
					return rc, false, nil
				},
			})
			labels.markPending()
			return
		}
	}

	var (
		sha256hex                    string
		storedSize, uncompressedSize int64
		smallData                    sql.NullString
		altObjectID                  string
		accessTime                   int64
		err                          error
		actionKey                    = actionKey{
			ActionID: actionID,
		}
	)
	if sessionData != nil && sessionData.namespaceID != 0 {
		err = srv.db.QueryRow(getFromSessionNamespace, sessionData.namespaceID, actionID).Scan(
			&sha256hex, &storedSize, &uncompressedSize, &smallData, &altObjectID, &accessTime, &actionKey.NamespaceID)
	} else {
		err = srv.db.QueryRow(getFromGlobalNamespace, actionID).Scan(
			&sha256hex, &storedSize, &uncompressedSize, &smallData, &altObjectID, &accessTime, &actionKey.NamespaceID)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			labels = writeObjectResponseLabels{storage: "none", result: "miss"}
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		srv.logf("QueryRow error: %v", err)
		httpErr("QueryRow error", http.StatusInternalServerError)
		return
	}

	if srv.maybeBumpAccessTime(actionKey, accessTime) {
		stats.GetAccessBumps++
	}

	stats.GetHits++

	var sd []byte
	if smallData.Valid {
		sd = []byte(smallData.String)
	}
	isLZ4 := storedSize != uncompressedSize
	labels = srv.writeObjectResponse(w, r, stats, objectSource{
		sha256hex:        sha256hex,
		storedSize:       storedSize,
		uncompressedSize: uncompressedSize,
		altOutputID:      altObjectID,
		inline:           smallData.Valid,
		smallData:        sd,
		open: func() (io.ReadCloser, bool, error) {
			return srv.getObjectFromDiskOrPeer(ctx, sha256hex, isLZ4)
		},
	})
}

// objectSource describes a cached object to be written as an HTTP response,
// independent of whether its metadata came from SQLite or elsewhere.
type objectSource struct {
	sha256hex        string
	storedSize       int64 // bytes stored (possibly lz4-compressed)
	uncompressedSize int64
	altOutputID      string // "" if the output ID equals sha256hex
	inline           bool   // whether smallData holds the object bytes
	smallData        []byte // object bytes if inline

	// open opens a non-inline object for reading.
	// A nil ReadCloser with nil error means the object was not found.
	open func() (rc io.ReadCloser, fromHot bool, err error)
}

// writeObjectResponseLabels describes how a [Server.writeObjectResponse]
// call was served. Its fields are the label values reported to the get
// latency metric.
type writeObjectResponseLabels struct {
	// storage says where the object bytes came from: "inline" (the
	// database), "disk" (the main blob directory), or "hot" (the hot
	// tier); "none" for a response without a body (a HEAD request, an
	// empty object, or a miss); or "error". Callers serving from the
	// put-queue relabel hits as "pending" via markPending.
	storage string

	// result is the request outcome: "get" (a hit), "miss" (a 404), or
	// "error".
	result string
}

// markPending relabels a hit that carried a body as served from "pending"
// storage, for responses served from the put-queue rather than committed
// storage. Bodyless responses, misses, and errors keep their labels.
func (l *writeObjectResponseLabels) markPending() {
	if l.result == "get" && l.storage != "none" {
		l.storage = "pending"
	}
}

// writeObjectResponse writes src to w, negotiating lz4 content encoding and
// serving inline data directly. It returns the labels for the get latency
// metric.
func (srv *Server) writeObjectResponse(w http.ResponseWriter, r *http.Request, stats *stats, src objectSource) (labels writeObjectResponseLabels) {
	labels = writeObjectResponseLabels{storage: "error", result: "get"}

	outputID := cmp.Or(src.altOutputID, src.sha256hex)
	isLZ4 := src.storedSize != src.uncompressedSize
	clientAcceptsLZ4 := requestAcceptsEncoding(r, "lz4")

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Go-Output-Id", outputID)

	if src.inline || !isLZ4 {
		// Inline, empty, or legacy uncompressed: Content-Length is the stored size.
		w.Header().Set("Content-Length", fmt.Sprint(src.storedSize))
	} else if isLZ4 && clientAcceptsLZ4 {
		// Disk lz4 + client accepts lz4: stream raw compressed file.
		w.Header().Set("Content-Encoding", "lz4")
		w.Header().Set("Content-Length", fmt.Sprint(src.storedSize))
		w.Header().Set("X-Uncompressed-Length", fmt.Sprint(src.uncompressedSize))
	} else {
		// Disk lz4 + client doesn't accept lz4: decompress for client.
		w.Header().Set("Content-Length", fmt.Sprint(src.uncompressedSize))
	}

	if r.Method == "HEAD" || (src.storedSize == 0 && src.uncompressedSize == 0) {
		labels.storage = "none"
		return
	}

	if src.inline {
		// For small outputs stored inline in the database, we can return them directly.
		stats.GetHitsInline++
		stats.GetBytes += src.storedSize
		labels.storage = "inline"
		srv.blobSize.WithLabelValues(labels.storage, labels.result).Observe(float64(src.storedSize))
		w.Write(src.smallData)
		return
	}

	// Otherwise, for large objects that we know about, we can try to get them
	// from our local disk or a peer.

	rc, fromHot, err := src.open()
	if err != nil {
		srv.logf("Get object error: %v", err)
		stats.GetErrs++
		http.Error(w, "Get object error", http.StatusInternalServerError)
		labels = writeObjectResponseLabels{storage: "error", result: "error"}
		return
	}
	if rc == nil {
		// Our database suggested we should've had this object,
		// but maybe somebody delete it by hand from the filesystem.
		// Just treat it as a cache miss. The background cleanup
		// will eventually remove the Action row from the DB
		// after identifying it as a dangling reference.
		http.Error(w, "not found", http.StatusNotFound)
		labels = writeObjectResponseLabels{storage: "none", result: "miss"}
		return
	}
	defer rc.Close()
	labels.storage = "disk"
	if fromHot {
		labels.storage = "hot"
	}

	if isLZ4 && !clientAcceptsLZ4 {
		// Client doesn't accept lz4; decompress on the fly.
		stats.GetBytes += src.uncompressedSize
		srv.blobSize.WithLabelValues(labels.storage, labels.result).Observe(float64(src.uncompressedSize))
		io.Copy(w, lz4.NewReader(rc))
	} else {
		stats.GetBytes += src.storedSize
		srv.blobSize.WithLabelValues(labels.storage, labels.result).Observe(float64(src.storedSize))
		io.Copy(w, rc)
	}
	return
}

// maybeBumpAccessTime reports whether it enqueued an access time bump for the
// given actionKey, if the provided prior access time (unix seconds) is old
// enough to warrant an update.
func (srv *Server) maybeBumpAccessTime(actionKey actionKey, priorAccessTimeUnixSec int64) (didBump bool) {
	now := srv.now().Unix()
	if priorAccessTimeUnixSec > now-relAtimeSeconds {
		return false
	}
	// If it's been more than a day since the last access, update the access time.
	// This is similar to the Linux "relatime" behavior.
	return srv.enqueueAccessTimeBump(actionKey)
}

// accessBatchSizeSoftLimit is the size of the accessDirty map at which
// we stop kicking the timer can down the road and let the thing flush,
// considering it large enough.
const accessBatchSizeSoftLimit = 1000

func (srv *Server) enqueueAccessTimeBump(action actionKey) (changed bool) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if _, ok := srv.accessDirty[action]; ok {
		// Another caller already bumped this from old (over realTimeSeconds)
		// to recent, so let that caller win, even if it's a few seconds old.
		// Those few seconds don't matter much for cleaning purposes, and it's
		// better for stats to only have one caller report the bump.
		return false
	}
	if srv.accessDirty == nil {
		srv.accessDirty = make(map[actionKey]int64)
	}
	srv.accessDirty[action] = srv.now().Unix()
	if srv.accessFlushTimer == nil {
		srv.accessFlushTimer = time.AfterFunc(5*time.Second, srv.flushAccessTimeBumps)
	} else if len(srv.accessDirty) < accessBatchSizeSoftLimit {
		srv.accessFlushTimer.Reset(5 * time.Second)
	}
	return true
}

// flushAccessTimeBumps writes any pending access time updates to the database.
//
// It doesn't return an error to it can be easily used as a time.AfterFunc
// callback.
func (srv *Server) flushAccessTimeBumps() {
	srv.flushAccessTimeBumpsWithErr()
}

// flushAccessTimeBumpsWithErr is the same as flushAccessTimeBumps but returns
// any error that's used only for the internal rescheduling defer logic.
func (srv *Server) flushAccessTimeBumpsWithErr() (ret error) {
	srv.sqliteWriteMu.Lock() // before srv.mu
	defer srv.sqliteWriteMu.Unlock()

	srv.mu.Lock()
	defer srv.mu.Unlock()

	defer func() {
		if ret != nil {
			srv.logf("flushAccessTimeBumps error: %v", ret)
			srv.accessFlushTimer = time.AfterFunc(5*time.Second, srv.flushAccessTimeBumps)
		}
	}()

	srv.accessFlushTimer = nil

	if len(srv.accessDirty) == 0 {
		return nil
	}

	if srv.writeConn == nil {
		var err error
		srv.writeConn, err = srv.db.Conn(context.Background())
		if err != nil {
			return fmt.Errorf("getting writeConn: %w", err)
		}
	}

	if srv.updateAccessTimeStmt == nil {
		var err error
		srv.updateAccessTimeStmt, err = srv.writeConn.PrepareContext(context.Background(), "UPDATE Actions SET AccessTime = ? WHERE NamespaceID = ? AND ActionID = ?")
		if err != nil {
			return fmt.Errorf("prepare updateAccessTimeStmt: %w", err)
		}
	}

	tx, err := srv.writeConn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("begin Tx: %w", err)
	}
	defer tx.Rollback()

	txStmt := tx.Stmt(srv.updateAccessTimeStmt)

	for action, accessTime := range srv.accessDirty {
		_, err := txStmt.Exec(accessTime, action.NamespaceID, action.ActionID)
		if err != nil {
			return fmt.Errorf("updating access time for ns=%d,action=%q: %w", action.NamespaceID, action.ActionID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	srv.accessDirty = nil
	return nil
}

// getObjectFromDiskOrPeer retrieves the object for the given actionID, either
// from disk or a peer. This is used after a local DB lookup discovers the content
// exists but is not stored in SQLite.
//
// If isLZ4 is true, the .lz4 suffixed path is opened; otherwise the plain path.
//
// With tiering enabled, the hot tier is consulted first; fromHot reports
// whether the returned reader is backed by the hot tier. A hot tier miss that
// hits the main directory kicks off an asynchronous promotion into the hot
// tier.
//
// It returns (nil, false, nil) on miss.
func (srv *Server) getObjectFromDiskOrPeer(_ context.Context, sha256hex string, isLZ4 bool) (rc io.ReadCloser, fromHot bool, err error) {
	if len(sha256hex) != sha256.Size*2 {
		return nil, false, fmt.Errorf("invalid sha256hex %q", sha256hex)
	}
	name := sha256hex
	if isLZ4 {
		name += ".lz4"
	}
	if srv.hot != nil {
		f, err := os.Open(srv.hotFilepath(name))
		if err == nil {
			srv.hot.touch(name)
			srv.m.HotHits.Add(1)
			return f, true, nil
		}
		srv.m.HotMisses.Add(1)
		if !os.IsNotExist(err) {
			srv.m.HotReadErrs.Add(1)
			srv.logf("hot tier: opening %v: %v", name, err)
		}
	}
	diskPath := filepath.Join(srv.dir, sha256hex[:2], name)
	f, err := os.Open(diskPath)
	if err != nil {
		if os.IsNotExist(err) {
			// TODO(bradfitz): search peers, S3, etc.
			// For now, just return a miss.
			return nil, false, nil
		}
		return nil, false, err
	}
	if srv.hot != nil {
		// The index may still hold an entry if the hot file vanished out from
		// under us (e.g. a wiped ephemeral disk); drop it so the stale entry
		// doesn't block re-promotion and usage accounting stays honest.
		srv.hot.remove(name)
		if srv.hot.tryStartPromotion(name) {
			go srv.promoteToHot(name)
		}
	}
	return f, false, nil
}

// promoteToHot copies the blob file with the given base filename from the
// main blob directory into the hot tier. It runs in its own goroutine after a
// hot tier miss; the caller must have registered the promotion via
// [hotIndex.tryStartPromotion].
func (srv *Server) promoteToHot(name string) {
	defer srv.hot.endPromotion(name)
	if srv.shutdownCtx.Err() != nil {
		return
	}
	size, err := srv.copyToHot(name)
	if err != nil {
		srv.logf("hot tier: promoting %v: %v", name, err)
		srv.m.HotPromotionErrs.Add(1)
		return
	}
	srv.hot.add(name, size)
	srv.m.HotPromotions.Add(1)
	srv.evictHotIfOver()
}

// copyToHot copies the named blob file from the main blob directory to a temp
// file in the hot dir and atomically renames it into place, returning the
// file size. The bytes are copied verbatim (in already-compressed form, if
// applicable), so no re-hashing or re-compression is needed.
func (srv *Server) copyToHot(name string) (size int64, err error) {
	src, err := os.Open(filepath.Join(srv.dir, name[:2], name))
	if err != nil {
		return 0, err
	}
	defer src.Close()

	tf, err := os.CreateTemp(srv.hotDir, fmt.Sprintf("upload-%d-*", srv.now().Unix()))
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			tf.Close()
			os.Remove(tf.Name())
		}
	}()

	size, err = io.Copy(tf, src)
	if err != nil {
		return 0, err
	}
	if err := tf.Close(); err != nil {
		return 0, err
	}
	target := srv.hotFilepath(name)
	if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
		return 0, err
	}
	if err := os.Rename(tf.Name(), target); err != nil {
		return 0, err
	}
	return size, nil
}

// hotEvictTargetPct is the percentage of the hot tier capacity that eviction
// shrinks usage down to once the capacity is exceeded. Evicting slightly past
// the limit reduces churn from evicting one file at a time on every write.
const hotEvictTargetPct = 95

// evictHotIfOver deletes the least recently used hot tier files if the hot
// tier is over its capacity. It's a no-op when tiering is disabled, the
// startup scan hasn't yet measured usage, or usage is within bounds. Hot
// files are just copies, so deletion never loses data.
func (srv *Server) evictHotIfOver() {
	if srv.hot == nil || !srv.hot.ready.Load() || srv.hot.usageBytes() <= srv.hotCap {
		return
	}
	for _, ent := range srv.hot.evictLRU(srv.hotCap * hotEvictTargetPct / 100) {
		if err := os.Remove(srv.hotFilepath(ent.name)); err != nil && !os.IsNotExist(err) {
			srv.logf("hot tier: evicting %v: %v", ent.name, err)
		}
		srv.m.HotEvicted.Add(1)
		srv.m.HotEvictedBytes.Add(ent.size)
	}
}

// removeFromHot deletes both possible hot tier files (plain and .lz4) for the
// given SHA256 hex string and drops them from the hot index. It is called
// when a blob is evicted from the cache entirely. The files are removed even
// if the index doesn't know them (e.g. the startup scan hasn't reached them
// yet). Errors are logged only: hot files are disposable copies, so a
// leftover is at worst wasted space that eviction later reclaims.
func (srv *Server) removeFromHot(sha256Hex string) {
	if srv.hot == nil {
		return
	}
	for _, name := range []string{sha256Hex, sha256Hex + ".lz4"} {
		srv.hot.remove(name)
		if err := os.Remove(srv.hotFilepath(name)); err != nil && !os.IsNotExist(err) {
			srv.logf("hot tier: removing %v: %v", name, err)
		}
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, stats *stats, namespaceID int64) {
	s.m.ActivePuts.Add(1)
	defer s.m.ActivePuts.Add(-1)

	start := s.now()
	storage := "error"
	result := "error"
	defer func() {
		d := s.now().Sub(start)
		stats.PutsNanos += d.Nanoseconds()
		s.putDuration.WithLabelValues(storage, result).Observe(d.Seconds())
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

	// Backpressure: reserve queue room for the declared size before reading
	// any of the body. This blocks when the background pipeline is behind,
	// and aborts if the client goes away while waiting.
	reserved, err := s.putq.reserve(r.Context(), r.ContentLength)
	if err != nil {
		stats.PutErrs++
		http.Error(w, "canceled while awaiting queue room", http.StatusServiceUnavailable)
		return
	}
	// The reservation is handed off to the pending entry on enqueue; until
	// then, any early return must give it back.
	handedOff := false
	defer func() {
		if !handedOff {
			s.putq.unreserve(reserved)
		}
	}()

	hasher := sha256.New()
	hashingBody := io.TeeReader(r.Body, hasher)

	storedSize := r.ContentLength
	uncompressedSize := r.ContentLength

	var smallData []byte
	var queueFile string
	if r.ContentLength <= smallObjectSize {
		// Small objects are held in memory and eventually stored inline in
		// the database.
		smallData, err = io.ReadAll(hashingBody)
		if err != nil {
			s.logf("Read content error: %v", err)
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
		// Larger objects are spooled (lz4 compressed) to the put-queue
		// directory on the local disk; the background movers copy them
		// into the main blob directory.
		diskSize, path, err := s.putq.spoolBlob(r.ContentLength, hashingBody)
		if err != nil {
			s.logf("Spool blob error: %v", err)
			stats.PutErrs++
			http.Error(w, "Spool blob error", http.StatusInternalServerError)
			return
		}
		storedSize = diskSize
		queueFile = path
	}

	sha256hex := fmt.Sprintf("%x", hasher.Sum(nil))

	altOutputID := ""
	if sha256hex != outputID {
		altOutputID = outputID
	}
	p := &pendingPut{
		key:              actionKey{NamespaceID: namespaceID, ActionID: actionID},
		sha256hex:        sha256hex,
		storedSize:       storedSize,
		uncompressedSize: uncompressedSize,
		altOutputID:      altOutputID,
		createTime:       s.now().Unix(),
		smallData:        smallData,
		queueFile:        queueFile,
		reservedBytes:    reserved,
	}

	if s.putq.enqueue(p) {
		// An entry for this action is already pending; the first PUT wins,
		// as it would at insert time in the database. Duplicates that are
		// already committed to SQLite aren't detected here; the flusher
		// discovers and counts those later.
		if queueFile != "" {
			os.Remove(queueFile)
		}
		stats.PutsDup++
		result = "dup"
	} else {
		handedOff = true
		result = "put"
	}

	stats.Puts++
	stats.PutsBytes += r.ContentLength
	if smallData != nil {
		stats.PutsInline++
		storage = "inline"
	} else {
		storage = "disk"
	}
	s.blobSize.WithLabelValues(storage, result).Observe(float64(r.ContentLength))

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
		srv.m.AuthErrs.Add(1)
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	jwtClaims, err := srv.jwtValidator.Validate(r.Context(), req.JWT)
	if err != nil {
		srv.m.AuthErrs.Add(1)
		if srv.verbose {
			srv.logf("token exchange: JWT validation error: %v", err)
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	ns, err := srv.namespaceMapping(jwtClaims)
	if err != nil {
		srv.m.AuthErrs.Add(1)
		if srv.verbose {
			srv.logf("token exchange: namespace func error: %v", err)
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if err := validateNamespace(ns); err != nil {
		srv.m.AuthErrs.Add(1)
		if srv.verbose {
			srv.logf("token exchange: invalid namespace from claims: %v", err)
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	var namespaceID int64
	if ns != GlobalNamespace {
		namespaceID, err = srv.resolveNamespaceID(ns)
		if err != nil {
			srv.m.AuthErrs.Add(1)
			srv.logf("token exchange: %v", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}

	const ttl = time.Hour
	// 52 base32 characters, 256 bits of entropy.
	accessToken := tokenPrefix + strings.ToLower(rand.Text()+rand.Text())
	srv.addSessionData(accessToken, &sessionData{
		expiry:      srv.now().UTC().Add(ttl),
		namespaceID: namespaceID,
		namespace:   ns,
		claims:      jwtClaims,
	})

	resp := map[string]any{
		"access_token": accessToken,
		"token_type":   "Bearer",
		"expires_in":   ttl.Seconds(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		srv.m.AuthErrs.Add(1)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	srv.m.Auths.Add(1)
}

// namespaceAllowedBytes is the set of non-alphanumeric bytes permitted in a
// namespace. It is chosen to cover characters common in JWT identity claims:
// issuer URLs, emails, and provider-structured "sub" values such as
// "repo:org/repo:environment:prod" and "auth0|abc123". It is deliberately
// ASCII-only so SQLite BINARY comparison matches Go string equality byte for
// byte, avoiding Unicode normalization divergence.
const namespaceAllowedBytes = "._~:/@+|=-"

func validateNamespace(ns Namespace) error {
	if ns == GlobalNamespace {
		return nil
	}
	for i := 0; i < len(ns); i++ {
		c := ns[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case strings.IndexByte(namespaceAllowedBytes, c) >= 0:
		default:
			return fmt.Errorf("namespace contains disallowed byte %#x at index %d", c, i)
		}
	}
	return nil
}

// resolveNamespaceID returns the integer ID for the given Namespace,
// inserting a row in the Namespaces table if one doesn't already exist.
func (srv *Server) resolveNamespaceID(ns Namespace) (int64, error) {
	// If it's not a new namespace, we only need to consult our cache of IDs.
	srv.mu.Lock()
	id, ok := srv.namespaces[ns]
	srv.mu.Unlock()
	if ok {
		return id, nil
	}

	srv.sqliteWriteMu.Lock()
	defer srv.sqliteWriteMu.Unlock()

	srv.mu.Lock()
	defer srv.mu.Unlock()

	// Check if we lost a race now that we have both locks.
	if id, ok = srv.namespaces[ns]; ok {
		return id, nil
	}

	err := srv.db.QueryRow(`INSERT INTO Namespaces (Namespace) VALUES (?)
		RETURNING NamespaceID;`, ns).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("resolving namespace %q: %w", ns, err)
	}

	srv.namespaces[ns] = id

	return id, nil
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
	return filepath.Join(s.dir, hex[:2], hex)
}

// hotFilepath returns the hot tier path for the given base filename, which is
// either "<sha256-hex>" or "<sha256-hex>.lz4".
func (s *Server) hotFilepath(name string) string {
	return filepath.Join(s.hotDir, name[:2], name)
}

type countAndSize struct {
	Count int64 // number of actions
	Size  int64 // total stored size in bytes (after compression, if any) of all actions' blobs, even if shared by other actions
}

func (cs countAndSize) String() string {
	if cs.Count == 0 {
		return "0 objects, 0 bytes"
	}
	return fmt.Sprintf("%d objects, %s", cs.Count, bytesFmt(cs.Size))
}

type usageStats struct {
	// ActionsLE is a histogram of the actions in the DB by their access time.
	// Sizes reflect stored size on disk (after any compression).
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

	// QueryDuration is how long the per-shard SQL query took. It is set by
	// [Server.scanShard] for shard snapshots and is left at the zero value
	// in the aggregate returned by [Server.usageStats]. Persisted in JSON
	// so /usage can surface slow shards across restarts.
	QueryDuration time.Duration
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

// computeDurs calculates histogram buckets for usage stats, given the
// configured maxAge. The math.MaxInt64 sentinel is always included as the
// "no upper bound" bucket.
func computeDurs(maxAge time.Duration) []time.Duration {
	if maxAge == 0 || slices.Contains(standardDurs, maxAge) {
		return slices.Clone(standardDurs)
	}
	durs := []time.Duration{maxAge, math.MaxInt64}
	for _, d := range standardDurs {
		if d < maxAge {
			durs = append(durs, d)
		}
	}
	slices.Sort(durs)
	return durs
}

// shardPrefix is the lowercase hex prefix that identifies one SHA256 shard
// (e.g. "00", "ab"). The named type keeps shard-key strings from being
// passed where any string would do — code that means "shard key" reads as
// shardPrefix; SQL parameters and other free-form strings stay plain string.
type shardPrefix string

// shardSnapshot is the cached state for a single SHA256-prefix shard.
type shardSnapshot struct {
	stats     *usageStats
	scannedAt time.Time
}

// shardDelta is the dead-reckoning state for one shard. count and bytes are
// signed: PUTs add positive values, evictions add negative ones.
// scanAndPersistShard subtracts the change in the persisted view of this
// shard (newStats - oldStats) so anything that just landed in the aggregate
// leaves the delta.
//
// The per-shard mu serializes the count+bytes pair so a reader can't catch
// a PUT mid-update with count incremented but bytes not (or vice versa).
// Storing the values inline avoids allocating a new struct on every PUT.
type shardDelta struct {
	mu    sync.Mutex
	count int64
	bytes int64
}

// addBlobDelta adjusts the shardDelta for the shard owning sha256hex by the
// given signed (count, bytes) values. Callers pass +1, +storedSize on a
// successful new Action insert, and -1, -storedSize when evicting an Action.
// Bogus sha256hex (too short or non-hex prefix) is silently ignored.
func (srv *Server) addBlobDelta(sha256hex string, count, bytes int64) {
	if srv.shardPrefixLen <= 0 || len(sha256hex) < srv.shardPrefixLen {
		return
	}
	n, err := strconv.ParseUint(sha256hex[:srv.shardPrefixLen], 16, 64)
	if err != nil || int(n) >= len(srv.shardDeltas) {
		return
	}
	sd := &srv.shardDeltas[n]
	sd.mu.Lock()
	sd.count += count
	sd.bytes += bytes
	sd.mu.Unlock()
}

// sumShardDeltas returns the sum of every shard's pending delta. The caller
// adds this to the persisted aggregate to get a live estimate of total cache
// occupancy between scans.
func (srv *Server) sumShardDeltas() (count, bytes int64) {
	for i := range srv.shardDeltas {
		sd := &srv.shardDeltas[i]
		sd.mu.Lock()
		count += sd.count
		bytes += sd.bytes
		sd.mu.Unlock()
	}
	return
}

// numShards returns 16^shardPrefixLen, the total number of SHA256-prefix
// shards used to partition usage statistics.
func (srv *Server) numShards() int {
	return 1 << (4 * srv.shardPrefixLen)
}

// shardPrefix returns the hex prefix for shard index i, zero-padded to
// shardPrefixLen characters (e.g. 0 -> "00", 255 -> "ff").
func (srv *Server) shardPrefix(i int) shardPrefix {
	return shardPrefix(fmt.Sprintf("%0*x", srv.shardPrefixLen, i))
}

// shardRange returns the [lo, hi) SHA256 hex range covered by shard index i.
// For the final shard, hi is a sentinel string of 'g' characters: 'g' sorts
// after every valid hex digit, so "SHA256 < hi" excludes nothing in that
// range. Using a sentinel keeps the SQL uniform (always two parameters).
// Returns plain strings since both ends bind directly into SQL.
func (srv *Server) shardRange(i int) (lo, hi string) {
	lo = string(srv.shardPrefix(i))
	if i+1 == srv.numShards() {
		hi = strings.Repeat("g", srv.shardPrefixLen)
	} else {
		hi = string(srv.shardPrefix(i + 1))
	}
	return
}

// usageStats returns the most recent aggregate of per-shard stats. It does no
// SQL work: the aggregate is maintained by the shard stats loop and seeded
// from the BlobShardStats table at startup, so callers see usage immediately
// after process restart rather than after a multi-minute full-table scan. The
// returned value is never nil; ActionsLE may be empty if no shard has been
// scanned and no persisted stats exist.
func (s *Server) usageStats() (*usageStats, error) {
	if us := s.lastUsage.Load(); us != nil {
		return us, nil
	}
	return &usageStats{ActionsLE: make(map[time.Duration]countAndSize)}, nil
}

// scanShard runs a single SQL-aggregated query that computes the cohort
// histogram for Actions whose Blob's SHA256 falls in shard idx's range.
// The aggregation happens entirely in SQL via SUM(CASE WHEN ...) clauses, so
// the driver returns one row regardless of how many Actions fall in the shard.
// This is what makes a scan cheap enough to pin a reader snapshot for only
// milliseconds per shard rather than minutes for the whole table.
func (srv *Server) scanShard(ctx context.Context, idx int) (*usageStats, error) {
	durs := srv.durs
	now := srv.now().Unix()

	var sb strings.Builder
	args := make([]any, 0, 2*len(durs)+2)
	sb.WriteString("SELECT ")
	finiteCount := 0
	for _, d := range durs {
		if d == math.MaxInt64 {
			continue
		}
		cutoff := now - int64(d/time.Second)
		if finiteCount > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("COALESCE(SUM(CASE WHEN a.AccessTime > ? THEN 1 ELSE 0 END),0),")
		sb.WriteString("COALESCE(SUM(CASE WHEN a.AccessTime > ? THEN b.StoredSize ELSE 0 END),0)")
		args = append(args, cutoff, cutoff)
		finiteCount++
	}
	sb.WriteString(",COUNT(*),COALESCE(SUM(b.StoredSize),0)")
	sb.WriteString(" FROM Blobs b JOIN Actions a ON a.BlobID = b.BlobID")
	sb.WriteString(" WHERE b.SHA256 >= ? AND b.SHA256 < ?")
	lo, hi := srv.shardRange(idx)
	args = append(args, lo, hi)

	vals := make([]int64, finiteCount*2+2)
	dest := make([]any, len(vals))
	for i := range vals {
		dest[i] = &vals[i]
	}
	start := time.Now()
	if err := srv.db.QueryRowContext(ctx, sb.String(), args...).Scan(dest...); err != nil {
		return nil, err
	}
	queryDuration := time.Since(start)
	if srv.shardScanDuration != nil {
		// Nil-check: scanShard is reachable from tests that build a bare
		// Server without calling start (e.g. TestShardPrefix).
		srv.shardScanDuration.Observe(queryDuration.Seconds())
	}

	st := &usageStats{
		ActionsLE:     make(map[time.Duration]countAndSize, len(durs)),
		QueryDuration: queryDuration,
	}
	j := 0
	for _, d := range durs {
		if d == math.MaxInt64 {
			continue
		}
		st.ActionsLE[d] = countAndSize{Count: vals[j*2], Size: vals[j*2+1]}
		j++
	}
	st.ActionsLE[math.MaxInt64] = countAndSize{Count: vals[finiteCount*2], Size: vals[finiteCount*2+1]}
	return st, nil
}

// shardCohortsMatch reports whether the persisted ActionsLE map keys exactly
// match the current srv.durs set. A mismatch means the persisted shard was
// written with a different maxAge (or a different code version), so we
// discard it and force the stats loop to re-scan that shard.
func shardCohortsMatch(le map[time.Duration]countAndSize, durs []time.Duration) bool {
	if len(le) != len(durs) {
		return false
	}
	for _, d := range durs {
		if _, ok := le[d]; !ok {
			return false
		}
	}
	return true
}

// loadShardStats reads every BlobShardStats row at startup, validates that
// the persisted cohort set matches srv.durs, and seeds shardStats + the
// aggregate. Rows with mismatched cohorts are dropped so the stats loop
// re-scans them. The aggregate becomes the source of truth for /usage and the
// cleanup loop until the first new shard scan completes.
func (srv *Server) loadShardStats(ctx context.Context) error {
	rows, err := srv.db.QueryContext(ctx, "SELECT Prefix, ScannedAt, StatsJSON FROM BlobShardStats")
	if err != nil {
		return err
	}
	defer rows.Close()
	srv.shardStatsMu.Lock()
	defer srv.shardStatsMu.Unlock()
	srv.shardStats = make(map[shardPrefix]*shardSnapshot, srv.numShards())
	for rows.Next() {
		var prefix, statsJSON string
		var scannedAt int64
		if err := rows.Scan(&prefix, &scannedAt, &statsJSON); err != nil {
			return err
		}
		if len(prefix) != srv.shardPrefixLen {
			// A previous run used a different shardPrefixLen. The stats loop
			// won't write to these keys, so they'd otherwise sit in the
			// table forever contributing stale data to the aggregate.
			srv.logf("BlobShardStats[%q] prefix length %d != current %d; will rescan",
				prefix, len(prefix), srv.shardPrefixLen)
			continue
		}
		var st usageStats
		if err := json.Unmarshal([]byte(statsJSON), &st); err != nil {
			srv.logf("BlobShardStats[%q] JSON decode: %v; will rescan", prefix, err)
			continue
		}
		if !shardCohortsMatch(st.ActionsLE, srv.durs) {
			srv.logf("BlobShardStats[%q] cohort mismatch; will rescan", prefix)
			continue
		}
		srv.shardStats[shardPrefix(prefix)] = &shardSnapshot{
			stats:     &st,
			scannedAt: time.Unix(scannedAt, 0),
		}
		// Replay the persisted scan time into the histogram so the
		// /metrics endpoint shows reasonable percentiles immediately after
		// restart instead of waiting a full pass over every shard. The observation is
		// older than "now" but it's the most accurate signal we have for
		// this shard until the stats loop refreshes it.
		if srv.shardScanDuration != nil && st.QueryDuration > 0 {
			srv.shardScanDuration.Observe(st.QueryDuration.Seconds())
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	srv.recomputeAggregateLocked()
	return nil
}

// upsertShardStats persists one shard's scan result. The row is keyed on
// Prefix so subsequent scans of the same shard overwrite in place.
func (srv *Server) upsertShardStats(ctx context.Context, prefix shardPrefix, scannedAt time.Time, st *usageStats) error {
	statsJSON, err := json.Marshal(st)
	if err != nil {
		return err
	}
	srv.sqliteWriteMu.Lock()
	defer srv.sqliteWriteMu.Unlock()
	_, err = srv.db.ExecContext(ctx,
		`INSERT INTO BlobShardStats (Prefix, ScannedAt, StatsJSON) VALUES (?, ?, ?)
		 ON CONFLICT(Prefix) DO UPDATE SET ScannedAt=excluded.ScannedAt, StatsJSON=excluded.StatsJSON`,
		prefix, scannedAt.Unix(), string(statsJSON))
	return err
}

// recomputeAggregateLocked sums the cohort histograms across every cached
// shard, stores the result in lastUsage, and updates the BlobCount/BlobBytes
// gauges. The caller must hold shardStatsMu.
func (srv *Server) recomputeAggregateLocked() {
	agg := &usageStats{ActionsLE: make(map[time.Duration]countAndSize, len(srv.durs))}
	for _, d := range srv.durs {
		agg.ActionsLE[d] = countAndSize{}
	}
	for _, sh := range srv.shardStats {
		for d, cs := range sh.stats.ActionsLE {
			was := agg.ActionsLE[d]
			was.Count += cs.Count
			was.Size += cs.Size
			agg.ActionsLE[d] = was
		}
	}
	srv.lastUsage.Store(agg)
	total := agg.All()
	srv.m.BlobCount.Set(total.Count)
	srv.m.BlobBytes.Set(total.Size)
}

// pickOldestShard returns the shard index whose ScannedAt is oldest, along
// with that ScannedAt. A shard that has never been scanned (no entry in
// shardStats) wins immediately, and is returned with a zero time so the
// caller treats it as infinitely stale (scan it now, no sleep).
func (srv *Server) pickOldestShard() (idx int, scannedAt time.Time) {
	srv.shardStatsMu.Lock()
	defer srv.shardStatsMu.Unlock()
	oldestIdx := -1
	var oldestTime time.Time
	for i := range srv.numShards() {
		sh, ok := srv.shardStats[srv.shardPrefix(i)]
		if !ok {
			return i, time.Time{}
		}
		if oldestIdx == -1 || sh.scannedAt.Before(oldestTime) {
			oldestIdx = i
			oldestTime = sh.scannedAt
		}
	}
	return oldestIdx, oldestTime
}

// scanAndPersistShard scans shard idx, writes the result back to
// BlobShardStats, and updates the in-memory cache + aggregate.
func (srv *Server) scanAndPersistShard(ctx context.Context, idx int) error {
	prefix := srv.shardPrefix(idx)
	scannedAt := srv.now()
	stats, err := srv.scanShard(ctx, idx)
	if err != nil {
		return fmt.Errorf("scan shard %q: %w", prefix, err)
	}
	if err := srv.upsertShardStats(ctx, prefix, scannedAt, stats); err != nil {
		return fmt.Errorf("persist shard %q: %w", prefix, err)
	}

	srv.shardStatsMu.Lock()
	var oldAll countAndSize
	if old, ok := srv.shardStats[prefix]; ok {
		oldAll = old.stats.All()
	}
	srv.shardStats[prefix] = &shardSnapshot{stats: stats, scannedAt: scannedAt}
	srv.recomputeAggregateLocked()
	srv.shardStatsMu.Unlock()

	// Subtract from the delta the exact change that just landed in the
	// persisted view: (newStats - oldStats). Anything in delta whose Action
	// commit happened before scanShard's snapshot is now in newStats, so we
	// subtract it out; anything that committed after the snapshot isn't in
	// newStats, so the diff doesn't touch its delta contribution. PUTs whose
	// addBlobDelta hasn't been called yet temporarily push delta negative;
	// when the addBlobDelta runs it cancels out.
	//
	// First-scan caveat: when oldStats was missing (no prior scan for this
	// shard AND no persisted row from a previous process run), oldAll is
	// zero. If the DB had pre-existing rows that delta never saw (e.g. first
	// deploy of this code on a populated DB whose BlobShardStats table was
	// empty), subtracting newStats from delta will leave delta with a
	// negative offset equal to the pre-existing row count. The aggregate
	// becomes correct from then on; the negative offset stays in the delta
	// until process restart (which reloads aggregate from the now-populated
	// BlobShardStats and starts delta at zero). Operators deploying onto a
	// populated DB can avoid this by nuking the DB first.
	newAll := stats.All()
	diffCount := newAll.Count - oldAll.Count
	diffBytes := newAll.Size - oldAll.Size
	sd := &srv.shardDeltas[idx]
	sd.mu.Lock()
	sd.count -= diffCount
	sd.bytes -= diffBytes
	sd.mu.Unlock()
	return nil
}

// runShardStatsLoop keeps usage stats fresh by rescanning the
// oldest-scanned shard whenever it crosses [shardStalenessTarget]. The
// per-scan cadence in steady state works out to about
// shardStalenessTarget/numShards (~2.3s at defaults)
func (srv *Server) runShardStatsLoop() {
	for {
		idx, scannedAt := srv.pickOldestShard()
		var sleep time.Duration
		if !scannedAt.IsZero() {
			sleep = max(shardStalenessTarget-srv.now().Sub(scannedAt), shardStatsMinInterval)
		}
		if sleep > 0 {
			select {
			case <-srv.shutdownCtx.Done():
				return
			case <-time.After(sleep):
			}
		}
		if err := srv.scanAndPersistShard(srv.shutdownCtx, idx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			srv.logf("shard stats loop: %v", err)
		}
	}
}

// scanAllShards synchronously rescans every shard. Used by tests to get a
// deterministic aggregate, and by POST /usage as a manual "refresh now"
// trigger when an operator doesn't want to wait for the stats loop.
func (srv *Server) scanAllShards(ctx context.Context) error {
	for i := range srv.numShards() {
		if err := srv.scanAndPersistShard(ctx, i); err != nil {
			return err
		}
	}
	return nil
}

// shardScanDurationPercentiles returns p25/p50/p90 of the QueryDuration
// across every cached shard, and the count of shards that contributed (i.e.
// have a non-zero QueryDuration). Returns zeros if no shard has been scanned
// yet. Linear interpolation is overkill given numShards is 16/256/etc.; a
// rank-index pick is good enough for a /usage page.
func (srv *Server) shardScanDurationPercentiles() (p25, p50, p90 time.Duration, n int) {
	srv.shardStatsMu.Lock()
	durs := make([]time.Duration, 0, len(srv.shardStats))
	for _, sh := range srv.shardStats {
		if sh.stats.QueryDuration > 0 {
			durs = append(durs, sh.stats.QueryDuration)
		}
	}
	srv.shardStatsMu.Unlock()
	n = len(durs)
	if n == 0 {
		return
	}
	slices.Sort(durs)
	pick := func(p int) time.Duration {
		i := (n * p) / 100
		if i >= n {
			i = n - 1
		}
		return durs[i]
	}
	return pick(25), pick(50), pick(90), n
}

// shardFreshnessBuckets are the "scanned within last X" age cohorts shown on
// the /usage page. The largest cohort is intentionally larger than
// [shardStalenessTarget] so the page makes it obvious when the stats loop
// has fallen behind (the smaller cohorts drop to zero before the largest does).
var shardFreshnessBuckets = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	1 * time.Hour,
	6 * time.Hour,
	24 * time.Hour,
}

// shardFreshness counts how many cached shards were last scanned within each
// of [shardFreshnessBuckets], and returns the absolute time of the oldest
// scan. The buckets are cumulative ("within last X"), matching the cohort
// histogram style elsewhere in this package.
func (srv *Server) shardFreshness(now time.Time) (counts map[time.Duration]int, oldest time.Time, total int) {
	counts = make(map[time.Duration]int, len(shardFreshnessBuckets))
	srv.shardStatsMu.Lock()
	defer srv.shardStatsMu.Unlock()
	total = len(srv.shardStats)
	for _, sh := range srv.shardStats {
		age := now.Sub(sh.scannedAt)
		for _, b := range shardFreshnessBuckets {
			if age <= b {
				counts[b]++
			}
		}
		if oldest.IsZero() || sh.scannedAt.Before(oldest) {
			oldest = sh.scannedAt
		}
	}
	return counts, oldest, total
}

// evictionCandidateQuery is the SQL used by [Server.evictOldestActions] to
// pick which Actions to delete next. INDEXED BY is the documented
// SQLite mechanism (https://sqlite.org/lang_indexedby.html) for locking
// down a plan so a future schema change can't silently regress this query
// from an O(log N) index range scan into an O(N) table scan. The plan is
// verified in TestEvictionQueryPlan.
const evictionCandidateQuery = `
SELECT NamespaceID, ActionID, BlobID
FROM Actions INDEXED BY idx_actions_access
WHERE AccessTime <= ?
ORDER BY AccessTime ASC
LIMIT ?`

// evictOldestActions deletes up to maxCount Actions whose AccessTime is at
// or before cutoff, oldest first, in a single short write transaction. For
// each deleted Action, if its Blob has no other referencing Actions
// remaining, the Blob row and its on-disk file are also removed. The batch
// stops early once at least maxBytes have been reclaimed (use math.MaxInt64
// for no byte budget — e.g., maxAge cleanup, where we want every stale
// Action gone regardless of size).
//
// This is Action-LRU: a stale Action is evicted even if its Blob is shared
// with newer Actions (the Blob then stays alive via those newer refs). That
// differs from the prior Blob-LRU policy, where a Blob was only evicted when
// MAX(AccessTime) of all its Actions was past the cutoff. The cache is
// mostly 1:1 Action-to-Blob so the two policies are equivalent for the
// common case; on shared Blobs, Action-LRU is the stricter LRU semantics.
func (srv *Server) evictOldestActions(ctx context.Context, cutoff int64, maxCount int, maxBytes int64) (countAndSize, error) {
	var ret countAndSize

	rows, err := srv.db.QueryContext(ctx, evictionCandidateQuery, cutoff, maxCount)
	if err != nil {
		return ret, fmt.Errorf("query eviction candidates: %w", err)
	}
	type candidate struct {
		NamespaceID int64
		ActionID    string
		BlobID      int64
	}
	var cands []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.NamespaceID, &c.ActionID, &c.BlobID); err != nil {
			rows.Close()
			return ret, fmt.Errorf("scan candidate: %w", err)
		}
		cands = append(cands, c)
	}
	if err := rows.Close(); err != nil {
		return ret, fmt.Errorf("close candidate rows: %w", err)
	}
	if len(cands) == 0 {
		return ret, nil
	}

	srv.sqliteWriteMu.Lock()
	defer srv.sqliteWriteMu.Unlock()

	tx, err := srv.db.BeginTx(ctx, nil)
	if err != nil {
		return ret, fmt.Errorf("eviction Begin: %w", err)
	}
	defer tx.Rollback()

	// pendingDelta queues addBlobDelta updates for after Commit. Holding
	// them until commit means a rolled-back tx (rare) doesn't poison the
	// dead-reckoning state.
	type pendingDelta struct {
		sha256Hex  string
		storedSize int64
	}
	var pending []pendingDelta

	var evictedBlobs int64
	var evictedBlobBytes int64   // bytes reclaimed (disk space + EvictedBytes metric)
	var evictedActionBytes int64 // aggregate-side bytes reduction (drives maxBytes check)
	var evictedActions int64
	for _, c := range cands {
		// Fetch SHA256 + StoredSize up front so we can both (a) decrement
		// the dead-reckoning delta for this Action and (b) remove the disk
		// file if the Blob ends up orphaned. A LEFT-JOIN here is just an
		// extra query; the row is keyed on the primary key so it's cheap.
		var sha256Hex string
		var storedSize int64
		blobExists := true
		switch err := tx.QueryRowContext(ctx,
			"SELECT SHA256, StoredSize FROM Blobs WHERE BlobID = ?",
			c.BlobID).Scan(&sha256Hex, &storedSize); {
		case err == nil:
		case errors.Is(err, sql.ErrNoRows):
			blobExists = false
		default:
			return ret, fmt.Errorf("fetch Blob row: %w", err)
		}

		if _, err := tx.ExecContext(ctx,
			"DELETE FROM Actions WHERE NamespaceID = ? AND ActionID = ?",
			c.NamespaceID, c.ActionID); err != nil {
			return ret, fmt.Errorf("delete action: %w", err)
		}
		evictedActions++
		if !blobExists {
			// Orphan Action pointing at a missing Blob; delete the row and
			// move on. No delta, no Blob row, no file.
			continue
		}
		// Each Action contributes 1 count + storedSize bytes to the aggregate
		// (see scanShard); removing it reduces both by exactly that. Decrement
		// deferred until after Commit so a rollback doesn't leave the delta
		// off.
		pending = append(pending, pendingDelta{sha256Hex, storedSize})
		evictedActionBytes += storedSize

		// Is this Blob now orphaned? idx_actions_blobid makes this O(log N).
		var stillReferenced int
		if err := tx.QueryRowContext(ctx,
			"SELECT EXISTS(SELECT 1 FROM Actions WHERE BlobID = ?)",
			c.BlobID).Scan(&stillReferenced); err != nil {
			return ret, fmt.Errorf("check orphan: %w", err)
		}
		if stillReferenced == 1 {
			continue
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM Blobs WHERE BlobID = ?", c.BlobID); err != nil {
			return ret, fmt.Errorf("delete Blob: %w", err)
		}
		evictedBlobs++
		evictedBlobBytes += storedSize
		var hash [sha256.Size]byte
		if _, err := hex.Decode(hash[:], []byte(sha256Hex)); err == nil {
			base := srv.sha256Filepath(hash)
			// Try removing both lz4 and plain paths; one or neither may exist.
			if err := os.Remove(base + ".lz4"); err != nil && !os.IsNotExist(err) {
				return ret, fmt.Errorf("removing disk file: %w", err)
			}
			if err := os.Remove(base); err != nil && !os.IsNotExist(err) {
				return ret, fmt.Errorf("removing disk file: %w", err)
			}
			srv.removeFromHot(sha256Hex)
		}
		// Stop once we've freed enough aggregate bytes. For maxAge cleanup
		// callers pass math.MaxInt64 so this check never triggers.
		if evictedActionBytes >= maxBytes {
			break
		}
	}
	if err := tx.Commit(); err != nil {
		return ret, fmt.Errorf("eviction Commit: %w", err)
	}

	// Apply the queued delta decrements now that the tx is durably committed.
	for _, p := range pending {
		srv.addBlobDelta(p.sha256Hex, -1, -p.storedSize)
	}

	srv.m.EvictedActions.Add(evictedActions)
	srv.m.EvictedBlobs.Add(evictedBlobs)
	srv.m.EvictedBytes.Add(evictedBlobBytes)
	ret.Count = evictedBlobs
	ret.Size = evictedBlobBytes
	return ret, nil
}

// cleanupTick decides whether the cache is over its configured maxAge or
// maxSize and, if so, runs one [Server.evictOldestActions] batch. The
// aggregate from the shard stats loop drives the decision; the eviction itself
// goes straight at idx_actions_access without consulting the shards.
func (srv *Server) cleanupTick(ctx context.Context) (countAndSize, error) {
	srv.evictHotIfOver()
	var ret countAndSize
	us := srv.lastUsage.Load()
	if us == nil {
		return ret, nil
	}
	all := us.All()
	// Include the dead-reckoning delta in the size pressure check so a
	// burst of PUTs between scans actually triggers cleanup. The maxAge
	// check intentionally ignores the delta: fresh PUTs are by definition
	// within maxAge, so they neither add to nor subtract from the count of
	// over-age Actions.
	dCount, dBytes := srv.sumShardDeltas()
	all.Count += dCount
	all.Size += dBytes

	// Default cutoff to "any age" for size-only cleanup. evictOldestActions
	// still uses idx_actions_access (the WHERE/ORDER BY drive the plan), so
	// the very-large cutoff just means "no upper bound".
	cutoff := int64(math.MaxInt64)
	overAge := false
	if srv.maxAge > 0 {
		cutoff = srv.now().Add(-srv.maxAge).Unix()
		overAge = us.All().Count-us.ActionsLE[srv.maxAge].Count > 0
	}
	overSize := srv.maxSize > 0 && all.Size > srv.maxSize
	if !overAge && !overSize {
		return ret, nil
	}
	// maxAge cleanup runs to count limit (every stale Action must go);
	// size-only cleanup runs until we've reclaimed enough bytes.
	maxBytes := int64(math.MaxInt64)
	if !overAge && overSize {
		maxBytes = all.Size - srv.maxSize
	}
	return srv.evictOldestActions(ctx, cutoff, cleanupBatchSize, maxBytes)
}

// checkpointTruncate runs PRAGMA wal_checkpoint(TRUNCATE) and returns SQLite's
// three result columns. A fully-applied checkpoint returns busy=0 and
// logFrames==ckptFrames; otherwise some frames remain in the WAL because of a
// concurrent reader pinning an older snapshot.
func (srv *Server) checkpointTruncate(ctx context.Context) (busy, logFrames, ckptFrames int, err error) {
	err = srv.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &ckptFrames)
	return busy, logFrames, ckptFrames, err
}

// runCheckpointLoop periodically runs a TRUNCATE checkpoint to keep the WAL
// bounded on disk. SQLite's autocheckpoint only runs PASSIVE checkpoints, which
// reuse WAL space in place but never shrink the file; without this loop the WAL
// can grow without bound under continuous traffic.
func (srv *Server) runCheckpointLoop() {
	t := time.NewTicker(checkpointInterval)
	defer t.Stop()
	for {
		select {
		case <-srv.shutdownCtx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(srv.shutdownCtx, 2*time.Minute)
		busy, logFrames, ckptFrames, err := srv.checkpointTruncate(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			srv.logf("wal_checkpoint(TRUNCATE) error: %v", err)
			continue
		}
		if busy != 0 || logFrames != ckptFrames {
			// A reader is pinning frames; we'll catch up next tick. Logged
			// because persistent partial checkpoints mean walJournalSizeLimit
			// is the only thing keeping the file bounded, and we'd want to
			// investigate.
			srv.logf("wal_checkpoint(TRUNCATE) partial: busy=%d log=%d ckpt=%d", busy, logFrames, ckptFrames)
		} else if srv.verbose {
			srv.logf("wal_checkpoint(TRUNCATE): log=%d ckpt=%d", logFrames, ckptFrames)
		}
		// Refresh the size gauges immediately so dashboards see the
		// post-truncate values without waiting for the next sampler tick.
		srv.updateDBSizeMetrics()
	}
}

// dbPath returns the on-disk path of the SQLite main database file.
// The WAL file is at dbPath() + "-wal".
func (srv *Server) dbPath() string {
	return filepath.Join(srv.sqliteDir, fmt.Sprintf("gocached-v%d.db", schemaVersion))
}

// updateDBSizeMetrics re-stats the SQLite files and updates the size gauges.
// A missing WAL file (e.g. on a fresh DB before the first write flushes) is
// reported as zero bytes. Other stat errors are logged but don't update the
// gauge, so a transient filesystem hiccup leaves the last-known value visible.
func (srv *Server) updateDBSizeMetrics() {
	dbPath := srv.dbPath()
	if fi, err := os.Stat(dbPath); err == nil {
		srv.m.SQLiteDataBytes.Set(fi.Size())
	} else {
		srv.logf("stat %s: %v", dbPath, err)
	}
	walPath := dbPath + "-wal"
	switch fi, err := os.Stat(walPath); {
	case err == nil:
		srv.m.SQLiteWALBytes.Set(fi.Size())
	case errors.Is(err, os.ErrNotExist):
		srv.m.SQLiteWALBytes.Set(0)
	default:
		srv.logf("stat %s: %v", walPath, err)
	}
}

// runDBSizeMetricsLoop samples the SQLite file sizes more frequently than the
// checkpoint loop runs, so the WAL gauge captures inter-checkpoint growth
// rather than only the post-truncate values.
func (srv *Server) runDBSizeMetricsLoop() {
	t := time.NewTicker(dbSizeMetricsInterval)
	defer t.Stop()
	srv.updateDBSizeMetrics() // seed an initial sample at startup
	for {
		select {
		case <-srv.shutdownCtx.Done():
			return
		case <-t.C:
		}
		srv.updateDBSizeMetrics()
	}
}

func (srv *Server) runCleanLoop() {
	for {
		select {
		case <-srv.shutdownCtx.Done():
			return
		case <-time.After(cleanupTickInterval):
		}
		if _, err := srv.cleanupTick(srv.shutdownCtx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			srv.logf("cleanup: %v", err)
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

		srv.mu.Lock()
		count := len(srv.sessions)
		var deleted int
		for token, metadata := range srv.sessions {
			if time.Now().After(metadata.expiry) {
				delete(srv.sessions, token)
				deleted++
				srv.m.Sessions.Add(-1)
			}
		}
		srv.mu.Unlock()
		if count > 0 {
			srv.logf("cleaned up %d/%d access tokens", deleted, count)
		}
	}
}

func durFmt(d time.Duration) string {
	days := int(d.Hours() / 24)
	if days > 0 {
		return fmt.Sprintf("%dd", days)
	}
	return d.String()
}

// requestAcceptsEncoding reports whether r's Accept-Encoding header includes
// the given encoding. It handles optional quality parameters (e.g.
// "gzip;q=1.0, lz4;q=0.5") by stripping them before comparison.
func requestAcceptsEncoding(r *http.Request, encoding string) bool {
	for part := range strings.SplitSeq(r.Header.Get("Accept-Encoding"), ",") {
		name, _, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(strings.TrimSpace(name), encoding) {
			return true
		}
	}
	return false
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
		// Manual "refresh now" trigger: a synchronous scan of every shard.
		// Slow on a large DB (numShards * per-shard query time), but
		// operators may want an immediate aggregate after a config change.
		if err := srv.scanAllShards(r.Context()); err != nil {
			http.Error(w, "scanAllShards: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	us := srv.lastUsage.Load()
	if us == nil {
		http.Error(w, "no usage stats available", http.StatusInternalServerError)
		return
	}

	// Dead-reckon the totals so PUT/eviction bursts since the last shard
	// scan show up on this page immediately. The persisted vs pending split
	// is implementation noise for /usage readers; if anyone needs the
	// breakdown they can read gocached_{blob,pending_blob}_{count,bytes}
	// from /metrics.
	dCount, dBytes := srv.sumShardDeltas()
	live := us.All()
	live.Count += dCount
	live.Size += dBytes

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<html><body><h1>gocached usage stats</h1>\n")
	fmt.Fprintf(w, "<p>Current usage: %v of limit %v</p>\n",
		live, bytesFmt(srv.maxSize))

	// Cohort histogram of stored Actions, by access-time age.
	fmt.Fprintf(w, "<h2>Actions by access-time age</h2>\n")
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

	// Shard scan freshness — how many shards were scanned within each
	// cohort. Watching the smaller-bucket counts drop is the easiest way to
	// spot the stats loop falling behind.
	now := srv.now()
	freshness, oldest, totalShards := srv.shardFreshness(now)
	fmt.Fprintf(w, "<h2>Shard scan freshness</h2>\n")
	fmt.Fprintf(w, "<p>%d of %d shards have a cached scan; staleness target %v.</p>\n",
		totalShards, srv.numShards(), shardStalenessTarget)
	if !oldest.IsZero() {
		fmt.Fprintf(w, "<p>Oldest scan: %v ago (%v).</p>\n",
			durFmt(now.Sub(oldest).Round(time.Second)), oldest.UTC().Format(time.RFC3339))
	}
	fmt.Fprintf(w, "<table border='1' cellpadding=5>\n")
	fmt.Fprintf(w, "<tr><th>Scanned within</th><th>Shards</th></tr>\n")
	for _, b := range shardFreshnessBuckets {
		fmt.Fprintf(w, "<tr><td>&lt;= %v</td><td>%d</td></tr>\n", durFmt(b), freshness[b])
	}
	fmt.Fprintf(w, "</table>\n")

	// Per-shard query duration percentiles, snapshotted from the cached
	// QueryDuration of every persisted shard. The Prometheus histogram
	// gocached_shard_scan_duration_seconds carries the same data over time.
	p25, p50, p90, nDur := srv.shardScanDurationPercentiles()
	fmt.Fprintf(w, "<h2>Per-shard scan duration</h2>\n")
	if nDur == 0 {
		fmt.Fprintf(w, "<p>No shard has been scanned yet.</p>\n")
	} else {
		fmt.Fprintf(w, "<table border='1' cellpadding=5>\n")
		fmt.Fprintf(w, "<tr><th>Percentile</th><th>Duration</th></tr>\n")
		fmt.Fprintf(w, "<tr><td>p25 (n=%d)</td><td>%v</td></tr>\n", nDur, p25.Round(time.Millisecond))
		fmt.Fprintf(w, "<tr><td>p50</td><td>%v</td></tr>\n", p50.Round(time.Millisecond))
		fmt.Fprintf(w, "<tr><td>p90</td><td>%v</td></tr>\n", p90.Round(time.Millisecond))
		fmt.Fprintf(w, "</table>\n")
	}
}

func (srv *Server) serveSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}

	srv.mu.RLock()
	// Make a copy of all session data (excluding the mutex).
	sessions := make([]*sessionData, 0, len(srv.sessions))
	for _, v := range srv.sessions {
		v.mu.Lock()
		sessions = append(sessions, &sessionData{
			expiry:      v.expiry,
			namespaceID: v.namespaceID,
			namespace:   v.namespace,
			claims:      v.claims,
			stats:       v.stats,
		})
		v.mu.Unlock()
	}
	srv.mu.RUnlock()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<html><body><h1>gocached sessions</h1>\n")
	for _, iss := range srv.jwtIssuers {
		fmt.Fprintf(w, "<p>JWT issuer: %s</p>\n", iss)
	}
	fmt.Fprintf(w, "<p>Number of sessions: %d</p>\n", len(sessions))

	fmt.Fprintf(w, "<table border='1' cellpadding=5>\n")
	fmt.Fprintf(w, "<tr><th>Last used</th><th>Expiry time</th><th>Namespace</th><th>Stats</th><th>Claims</th></tr>\n")
	slices.SortFunc(sessions, func(a, b *sessionData) int {
		return a.stats.LastUsed.Compare(b.stats.LastUsed)
	})
	for _, d := range slices.Backward(sessions) {
		lastUsed := "never"
		if !d.stats.LastUsed.IsZero() {
			lastUsed = durFmt(time.Since(d.stats.LastUsed)) + " ago"
		}
		nsLabel := "(global)"
		if d.namespaceID != 0 {
			nsLabel = fmt.Sprintf("%q (id=%d)", d.namespace, d.namespaceID)
		}
		statsJSON, _ := json.MarshalIndent(d.stats, "", "  ")
		claimsJSON, _ := json.MarshalIndent(d.claims, "", "  ")
		fmt.Fprintf(w, "<tr><td>%s</td><td>%s</td><td>%s</td><td><pre>%s</pre></td><td><pre>%s</pre></td></tr>\n",
			lastUsed, d.expiry.Format(time.RFC3339), nsLabel, statsJSON, claimsJSON)
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
