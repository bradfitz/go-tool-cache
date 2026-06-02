// Copyright (c) Tailscale Inc & AUTHORS
// SPDX-License-Identifier: BSD-3-Clause

package gocached

// This file holds the perf regression test. It always runs in CI (so
// refactors that change query shape are caught by the assertions immediately),
// but defaults to a tiny row count so normal CI takes a fraction of a
// second. Operators investigating real-world scale set
// -performance-test-rows to e.g. 250_000_000 and -performance-test-dir to a
// path on a fast disk; the seed is reused across runs.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	perfTestRows = flag.Int("performance-test-rows", 100,
		"target number of (Blob, Action) pairs to populate before measuring; default 100 so plain `go test` finishes in a fraction of a second. Bump to e.g. 250_000_000 for real-scale measurements; only the deficit is inserted on each run, so re-runs with the same value are instant.")
	perfTestDir = flag.String("performance-test-dir", "",
		"directory holding the pre-seeded perf test DB. If empty (the default), a fresh per-run t.TempDir() is used. Set to a persistent path (with $GOCACHED_PERFTEST_DIR as an alternative) when -performance-test-rows is large enough that you don't want to re-seed every run.")
)

// perfSeedBatchSize is the number of rows inserted per transaction during
// seeding. 5000 keeps each multi-row INSERT comfortably below SQLite's
// default 32766 host-parameter limit (5000 * 4 = 20000 < 32766) and gives a
// reasonable bytes-per-syscall ratio for fsync-free seed inserts.
const perfSeedBatchSize = 5000

func TestPerfQueries(t *testing.T) {
	dir := *perfTestDir
	if dir == "" {
		if v := os.Getenv("GOCACHED_PERFTEST_DIR"); v != "" {
			dir = v
		} else {
			// Default: per-test temp dir so a normal `go test` doesn't
			// leave a seeded DB hanging around. Operators running real-scale
			// measurements set -performance-test-dir explicitly to get
			// seed reuse across runs.
			dir = t.TempDir()
		}
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	t.Logf("perf test dir: %s (rows=%d)", dir, *perfTestRows)

	srv, err := NewServer(
		WithDir(dir),
		WithLogf(t.Logf),
		withoutBackgroundLoops(),
		WithShardPrefixLen(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })

	n := *perfTestRows
	if err := perfSeed(t, srv, n); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Confirm the eviction query plan still uses idx_actions_access even on
	// a 250M-row table — that's the whole point of INDEXED BY, but it's
	// worth re-asserting at scale because the planner picks differently as
	// table cardinality grows.
	t.Run("EvictionQueryPlan_atScale", func(t *testing.T) {
		row := srv.db.QueryRow("EXPLAIN QUERY PLAN "+evictionCandidateQuery, 0, cleanupBatchSize)
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
	})

	timed := func(name string, f func() error) time.Duration {
		t.Helper()
		start := time.Now()
		if err := f(); err != nil {
			t.Errorf("%s failed: %v", name, err)
			return 0
		}
		d := time.Since(start)
		t.Logf("%-44s %v", name, d)
		return d
	}

	// usageStats is a plain in-memory aggregate read; it doesn't touch
	// SQLite at all. The benchmark is here to lock in that property: any
	// future regression that re-introduces a per-call DB scan would blow
	// past the sub-millisecond threshold.
	aggRead := timed("usageStats (in-memory aggregate read)", func() error {
		_, err := srv.usageStats()
		return err
	})

	perShard := timed(fmt.Sprintf("scanShard (1 shard, ~%dM rows)", n/256/1_000_000), func() error {
		_, err := srv.scanShard(context.Background(), 0)
		return err
	})

	cleanup := timed(fmt.Sprintf("evictOldestActions (%d-row batch)", cleanupBatchSize), func() error {
		_, err := srv.evictOldestActions(context.Background(), math.MaxInt64, cleanupBatchSize, math.MaxInt64)
		return err
	})

	// Legacy world-scoped GROUP BY: the query gocached used to run every
	// 5 minutes. It's here as a baseline so reviewers can see how much
	// faster the per-shard approach is and as an early-warning if SQLite
	// ever gets dramatically faster at this shape.
	legacy := timed(fmt.Sprintf("legacy GROUP BY cleanCandidates (%d-row LIMIT)", cleanupBatchSize), func() error {
		rows, err := srv.db.Query(`
			SELECT b.BlobID, MAX(a.AccessTime), b.StoredSize
			FROM Blobs b LEFT JOIN Actions a ON b.BlobID = a.BlobID
			GROUP BY b.BlobID
			HAVING MAX(a.AccessTime) <= ?
			ORDER BY MAX(a.AccessTime)
			LIMIT ?`, int64(math.MaxInt64), cleanupBatchSize)
		if err != nil {
			return err
		}
		defer rows.Close()
		var blobID, accessTime, storedSize int64
		for rows.Next() {
			if err := rows.Scan(&blobID, &accessTime, &storedSize); err != nil {
				return err
			}
		}
		return rows.Err()
	})

	if cleanup > 0 {
		t.Logf("legacy/incremental cleanup ratio: %.1fx slower", legacy.Seconds()/cleanup.Seconds())
	}

	// Wall-clock regression thresholds. These are deliberately loose for
	// laptop/dev hardware; tighten on the CI box once we have a baseline
	// run there. The intent is "scream loudly if any of these queries
	// quietly grows by an order of magnitude," not "lock in a tight SLO."
	if aggRead > 1*time.Millisecond {
		t.Errorf("usageStats took %v; want sub-millisecond (it should not hit SQLite)", aggRead)
	}
	if perShard > 30*time.Second {
		t.Errorf("scanShard took %v on ~%dM rows; want < 30s", perShard, n/256/1_000_000)
	}
	if cleanup > 2*time.Second {
		t.Errorf("evictOldestActions took %v on %d-row batch; want < 2s (idx_actions_access range scan)", cleanup, cleanupBatchSize)
	}
}

// perfSeed brings the DB up to at least n (Blob, Action) pairs. If the DB
// already has that many it returns immediately so re-runs are fast; if it's
// short, only the deficit is inserted. Per-batch work is wrapped in one
// transaction so a Ctrl-C resumes cleanly at the last complete batch.
func perfSeed(t *testing.T, srv *Server, n int) error {
	t.Helper()

	var seq sql.NullInt64
	err := srv.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'Blobs'`).Scan(&seq)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("query sqlite_sequence: %w", err)
	}
	cur := int(seq.Int64)
	if cur >= n {
		t.Logf("DB already has %d Blobs (>= %d target); reusing seed", cur, n)
		return nil
	}

	t.Logf("seeding rows %d..%d", cur, n)
	// PRAGMA synchronous=OFF makes the seed dramatically faster at the
	// cost of crash safety. Acceptable for a synthetic test DB; the
	// process explicitly runs with no other writers (background loops are
	// disabled via withoutBackgroundLoops). The pragma resets on Close.
	if _, err := srv.db.Exec(`PRAGMA synchronous = OFF`); err != nil {
		return fmt.Errorf("PRAGMA synchronous: %w", err)
	}
	// 512 MiB page cache so the bulk inserts thrash less.
	if _, err := srv.db.Exec(`PRAGMA cache_size = -524288`); err != nil {
		return fmt.Errorf("PRAGMA cache_size: %w", err)
	}

	start := time.Now()
	lastLog := start
	for i := cur; i < n; i += perfSeedBatchSize {
		end := min(i+perfSeedBatchSize, n)
		if err := perfSeedBatch(srv, i, end); err != nil {
			return fmt.Errorf("seed batch [%d, %d): %w", i, end, err)
		}
		if time.Since(lastLog) > 30*time.Second {
			elapsed := time.Since(start)
			rate := float64(end-cur) / elapsed.Seconds()
			remaining := time.Duration(float64(n-end) / rate * float64(time.Second))
			t.Logf("seed progress: %d / %d (%.1f%%, elapsed %v, ETA %v)",
				end, n, 100*float64(end)/float64(n),
				elapsed.Round(time.Second), remaining.Round(time.Second))
			lastLog = time.Now()
		}
	}
	t.Logf("seeded %d rows in %v", n-cur, time.Since(start).Round(time.Second))
	return nil
}

// perfSeedBatch inserts (Blob, Action) pairs for indices [start, end) in a
// single transaction. Synthetic SHA256s come from sha256(itoa(i)) so the
// resulting hashes are uniformly distributed across all shards. BlobIDs are
// the AUTOINCREMENT-assigned sequential values; we know them in advance
// because seeding always continues from sqlite_sequence's max.
func perfSeedBatch(srv *Server, start, end int) error {
	tx, err := srv.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rowCount := end - start

	var sb strings.Builder
	sb.Grow(64 + rowCount*8)
	sb.WriteString("INSERT INTO Blobs (SHA256, StoredSize, UncompressedSize) VALUES ")
	args := make([]any, 0, rowCount*3)
	for i := start; i < end; i++ {
		if i > start {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?)")
		h := sha256.Sum256([]byte(strconv.Itoa(i)))
		args = append(args, hex.EncodeToString(h[:]), int64(100), int64(100))
	}
	if _, err := tx.Exec(sb.String(), args...); err != nil {
		return fmt.Errorf("insert Blobs: %w", err)
	}

	sb.Reset()
	sb.Grow(96 + rowCount*16)
	sb.WriteString("INSERT INTO Actions (NamespaceID, ActionID, BlobID, AltOutputID, CreateTime, AccessTime) VALUES ")
	args = args[:0]
	for i := start; i < end; i++ {
		if i > start {
			sb.WriteByte(',')
		}
		sb.WriteString("(0,?,?,'',?,?)")
		h := sha256.Sum256([]byte(strconv.Itoa(i)))
		args = append(args,
			hex.EncodeToString(h[:]),
			int64(i+1),      // BlobID; matches the AUTOINCREMENT sequence
			int64(1234+i),   // CreateTime
			int64(1234+i),   // AccessTime — monotonically increasing makes cleanup deterministic
		)
	}
	if _, err := tx.Exec(sb.String(), args...); err != nil {
		return fmt.Errorf("insert Actions: %w", err)
	}

	return tx.Commit()
}
