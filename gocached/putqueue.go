package gocached

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pierrec/lz4/v4"
	"golang.org/x/sync/semaphore"
)

// putQueueDirName is the name of the spool directory holding blob bytes for
// PUTs whose metadata hasn't been committed to SQLite yet. It lives on the
// hot tier's filesystem when tiering is enabled (so a spooled blob can be
// renamed into the hot tier) and in the main blob directory otherwise. The
// leading dot keeps the hot tier startup scan from indexing it.
const putQueueDirName = ".put-queue"

const (
	// putQueuePendingBytesCap bounds the total declared size of pending
	// PUTs, which in turn bounds the spool directory's disk usage and the
	// backlog of blobs awaiting a copy into the main blob directory. It
	// bounds disk, not memory (the count cap bounds memory), so it can be
	// generous: individual PUTs run to hundreds of MB and each holds its
	// reservation for its whole pending window (spool, copy, batch flush;
	// or a few minutes of TCP keepalive if the client dies mid-body), so
	// a cap near a single PUT's size would serialize the big ones. The
	// spool must still fit on the hot tier's disk alongside the hot
	// capacity when tiering is enabled.
	putQueuePendingBytesCap = 16 << 30

	// putQueuePendingCountCap bounds the number of pending PUTs, which in
	// turn bounds the memory held by the pending map (each entry retains at
	// most smallObjectSize bytes of inline data).
	putQueuePendingCountCap = 8192

	// putFlushInterval is how long the flusher waits after the first item
	// of a batch before committing it, giving later PUTs a chance to share
	// the transaction. GETs are served from the pending map in the
	// meantime, so this delays only durability, not visibility.
	putFlushInterval = time.Second

	// putFlushBatchCap is the maximum number of pending PUTs committed in
	// one SQLite transaction; a full batch flushes without waiting for
	// putFlushInterval.
	putFlushBatchCap = 512

	// numPutMovers is how many goroutines copy spooled blobs into the main
	// blob directory. The main directory may be a high-latency network
	// filesystem, so several copies proceed in parallel.
	numPutMovers = 8

	// putCopyRetries is how many times a mover attempts a blob's copy into
	// the main blob directory before the pending PUT is dropped.
	putCopyRetries = 3

	// putFlushRetries is how many times the flusher attempts a metadata
	// batch before its pending PUTs are dropped.
	putFlushRetries = 10
)

// pendingPut is one PUT that has been accepted but whose metadata has not
// yet been committed to SQLite.
type pendingPut struct {
	key              actionKey
	sha256hex        string
	storedSize       int64  // bytes stored: len(smallData) if inline, file size (possibly lz4) otherwise
	uncompressedSize int64  // original uncompressed content size
	altOutputID      string // the PUT's outputID, or "" if it equals sha256hex
	createTime       int64  // unix seconds
	smallData        []byte // non-nil iff the object is stored inline (<= smallObjectSize)
	queueFile        string // path of the spool file holding the blob bytes; "" if inline
	reservedBytes    int64  // semaphore weight to release when the entry retires
}

// blobName returns the base filename for p's blob in the main and hot blob
// directories: the SHA-256 hex, with ".lz4" appended when the spooled bytes
// are compressed (the same policy spoolBlob used to write them).
func (p *pendingPut) blobName() string {
	if p.uncompressedSize >= lz4CompressThreshold {
		return p.sha256hex + ".lz4"
	}
	return p.sha256hex
}

// putQueue tracks PUTs that have been accepted (and their blob bytes safely
// spooled to the local disk or held in memory) but whose metadata hasn't
// been committed to SQLite yet. GETs consult the pending map before the
// database so a just-written PUT is immediately readable.
type putQueue struct {
	srv *Server
	dir string // spool directory for blob bytes

	// bytesSem and countSem implement backpressure: a PUT reserves its
	// declared size (clamped to the cap) and one entry slot before spooling
	// anything, and the reservation is released when the entry retires.
	// Blocked reservations abort when the HTTP request context is canceled.
	bytesSem *semaphore.Weighted
	countSem *semaphore.Weighted

	// mu is a leaf mutex: no other lock is acquired while holding it.
	mu      sync.Mutex
	pending map[actionKey]*pendingPut
	bytes   int64 // sum of reservedBytes over pending

	// moverCh feeds spooled big blobs to the movers; flushCh feeds entries
	// whose bytes are settled (inline, or copied to the main blob dir) to
	// the metadata flusher. Both are buffered to putQueuePendingCountCap,
	// which countSem guarantees is never exceeded, so sends don't block.
	moverCh chan *pendingPut
	flushCh chan *pendingPut
}

func newPutQueue(srv *Server, dir string) *putQueue {
	return &putQueue{
		srv:      srv,
		dir:      dir,
		bytesSem: semaphore.NewWeighted(putQueuePendingBytesCap),
		countSem: semaphore.NewWeighted(putQueuePendingCountCap),
		pending:  make(map[actionKey]*pendingPut),
		moverCh:  make(chan *pendingPut, putQueuePendingCountCap),
		flushCh:  make(chan *pendingPut, putQueuePendingCountCap),
	}
}

// start launches the background movers and the metadata flusher. It is not
// called under disableBackgroundLoops; tests drive the pipeline with
// [Server.drainPendingPuts] instead.
func (q *putQueue) start(ctx context.Context) {
	for range numPutMovers {
		go q.moverLoop(ctx)
	}
	go q.flusherLoop(ctx)
}

// reserve blocks until the queue has room for a PUT of the given declared
// content length, or ctx is done. It returns the reserved byte weight, which
// the caller must eventually return via unreserve (typically by retiring the
// pending entry). The weight is clamped to the total capacity so a single
// blob bigger than the cap is still admitted (alone).
func (q *putQueue) reserve(ctx context.Context, contentLength int64) (reserved int64, err error) {
	reserved = min(contentLength, putQueuePendingBytesCap)
	if !q.bytesSem.TryAcquire(reserved) {
		q.srv.m.PutQueueBlocked.Add(1)
		if err := q.bytesSem.Acquire(ctx, reserved); err != nil {
			return 0, err
		}
	}
	if !q.countSem.TryAcquire(1) {
		q.srv.m.PutQueueBlocked.Add(1)
		if err := q.countSem.Acquire(ctx, 1); err != nil {
			q.bytesSem.Release(reserved)
			return 0, err
		}
	}
	return reserved, nil
}

// unreserve returns a reservation made by reserve.
func (q *putQueue) unreserve(reserved int64) {
	q.countSem.Release(1)
	q.bytesSem.Release(reserved)
}

// insert adds p to the pending map. It reports whether an entry with p's key
// already exists, in which case p is not added: the first PUT wins, matching
// the INSERT OR IGNORE semantics of the Actions table.
func (q *putQueue) insert(p *pendingPut) (dup bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[p.key]; ok {
		return true
	}
	q.pending[p.key] = p
	q.bytes += p.reservedBytes
	return false
}

// enqueue registers p as pending and hands it to the background pipeline:
// spooled blobs go to a mover for the copy into the main blob directory,
// inline ones straight to the metadata flusher. It reports whether an entry
// with p's key was already pending, in which case p is not queued: the
// first PUT wins.
func (q *putQueue) enqueue(p *pendingPut) (dup bool) {
	if q.insert(p) {
		return true
	}
	if p.queueFile != "" {
		q.moverCh <- p
	} else {
		q.flushCh <- p
	}
	return false
}

// lookup returns the pending entry for key, if any.
func (q *putQueue) lookup(key actionKey) (*pendingPut, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	p, ok := q.pending[key]
	return p, ok
}

// retire removes p from the pending map and releases its reservation,
// reporting whether p was still pending. A false return means a concurrent
// flush or drop got there first and the caller must not touch p's spool
// file. The caller that wins is responsible for the spool file (renaming it
// into the hot tier, or deleting it).
func (q *putQueue) retire(p *pendingPut) bool {
	q.mu.Lock()
	cur, ok := q.pending[p.key]
	if ok = ok && cur == p; ok {
		delete(q.pending, p.key)
		q.bytes -= p.reservedBytes
	}
	q.mu.Unlock()
	if ok {
		q.unreserve(p.reservedBytes)
	}
	return ok
}

// pendingStats returns the number of pending entries and the sum of their
// reserved bytes.
func (q *putQueue) pendingStats() (count int, bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending), q.bytes
}

// drop abandons p after repeated failures: the client already saw success,
// but a cache is allowed to forget. The spool file is deleted and the entry
// stops being served.
func (q *putQueue) drop(p *pendingPut) {
	if !q.retire(p) {
		return
	}
	q.srv.m.PutQueueDropped.Add(1)
	if p.queueFile != "" {
		os.Remove(p.queueFile)
	}
}

// spoolBlob writes the contents of r, of the given uncompressed size, to a
// new spool file in the queue directory, lz4-compressing it if it's big
// enough (the same policy as the main blob directory, so the file's bytes
// are exactly what the other tiers want and installing it is a rename or a
// dumb copy). It returns the file's path and on-disk size. The file keeps
// its randomly generated name for its whole life in the spool; the caller
// must eventually rename or remove it.
func (q *putQueue) spoolBlob(size int64, r io.Reader) (diskSize int64, path string, retErr error) {
	compress := size >= lz4CompressThreshold
	tf, err := os.CreateTemp(q.dir, fmt.Sprintf("put-%d-*", q.srv.now().Unix()))
	if err != nil {
		return 0, "", err
	}
	defer func() {
		tf.Close()
		if retErr != nil {
			os.Remove(tf.Name())
		}
	}()

	lr := io.LimitReader(r, size+1)
	var dst io.Writer = tf
	var lzw *lz4.Writer
	if compress {
		lzw = lz4.NewWriter(tf)
		if err := lzw.Apply(lz4.SizeOption(uint64(size))); err != nil {
			return 0, "", err
		}
		dst = lzw
	}

	n, err := io.Copy(dst, lr)
	if err != nil {
		return 0, "", err
	}
	if n != size {
		return 0, "", fmt.Errorf("wrote %d bytes; wanted %d", n, size)
	}
	if lzw != nil {
		if err := lzw.Close(); err != nil {
			return 0, "", err
		}
	}
	if err := tf.Close(); err != nil {
		return 0, "", err
	}

	fi, err := os.Stat(tf.Name())
	if err != nil {
		return 0, "", err
	}
	return fi.Size(), tf.Name(), nil
}

// moverLoop copies spooled blobs into the main blob directory and passes
// them on to the flusher. Several movers run concurrently since the main
// directory may be a high-latency network filesystem.
func (q *putQueue) moverLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-q.moverCh:
			q.moveToFlush(ctx, p)
		}
	}
}

// moveToFlush copies p's blob into the main blob directory, retrying a few
// times, then hands p to the flusher. After the last failed attempt p is
// dropped.
func (q *putQueue) moveToFlush(ctx context.Context, p *pendingPut) {
	for try := range putCopyRetries {
		if try > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second << (try - 1)):
			}
		}
		err := q.copyToMain(p)
		if err == nil {
			q.flushCh <- p
			return
		}
		q.srv.logf("put-queue: copying blob %v to main dir: %v", p.sha256hex, err)
		q.srv.m.PutQueueCopyErrs.Add(1)
	}
	q.drop(p)
}

// copyToMain installs p's spooled blob into the main blob directory, leaving
// the spool file in place: it remains the bytes served for GETs of the
// pending entry, and finishCommitted may later rename it into the hot tier.
// A blob already present with the expected size is left alone; blobs are
// content-addressed, so it holds the same bytes.
func (q *putQueue) copyToMain(p *pendingPut) error {
	srv := q.srv
	name := p.blobName()
	target := filepath.Join(srv.dir, name[:2], name)
	if fi, err := os.Stat(target); err == nil && fi.Size() == p.storedSize {
		return nil
	}

	src, err := os.Open(p.queueFile)
	if err != nil {
		return err
	}
	defer src.Close()

	tf, err := os.CreateTemp(srv.dir, fmt.Sprintf("upload-%d-*", srv.now().Unix()))
	if err != nil {
		return err
	}
	tmpName := tf.Name()
	_, copyErr := io.Copy(tf, src)
	if err := cmp.Or(copyErr, tf.Close()); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// flusherLoop batches entries from flushCh into single SQLite transactions.
// A batch commits when it reaches putFlushBatchCap entries or when
// putFlushInterval has passed since its first entry arrived.
func (q *putQueue) flusherLoop(ctx context.Context) {
	for {
		var first *pendingPut
		select {
		case <-ctx.Done():
			return
		case first = <-q.flushCh:
		}
		batch := []*pendingPut{first}
		timer := time.NewTimer(putFlushInterval)
	collect:
		for len(batch) < putFlushBatchCap {
			select {
			case <-ctx.Done():
				break collect
			case p := <-q.flushCh:
				batch = append(batch, p)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		q.flushBatchWithRetry(ctx, batch)
	}
}

// flushBatchWithRetry commits batch, retrying transient failures. The
// inserts are idempotent, so retrying a batch whose commit outcome was
// unknown is safe. After the last failed attempt the whole batch is
// dropped.
func (q *putQueue) flushBatchWithRetry(ctx context.Context, batch []*pendingPut) {
	for try := range putFlushRetries {
		if try > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		err := q.flushBatch(batch)
		if err == nil {
			return
		}
		q.srv.logf("put-queue: flushing %d-item metadata batch (attempt %d): %v", len(batch), try+1, err)
	}
	for _, p := range batch {
		q.drop(p)
	}
}

// flushBatch commits the metadata for every entry of batch in one SQLite
// transaction, then finishes each entry.
func (q *putQueue) flushBatch(batch []*pendingPut) error {
	srv := q.srv
	srv.sqliteWriteMu.Lock()
	defer srv.sqliteWriteMu.Unlock()

	if srv.writeConn == nil {
		var err error
		srv.writeConn, err = srv.db.Conn(context.Background())
		if err != nil {
			return fmt.Errorf("getting writeConn: %w", err)
		}
	}
	tx, err := srv.writeConn.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dups := make([]bool, len(batch))
	for i, p := range batch {
		dup, err := srv.insertPutTx(tx, p)
		if err != nil {
			return fmt.Errorf("inserting action %v: %w", p.key.ActionID, err)
		}
		dups[i] = dup
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	srv.m.PutQueueFlushes.Add(1)
	srv.m.PutQueueFlushedItems.Add(int64(len(batch)))
	for i, p := range batch {
		q.finishCommitted(p, dups[i])
	}
	return nil
}

// finishCommitted retires p after its metadata committed: it updates the
// shard deltas that gate size-based cleanup and installs the spool file into
// the hot tier (a cheap same-filesystem rename) or deletes it. Once the
// entry leaves the pending map, GETs find the committed row in SQLite
// instead, so there is no window where the object is unreadable.
func (q *putQueue) finishCommitted(p *pendingPut, dup bool) {
	if !q.retire(p) {
		return // a concurrent drain or drop already handled it
	}
	srv := q.srv
	if dup {
		srv.m.PutQueueFlushDups.Add(1)
	} else {
		srv.addBlobDelta(p.sha256hex, +1, +p.storedSize)
	}
	if p.queueFile == "" {
		return
	}
	// A dup action doesn't necessarily reference our blob (the earlier
	// action's blob won), so don't promote it into the hot tier.
	if !dup && srv.hot != nil && srv.hot.ready.Load() {
		name := p.blobName()
		target := srv.hotFilepath(name)
		err := os.MkdirAll(filepath.Dir(target), 0750)
		if err == nil {
			err = os.Rename(p.queueFile, target)
		}
		if err == nil {
			srv.hot.add(name, p.storedSize)
			srv.evictHotIfOver()
			return
		}
		srv.logf("hot tier: installing spooled blob %v: %v", name, err)
		srv.m.HotWriteErrs.Add(1)
	}
	os.Remove(p.queueFile)
}

// drainPendingPuts synchronously runs the whole pipeline for every pending
// PUT: the copies into the main blob directory, then one metadata batch
// flush. Tests use it in place of the background loops, and Close uses it
// to settle the queue on shutdown. Entries whose copy fails are dropped,
// as the movers would after retries.
func (srv *Server) drainPendingPuts() error {
	q := srv.putq
	if q == nil {
		return nil
	}
	q.mu.Lock()
	batch := make([]*pendingPut, 0, len(q.pending))
	for _, p := range q.pending {
		batch = append(batch, p)
	}
	q.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}

	toFlush := batch[:0]
	for _, p := range batch {
		if p.queueFile != "" {
			if err := q.copyToMain(p); err != nil {
				srv.logf("put-queue: copying blob %v to main dir: %v", p.sha256hex, err)
				srv.m.PutQueueCopyErrs.Add(1)
				q.drop(p)
				continue
			}
		}
		toFlush = append(toFlush, p)
	}
	if len(toFlush) == 0 {
		return nil
	}
	return q.flushBatch(toFlush)
}

// insertPutTx runs the two metadata inserts for p inside tx: the Blobs
// upsert and the Actions insert. It reports whether the action already
// existed (a duplicate PUT). The caller is responsible for holding
// sqliteWriteMu and committing tx.
func (s *Server) insertPutTx(tx *sql.Tx, p *pendingPut) (dup bool, err error) {
	var blobID int64
	err = tx.QueryRow(`INSERT INTO Blobs (SHA256, StoredSize, UncompressedSize, SmallData)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(SHA256) DO UPDATE SET SHA256=excluded.SHA256
		RETURNING BlobID;
`, p.sha256hex, p.storedSize, p.uncompressedSize, p.smallData).Scan(&blobID)
	if err != nil {
		return false, fmt.Errorf("Blobs insert: %w", err)
	}

	res, err := tx.Exec(`INSERT OR IGNORE INTO Actions (NamespaceID, ActionID, BlobID, AltOutputID, CreateTime, AccessTime)
	VALUES (?, ?, ?, ?, ?, ?)`,
		p.key.NamespaceID,
		p.key.ActionID,
		blobID,
		p.altOutputID,
		p.createTime,
		p.createTime,
	)
	if err != nil {
		return false, fmt.Errorf("Actions insert: %w", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("Actions rows affected: %w", err)
	}
	return affected == 0, nil
}
