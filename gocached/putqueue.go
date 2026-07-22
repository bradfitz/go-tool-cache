package gocached

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"sync"

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
}

func newPutQueue(srv *Server, dir string) *putQueue {
	return &putQueue{
		srv:      srv,
		dir:      dir,
		bytesSem: semaphore.NewWeighted(putQueuePendingBytesCap),
		countSem: semaphore.NewWeighted(putQueuePendingCountCap),
		pending:  make(map[actionKey]*pendingPut),
	}
}

// reserve blocks until the queue has room for a PUT of the given declared
// content length, or ctx is done. It returns the reserved byte weight, which
// the caller must eventually return via unreserve (typically by retiring the
// pending entry). The weight is clamped to the total capacity so a single
// blob bigger than the cap is still admitted (alone).
func (q *putQueue) reserve(ctx context.Context, contentLength int64) (reserved int64, err error) {
	reserved = min(contentLength, putQueuePendingBytesCap)
	if err := q.bytesSem.Acquire(ctx, reserved); err != nil {
		return 0, err
	}
	if err := q.countSem.Acquire(ctx, 1); err != nil {
		q.bytesSem.Release(reserved)
		return 0, err
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
	return false
}

// lookup returns the pending entry for key, if any.
func (q *putQueue) lookup(key actionKey) (*pendingPut, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	p, ok := q.pending[key]
	return p, ok
}

// retire removes p from the pending map and releases its reservation. The
// caller is responsible for p's spool file (renaming it into the hot tier,
// or deleting it).
func (q *putQueue) retire(p *pendingPut) {
	q.mu.Lock()
	delete(q.pending, p.key)
	q.mu.Unlock()
	q.unreserve(p.reservedBytes)
}

// len returns the number of pending entries.
func (q *putQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
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
