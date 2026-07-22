package gocached

import (
	"database/sql"
	"fmt"
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
