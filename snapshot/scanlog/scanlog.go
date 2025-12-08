package scanlog

import (
	"database/sql"
	"errors"
	"iter"
	"path"

	"github.com/PlakarKorp/kloset/caching/sqlite"
)

type EntryKind int

const (
	KindDirectory EntryKind = 1
	KindFile      EntryKind = 2
)

type ScanBatch struct {
	db   *sqlite.SQLiteCache
	recs []batchRec
}

type batchRec struct {
	kind    EntryKind
	path    string
	payload []byte
}

type ScanLog struct {
	db *sqlite.SQLiteCache
}

func New(path string) (*ScanLog, error) {
	db, err := sqlite.New(path, "scanlog", &sqlite.Options{
		DeleteOnClose: true,
		Compressed:    true,
	})

	if err != nil {
		return nil, err
	}

	if err := createSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &ScanLog{
		db: db,
	}, nil
}

func createSchema(db *sqlite.SQLiteCache) error {
	const schema = `
	CREATE TABLE IF NOT EXISTS entries (
		kind    INTEGER NOT NULL, -- 1 = dir, 2 = file
		path    TEXT    NOT NULL,
		parent  TEXT    NOT NULL,
		payload BLOB    NOT NULL,
		PRIMARY KEY (kind, path)
	) WITHOUT ROWID;

	CREATE INDEX IF NOT EXISTS entries_parent_idx
	ON entries(parent, kind, path);
	`

	_, err := db.Exec(schema)
	return err
}

func (s *ScanLog) Close() error {
	return s.db.Close()
}

func (s *ScanLog) PutDirectory(p string, payload []byte) error {
	return s.put(KindDirectory, p, payload)
}

func (s *ScanLog) PutFile(p string, payload []byte) error {
	return s.put(KindFile, p, payload)
}

func (s *ScanLog) put(kind EntryKind, p string, payload []byte) error {
	parent := path.Dir(p)
	stored := s.db.Encode(payload)
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO entries (kind, path, parent, payload) VALUES (?, ?, ?, ?)`,
		int(kind), p, parent, stored,
	)
	return err
}

func (s *ScanLog) GetDirectory(p string) ([]byte, error) {
	return s.get(KindDirectory, p)
}

func (s *ScanLog) GetFile(p string) ([]byte, error) {
	return s.get(KindFile, p)
}

func (s *ScanLog) get(kind EntryKind, p string) ([]byte, error) {
	var stored []byte
	err := s.db.QueryRow(
		`SELECT payload FROM entries WHERE kind = ? AND path = ?`,
		int(kind), p,
	).Scan(&stored)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	payload, err := s.db.Decode(stored)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (s *ScanLog) NewBatch() *ScanBatch {
	return &ScanBatch{db: s.db}
}

func (b *ScanBatch) PutDirectory(p string, payload []byte) error {
	return b.put(KindDirectory, p, payload)
}

func (b *ScanBatch) PutFile(p string, payload []byte) error {
	return b.put(KindFile, p, payload)
}

func (b *ScanBatch) put(kind EntryKind, p string, payload []byte) error {
	stored := b.db.Encode(payload)

	b.recs = append(b.recs, batchRec{
		kind:    kind,
		path:    p,
		payload: stored,
	})
	return nil
}

func (b *ScanBatch) Count() uint32 {
	return uint32(len(b.recs))
}

func (b *ScanBatch) Commit() error {
	if len(b.recs) == 0 {
		return nil
	}

	tx, err := b.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO entries (kind, path, parent, payload) VALUES (?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, rec := range b.recs {
		parent := path.Dir(rec.path)
		if _, err := stmt.Exec(int(rec.kind), rec.path, parent, rec.payload); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	b.recs = nil
	return nil
}

type Entry struct {
	Kind    EntryKind
	Path    string
	Payload []byte
}

func (s *ScanLog) list(kind EntryKind, prefix string, reverse bool) iter.Seq[Entry] {
	return func(yield func(Entry) bool) {
		hi := prefix + string([]byte{0xFF})

		order := "ASC"
		if reverse {
			order = "DESC"
		}

		query := `
		SELECT kind, path
		FROM entries
		WHERE path >= ? AND path < ?
		`
		args := []any{prefix, hi}

		if kind != 0 {
			query += ` AND kind = ?`
			args = append(args, int(kind))
		}

		query += ` ORDER BY path ` + order

		rows, err := s.db.Query(query, args...)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var kInt int
			var p string
			if err := rows.Scan(&kInt, &p); err != nil {
				return
			}

			if !yield(Entry{
				Kind: EntryKind(kInt),
				Path: p,
			}) {
				return
			}
		}
	}
}

func (s *ScanLog) ListFiles(kind EntryKind, prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(KindFile, prefix, reverse)
}

func (s *ScanLog) ListDirectories(prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(KindDirectory, prefix, reverse)
}

func (s *ScanLog) ListPathnames(prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(0, prefix, reverse)
}

func (s *ScanLog) ListDirectPathnames(parent string, reverse bool) iter.Seq[Entry] {

	return func(yield func(Entry) bool) {
		order := "ASC"
		if reverse {
			order = "DESC"
		}

		query := `
		SELECT kind, path, payload
		FROM entries
		WHERE parent = ? AND parent != path
		`
		args := []any{parent}

		query += ` ORDER BY path ` + order

		rows, err := s.db.Query(query, args...)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var k int
			var p string
			var stored []byte
			if err := rows.Scan(&k, &p, &stored); err != nil {
				return
			}

			payload, err := s.db.Decode(stored)
			if err != nil {
				return
			}

			if !yield(Entry{
				Kind:    EntryKind(k),
				Path:    p,
				Payload: payload,
			}) {
				return
			}
		}
	}
}
