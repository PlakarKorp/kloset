package scanlog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"os"
	"path"
	"strings"

	"github.com/golang/snappy"
	_ "github.com/mattn/go-sqlite3"
)

type EntryKind int

const (
	KindDirectory EntryKind = 1
	KindFile      EntryKind = 2
)

type ScanBatch struct {
	db   *sql.DB
	recs []batchRec
}

type batchRec struct {
	kind    EntryKind
	path    string
	payload []byte
}

type ScanLog struct {
	db   *sql.DB
	path string
}

func New(ctx context.Context, dsn string) (*ScanLog, error) {
	if dsn == "" {
		dsn = ":memory:"
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	if err := configureSQLite(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := createSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &ScanLog{
		db:   db,
		path: dsn,
	}, nil
}

func configureSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode = WAL;", // one-writer WAL, good for cache
		"PRAGMA synchronous = OFF;",  // speed; scanlog is scratch
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA mmap_size = 0;",
		"PRAGMA cache_size = -20000;",      // ~20MB
		"PRAGMA locking_mode = EXCLUSIVE;", // single-process owner
		"PRAGMA busy_timeout = 5000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("pragma %q failed: %w", p, err)
		}
	}
	return nil
}

func createSchema(db *sql.DB) error {
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

func encodePayload(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	return snappy.Encode(nil, raw)
}

func decodePayload(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return stored, nil
	}
	return snappy.Decode(nil, stored)
}

func (s *ScanLog) Close() error {
	err := s.db.Close()

	if s.path != "" {
		os.RemoveAll(s.path)
	}
	return err
}

// PutDirectory stores a directory entry at path.
func (s *ScanLog) PutDirectory(p string, payload []byte) error {
	return s.put(KindDirectory, p, payload)
}

// PutFile stores a file entry at path.
func (s *ScanLog) PutFile(p string, payload []byte) error {
	return s.put(KindFile, p, payload)
}

func (s *ScanLog) put(kind EntryKind, p string, payload []byte) error {
	parent := path.Dir(p)
	stored := encodePayload(payload)
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO entries (kind, path, parent, payload) VALUES (?, ?, ?, ?)`,
		int(kind), p, parent, stored,
	)
	return err
}

// GetDirectory returns the serialized directory entry at path, or nil if not found.
func (s *ScanLog) GetDirectory(p string) ([]byte, error) {
	return s.get(KindDirectory, p)
}

// GetFile returns the serialized file entry at path, or nil if not found.
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

	payload, err := decodePayload(stored)
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
	stored := encodePayload(payload)

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

// List returns Entry{Kind, Path, Payload} for a prefix range.
// kind == 0 means "any kind".
func (s *ScanLog) List(prefix string, reverse bool, kind EntryKind) iter.Seq[Entry] {
	return func(yield func(Entry) bool) {
		hi := prefix + string([]byte{0xFF})

		order := "ASC"
		if reverse {
			order = "DESC"
		}

		query := `
			SELECT kind, path, payload
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
			var stored []byte
			if err := rows.Scan(&kInt, &p, &stored); err != nil {
				return
			}

			payload, err := decodePayload(stored)
			if err != nil {
				return
			}

			if !yield(Entry{
				Kind:    EntryKind(kInt),
				Path:    p,
				Payload: payload,
			}) {
				return
			}
		}
	}
}

func (s *ScanLog) ListFiles(prefix string, reverse bool) iter.Seq[Entry] {
	return s.List(prefix, reverse, KindFile)
}

func (s *ScanLog) ListDirectories(prefix string, reverse bool) iter.Seq[Entry] {
	return s.List(prefix, reverse, KindDirectory)
}

func (s *ScanLog) ListAll(prefix string, reverse bool) iter.Seq[Entry] {
	return s.List(prefix, reverse, 0)
}

// ListDirectPathnames returns only direct children of `parent` (no grandchildren),
// using the parent column instead of subtree scans.
func (s *ScanLog) ListDirectPathnames2(kind EntryKind, parent string, reverse bool) iter.Seq[Entry] {

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
			var k int
			var p string
			var stored []byte
			if err := rows.Scan(&k, &p, &stored); err != nil {
				return
			}

			payload, err := decodePayload(stored)
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

// ListDirectPathnames returns only direct children of `parent` (no grandchildren),
// using the parent column instead of subtree scans.
func (s *ScanLog) ListDirectPathnames(kind EntryKind, parent string, reverse bool) iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		order := "ASC"
		if reverse {
			order = "DESC"
		}

		query := `
            SELECT kind, path, payload
            FROM entries
            WHERE parent = ?
        `
		args := []any{parent}

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
			var k int
			var p string
			var stored []byte
			if err := rows.Scan(&k, &p, &stored); err != nil {
				return
			}

			// Optional: keep your "directory paths end with /" convention
			if EntryKind(k) == KindDirectory && !strings.HasSuffix(p, "/") {
				p += "/"
			}

			payload, err := decodePayload(stored)
			if err != nil {
				return
			}

			if !yield(p, payload) {
				return
			}
		}
	}
}

type DirChildRow struct {
	DirPath      string
	DirPayload   []byte
	HasChild     bool
	ChildKind    EntryKind
	ChildPath    string
	ChildPayload []byte
}

func (s *ScanLog) ListDirectoryWithChildren(prefix string, reverse bool) iter.Seq[DirChildRow] {
	return func(yield func(DirChildRow) bool) {
		hi := prefix + string([]byte{0xFF})

		dirOrder := "ASC"
		if reverse {
			dirOrder = "DESC"
		}

		// We only join directories (d.kind = KindDirectory); children can be files or dirs.
		query := `
			SELECT
				d.path      AS dir_path,
				d.payload   AS dir_payload,
				c.kind      AS child_kind,
				c.path      AS child_path,
				c.payload   AS child_payload
			FROM entries AS d
			LEFT JOIN entries AS c
			  ON c.parent = d.path
			WHERE d.kind = ? AND d.path >= ? AND d.path < ?
			ORDER BY d.path ` + dirOrder + `, c.path ASC
		`

		rows, err := s.db.Query(query, int(KindDirectory), prefix, hi)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var (
				dirPath          string
				dirStoredPayload []byte

				childKindVal       sql.NullInt64
				childPathVal       sql.NullString
				childStoredPayload []byte
			)

			if err := rows.Scan(
				&dirPath,
				&dirStoredPayload,
				&childKindVal,
				&childPathVal,
				&childStoredPayload,
			); err != nil {
				return
			}

			dirPayload, err := decodePayload(dirStoredPayload)
			if err != nil {
				return
			}

			row := DirChildRow{
				DirPath:    dirPath,
				DirPayload: dirPayload,
			}

			if childKindVal.Valid && childPathVal.Valid {

				childPayload, err := decodePayload(childStoredPayload)
				if err != nil {
					return
				}

				row.HasChild = true
				row.ChildKind = EntryKind(childKindVal.Int64)
				row.ChildPath = childPathVal.String
				row.ChildPayload = childPayload
			}

			if !yield(row) {
				return
			}
		}
	}
}

func (s *ScanLog) ListPathnames(kind EntryKind, prefix string, reverse bool) iter.Seq[Entry] {
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
