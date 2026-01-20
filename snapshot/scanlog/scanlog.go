package scanlog

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path"

	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/PlakarKorp/kloset/objects"
)

type EntryKind int

const (
	KindDirectory EntryKind = 1
	KindFile      EntryKind = 2
)

type ScanBatch struct {
	db          *sqlite.SQLiteCache
	recs        []batchRec // entries
	pathmacRecs []pathmacRec
}

type batchRec struct {
	kind    EntryKind
	path    string
	mac     objects.MAC
	payload []byte
	summary []byte
}

type pathmacRec struct {
	path   string
	parent string
	mac    objects.MAC
}

type ScanLog struct {
	db *sqlite.SQLiteCache
}

func New(path string, name string) (*ScanLog, error) {
	db, err := sqlite.New(path, name, &sqlite.Options{
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
		mac     BLOB NOT NULL,
		payload BLOB    NOT NULL,
		summary BLOB,
		PRIMARY KEY (kind, path)
	) WITHOUT ROWID;

	CREATE INDEX IF NOT EXISTS entries_parent_idx
	ON entries(parent, kind, path);

	CREATE TABLE IF NOT EXISTS pathmac (
		path    TEXT NOT NULL PRIMARY KEY,
		parent  TEXT NOT NULL,
		mac     BLOB NOT NULL
	) WITHOUT ROWID;

	CREATE INDEX IF NOT EXISTS pathmac_parent_idx
	ON pathmac(parent, path);
	`

	_, err := db.Exec(schema)
	return err
}

func (s *ScanLog) Close() error {
	return s.db.Close()
}

func (s *ScanLog) PutDirectory(p string, mac objects.MAC, payload []byte) error {
	return s.put(KindDirectory, p, mac, payload, nil)
}

func (s *ScanLog) PutFile(p string, mac objects.MAC, payload []byte, summary []byte) error {
	return s.put(KindFile, p, mac, payload, summary)
}

func (s *ScanLog) put(kind EntryKind, p string, mac objects.MAC, payload []byte, summary []byte) error {
	parent := path.Dir(p)
	storedPayload := s.db.Encode(payload)
	storedSummary := summary
	if storedSummary != nil {
		storedSummary = s.db.Encode(summary)
	}

	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO entries (kind, path, parent, mac, payload, summary) VALUES (?, ?, ?, ?, ?, ?)`,
		int(kind), p, parent, mac[:], storedPayload, storedSummary,
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

func (b *ScanBatch) PutDirectory(p string, mac objects.MAC, payload []byte) error {
	return b.put(KindDirectory, p, mac, payload, nil)
}

func (b *ScanBatch) PutFile(p string, mac objects.MAC, payload []byte, summary []byte) error {
	return b.put(KindFile, p, mac, payload, summary)
}

func (b *ScanBatch) PutPathMAC(p string, mac objects.MAC) error {
	b.pathmacRecs = append(b.pathmacRecs, pathmacRec{
		path:   p,
		parent: path.Dir(p),
		mac:    mac,
	})
	return nil
}

func (b *ScanBatch) put(kind EntryKind, p string, mac objects.MAC, payload []byte, summary []byte) error {
	storedPayload := b.db.Encode(payload)
	storedSummary := summary
	if summary != nil {
		storedSummary = b.db.Encode(summary)
	}
	b.recs = append(b.recs, batchRec{
		kind:    kind,
		path:    p,
		mac:     mac,
		payload: storedPayload,
		summary: storedSummary,
	})
	return nil
}

func (b *ScanBatch) Count() uint32 {
	return uint32(len(b.recs) + len(b.pathmacRecs))
}

func (b *ScanBatch) Commit() error {
	if len(b.recs) == 0 && len(b.pathmacRecs) == 0 {
		return nil
	}

	tx, err := b.db.Begin()
	if err != nil {
		return err
	}

	entriesStmt, err := tx.Prepare(`INSERT OR REPLACE INTO entries (kind, path, parent, mac, payload, summary) VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer entriesStmt.Close()

	pathmacStmt, err := tx.Prepare(`INSERT OR REPLACE INTO pathmac (path, parent, mac) VALUES (?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer pathmacStmt.Close()

	for _, rec := range b.recs {
		parent := path.Dir(rec.path)
		if _, err := entriesStmt.Exec(int(rec.kind), rec.path, parent, rec.mac[:], rec.payload, rec.summary); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	for _, rec := range b.pathmacRecs {
		if _, err := pathmacStmt.Exec(rec.path, rec.parent, rec.mac[:]); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	b.recs = nil
	b.pathmacRecs = nil
	return nil
}

type Entry struct {
	Kind    EntryKind
	Path    string
	MAC     objects.MAC
	Payload []byte
	Summary []byte
}

func (s *ScanLog) list(kind EntryKind, prefix string, reverse bool, withMac bool) iter.Seq[Entry] {
	return func(yield func(Entry) bool) {
		hi := prefix + string([]byte{0xFF})

		order := "ASC"
		if reverse {
			order = "DESC"
		}

		var query string
		if !withMac {
			query = `
		SELECT kind, path
		FROM entries
		WHERE path >= ? AND path < ?
		`
		} else {
			query = `
		SELECT kind, path, mac
		FROM entries
		WHERE path >= ? AND path < ?
		`
		}
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
			var payload []byte
			var macStored []byte
			var mac objects.MAC

			if !withMac {
				if err := rows.Scan(&kInt, &p); err != nil {
					return
				}
			} else {
				if err := rows.Scan(&kInt, &p, &macStored); err != nil {
					return
				}
				mac = objects.MAC(macStored)
			}

			if !yield(Entry{
				Kind:    EntryKind(kInt),
				Path:    p,
				Payload: payload,
				MAC:     mac,
			}) {
				return
			}
		}
	}
}

func (s *ScanLog) ListFiles(kind EntryKind, prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(KindFile, prefix, reverse, false)
}

func (s *ScanLog) ListDirectories(prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(KindDirectory, prefix, reverse, false)
}

func (s *ScanLog) ListPathnames(prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(0, prefix, reverse, false)
}

func (s *ScanLog) ListPathnameMAC(prefix string, reverse bool) iter.Seq[Entry] {
	return s.list(0, prefix, reverse, true)
}

func (s *ScanLog) ListDirectPathnames(parent string, reverse bool) iter.Seq[Entry] {

	return func(yield func(Entry) bool) {
		order := "ASC"
		if reverse {
			order = "DESC"
		}

		query := `
		SELECT kind, path, payload, summary
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
			var storedPayload []byte
			var storedSummary []byte
			if err := rows.Scan(&k, &p, &storedPayload, &storedSummary); err != nil {
				return
			}

			payload, err := s.db.Decode(storedPayload)
			if err != nil {
				return
			}

			var summary []byte
			if storedSummary != nil {
				summary, err = s.db.Decode(storedSummary)
				if err != nil {
					return
				}
			}

			if !yield(Entry{
				Kind:    EntryKind(k),
				Path:    p,
				Payload: payload,
				Summary: summary,
			}) {
				return
			}
		}
	}
}

type PathMacEntry struct {
	Path string
	MAC  objects.MAC
}

func (s *ScanLog) PutPathMAC(p string, mac objects.MAC) error {
	parent := path.Dir(p)
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO pathmac (path, parent, mac) VALUES (?, ?, ?)`,
		p, parent, mac[:],
	)
	return err
}

func (s *ScanLog) GetPathMAC(p string) (*objects.MAC, error) {
	var stored []byte
	err := s.db.QueryRow(`SELECT mac FROM pathmac WHERE path = ?`, p).Scan(&stored)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if len(stored) != len(objects.MAC{}) {
		return nil, fmt.Errorf("invalid error mac length for %q: %d", p, len(stored))
	}
	var mac objects.MAC
	copy(mac[:], stored)
	return &mac, nil
}

func (s *ScanLog) ListPathMACsFrom(prefix string) iter.Seq[PathMacEntry] {
	return func(yield func(PathMacEntry) bool) {
		hi := prefix + string([]byte{0xFF})

		rows, err := s.db.Query(
			`SELECT path, mac FROM pathmac WHERE path >= ? AND path < ? ORDER BY path ASC`,
			prefix, hi,
		)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var p string
			var stored []byte
			if err := rows.Scan(&p, &stored); err != nil {
				return
			}
			if len(stored) != len(objects.MAC{}) {
				return
			}
			var mac objects.MAC
			copy(mac[:], stored)
			if !yield(PathMacEntry{Path: p, MAC: mac}) {
				return
			}
		}
	}
}

func (s *ScanLog) ListDirectPathMACs(parent string, reverse bool) iter.Seq[PathMacEntry] {
	return func(yield func(PathMacEntry) bool) {
		order := "ASC"
		if reverse {
			order = "DESC"
		}

		rows, err := s.db.Query(
			`SELECT path, mac FROM pathmac WHERE parent = ? AND parent != path ORDER BY path `+order,
			parent,
		)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var p string
			var stored []byte
			if err := rows.Scan(&p, &stored); err != nil {
				return
			}
			if len(stored) != len(objects.MAC{}) {
				return
			}
			var mac objects.MAC
			copy(mac[:], stored)
			if !yield(PathMacEntry{Path: p, MAC: mac}) {
				return
			}
		}
	}
}

func (s *ScanLog) CountPathMACs() (uint64, error) {
	var n uint64
	err := s.db.QueryRow(
		`SELECT COUNT(1) FROM pathmac`,
	).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s *ScanLog) CountDirectPathMACs(parent string) (uint64, error) {
	var n uint64
	err := s.db.QueryRow(
		`SELECT COUNT(1) FROM pathmac WHERE parent = ? AND parent != path`,
		parent,
	).Scan(&n)
	if err != nil {
		return 0, err
	}
	return n, nil
}
