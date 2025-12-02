package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path"

	"github.com/golang/snappy"
	_ "github.com/mattn/go-sqlite3"
)

// Low level encapsulation of an SQLite cache.
type SQLiteCache struct {
	*sql.DB

	dir           string
	name          string
	deleteOnClose bool
	compressed    bool
}

func New(dir, name string, deletedOnClose, compressed bool) (*SQLiteCache, error) {

	var db *sql.DB
	var err error
	if name == ":memory:" {
		deletedOnClose = false
		db, err = sql.Open("sqlite3", name)
		if err != nil {
			return nil, err
		}
	} else {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}

		db, err = sql.Open("sqlite3", path.Join(dir, name))
		if err != nil {
			return nil, err
		}
	}

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
			return nil, fmt.Errorf("pragma %q failed: %w", p, err)
		}
	}

	return &SQLiteCache{
		DB:            db,
		dir:           dir,
		name:          name,
		deleteOnClose: deletedOnClose,
		compressed:    compressed,
	}, err
}

func (s *SQLiteCache) Encode(raw []byte) []byte {
	if len(raw) == 0 || s.compressed == false {
		return raw
	}
	return snappy.Encode(nil, raw)
}

func (s *SQLiteCache) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 || s.compressed == false {
		return stored, nil
	}

	return snappy.Decode(nil, stored)
}

func (s *SQLiteCache) Close() error {
	err := s.DB.Close()

	if s.deleteOnClose {
		os.RemoveAll(s.dir)
	}

	return err
}
