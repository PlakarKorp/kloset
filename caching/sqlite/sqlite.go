package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/golang/snappy"
	_ "github.com/mattn/go-sqlite3"
)

// Low level encapsulation of an SQLite cache.
type SQLiteCache struct {
	*sql.DB

	dir  string
	name string
	opts *Options
}

type Options struct {
	DeleteOnClose bool
	Compressed    bool
	ReadOnly      bool
	Shared        bool
}

func makeDefaultOptions() *Options {
	return &Options{
		DeleteOnClose: false,
		Compressed:    false,
		ReadOnly:      false,
	}
}

func New(dir, name string, opts *Options) (*SQLiteCache, error) {
	if opts == nil {
		opts = makeDefaultOptions()
	}

	var db *sql.DB
	var err error
	if name == ":memory:" {
		opts.DeleteOnClose = false
		db, err = sql.Open("sqlite3", name)
		if err != nil {
			return nil, err
		}
	} else {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}

		dbpath := path.Join(dir, name)

		// If ro and the file does not exist, we need to open the db rw close it
		// and reopen it.
		if opts.ReadOnly {
			if _, err := os.Stat(dbpath); errors.Is(err, os.ErrNotExist) {
				tmpDb, err := sql.Open("sqlite3", "file:"+dbpath)
				if err != nil {
					return nil, err
				}

				// A bit ugly but we gotta exec at least something so that the
				// file is created on disk.
				pragmas := []string{
					"PRAGMA journal_mode = WAL;", // one-writer WAL, good for cache
				}

				for _, p := range pragmas {
					if _, err := tmpDb.Exec(p); err != nil {
						return nil, fmt.Errorf("pragma %q failed: %w", p, err)
					}
				}
				tmpDb.Close()
			}

			dbpath += "?mode=ro"
		}

		db, err = sql.Open("sqlite3", "file:"+dbpath)
		if err != nil {
			return nil, err
		}
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pragmas := []string{
		"PRAGMA journal_mode = WAL;", // one-writer WAL, good for cache
		"PRAGMA synchronous = OFF;",  // speed; scanlog is scratch
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA mmap_size = 0;",
		"PRAGMA cache_size = -20000;", // ~20MB
		"PRAGMA busy_timeout = 5000;",
	}

	if !opts.Shared {
		pragmas = append(pragmas, "PRAGMA locking_mode = EXCLUSIVE;")
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return nil, fmt.Errorf("pragma %q failed: %w", p, err)
		}
	}

	return &SQLiteCache{
		DB:   db,
		dir:  dir,
		name: name,
		opts: opts,
	}, err
}

func (s *SQLiteCache) Encode(raw []byte) []byte {
	if len(raw) == 0 || s.opts.Compressed == false {
		return raw
	}
	return snappy.Encode(nil, raw)
}

func (s *SQLiteCache) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 || s.opts.Compressed == false {
		return stored, nil
	}

	return snappy.Decode(nil, stored)
}

func (s *SQLiteCache) Close() error {
	err := s.DB.Close()

	if s.opts.DeleteOnClose {
		os.RemoveAll(s.dir)
	}

	return err
}
