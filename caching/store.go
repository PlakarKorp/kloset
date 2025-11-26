package caching

import (
	"fmt"
	"os"
	"path"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

// DBStore implements btree.Storer
type DBStore[K any, V any] struct {
	Prefix string
	idx    int
	Cache  *ScanCache
}

func (ds *DBStore[K, V]) Get(idx int) (*btree.Node[K, int, V], error) {
	bytes, err := ds.Cache.cache.Get([]byte(fmt.Sprintf("%s:%d", ds.Prefix, idx)))
	if err != nil {
		return nil, err
	}
	node := &btree.Node[K, int, V]{}
	err = msgpack.Unmarshal(bytes, node)
	return node, nil
}

func (ds *DBStore[K, V]) Update(idx int, node *btree.Node[K, int, V]) error {
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return err
	}
	return ds.Cache.cache.Put([]byte(fmt.Sprintf("%s:%d", ds.Prefix, idx)), bytes)
}

func (ds *DBStore[K, V]) Put(node *btree.Node[K, int, V]) (int, error) {
	ds.idx++
	idx := ds.idx
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}
	return idx, ds.Cache.cache.Put([]byte(fmt.Sprintf("%s:%d", ds.Prefix, idx)), bytes)
}

// SQLiteDBStore implements btree.Storer
type SQLiteDBStore[K any, V any] struct {
	dbpath string
	idx    int
	db     *sql.DB
}

func NewSQLiteDBStore[K, V any](storePath, storeName string) (*SQLiteDBStore[K, V], error) {
	storeName += uuid.NewString()
	dbpath := path.Join(storePath, storeName)
	if err := os.MkdirAll(dbpath, 0700); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", path.Join(dbpath, "dbstore.db"))
	if err != nil {
		return nil, err
	}

	create := `CREATE TABLE IF NOT EXISTS dbstore (
		id INTEGER NOT NULL PRIMARY KEY,
		node BLOB NOT NULL
	);`

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
			return nil, err
		}
	}

	if _, err := db.Exec(create); err != nil {
		return nil, err
	}

	return &SQLiteDBStore[K, V]{dbpath, 0, db}, nil
}

func (ds *SQLiteDBStore[K, V]) Get(idx int) (*btree.Node[K, int, V], error) {
	var bytes []byte
	if err := ds.db.QueryRow("SELECT node FROM dbstore WHERE id = ?", idx).Scan(&bytes); err != nil {
		return nil, err
	}

	node := &btree.Node[K, int, V]{}
	if err := msgpack.Unmarshal(bytes, node); err != nil {
		return nil, err
	}

	return node, nil
}

func (ds *SQLiteDBStore[K, V]) Update(idx int, node *btree.Node[K, int, V]) error {
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return err
	}

	_, err = ds.db.Exec("UPDATE dbstore SET node=? WHERE id=?", bytes, idx)
	return err
}

func (ds *SQLiteDBStore[K, V]) Put(node *btree.Node[K, int, V]) (int, error) {
	ds.idx++
	idx := ds.idx
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}

	_, err = ds.db.Exec("INSERT INTO dbstore(id, node) VALUES(?, ?)", idx, bytes)
	if err != nil {
		return 0, err
	}

	return ds.idx, nil
}

func (ds *SQLiteDBStore[K, V]) Close() error {
	err := ds.db.Close()

	os.RemoveAll(ds.dbpath)

	return err
}
