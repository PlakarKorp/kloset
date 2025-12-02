package caching

import (
	"fmt"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/vmihailenco/msgpack/v5"
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

func (ds *DBStore[K, V]) Close() error {
	// We do not own the cache.
	return nil
}

// SQLiteDBStore implements btree.Storer
type SQLiteDBStore[K any, V any] struct {
	dbpath string
	idx    int
	db     *sqlite.SQLiteCache
}

func NewSQLiteDBStore[K, V any](storePath, storeName string) (*SQLiteDBStore[K, V], error) {
	db, err := sqlite.New(storePath, storeName, true, false)
	if err != nil {
		return nil, err
	}

	create := `CREATE TABLE IF NOT EXISTS dbstore (
		id INTEGER NOT NULL PRIMARY KEY,
		node BLOB NOT NULL
	);`

	if _, err := db.Exec(create); err != nil {
		return nil, err
	}

	return &SQLiteDBStore[K, V]{db: db}, nil
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
	return ds.db.Close()
}
