package caching

import (
	"fmt"

	"github.com/PlakarKorp/kloset/btree"
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
	storeName string
	idx       int
	db        *sql.DB
}

func NewSQLiteDBStore[K, V any](storeName string, db *sql.DB) (*SQLiteDBStore[K, V], error) {
	create :=
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id INTEGER NOT NULL PRIMARY KEY,
		node BLOB NOT NULL
	);`, storeName)

	if _, err := db.Exec(create); err != nil {
		return nil, err
	}

	return &SQLiteDBStore[K, V]{storeName, 0, db}, nil
}

func (ds *SQLiteDBStore[K, V]) Get(idx int) (*btree.Node[K, int, V], error) {
	var bytes []byte
	if err := ds.db.QueryRow(fmt.Sprintf("SELECT node FROM %s WHERE id = ?", ds.storeName), idx).Scan(&bytes); err != nil {
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

	_, err = ds.db.Exec(fmt.Sprintf("UPDATE %s SET node=? WHERE id=?", ds.storeName), bytes, idx)
	return err
}

func (ds *SQLiteDBStore[K, V]) Put(node *btree.Node[K, int, V]) (int, error) {
	ds.idx++
	idx := ds.idx
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}

	_, err = ds.db.Exec(fmt.Sprintf("INSERT INTO %s(id, node) VALUES(?, ?)", ds.storeName), idx, bytes)
	if err != nil {
		return 0, err
	}

	return ds.idx, nil
}
