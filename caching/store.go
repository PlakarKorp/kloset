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
	bytes, err := ds.Cache.cache.Get(fmt.Appendf(nil, "%s:%d", ds.Prefix, idx))
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
	return ds.Cache.cache.Put(fmt.Appendf(nil, "%s:%d", ds.Prefix, idx), bytes)
}

func (ds *DBStore[K, V]) Put(node *btree.Node[K, int, V]) (int, error) {
	ds.idx++
	idx := ds.idx
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}
	return idx, ds.Cache.cache.Put(fmt.Appendf(nil, "%s:%d", ds.Prefix, idx), bytes)
}

func (ds *DBStore[K, V]) Close() error {
	// We do not own the cache.
	return nil
}

// SQLiteDBStore implements btree.Storer
type SQLiteDBStore[K any, V any] struct {
	dbpath string
	db     *sqlite.SQLiteCache
}

func NewSQLiteDBStore[K, V any](storePath, storeName string) (*SQLiteDBStore[K, V], error) {
	db, err := sqlite.New(storePath, storeName, true, false)
	if err != nil {
		return nil, err
	}

	// It's safe to rely on the implicit id<=>ROWID alias here, ROWID will only
	// be reused in case of DELETE which we don't do.
	create := `CREATE TABLE IF NOT EXISTS dbstore (
		id INTEGER PRIMARY KEY,
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
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}

	var id int
	query := "INSERT INTO dbstore(id, node) VALUES(?, ?) RETURNING id"
	if err := ds.db.QueryRow(query, nil, bytes).Scan(&id); err != nil {
		return 0, err
	}

	return id, nil
}

func (ds *SQLiteDBStore[K, V]) Close() error {
	return ds.db.Close()
}
