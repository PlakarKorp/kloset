package caching

import (
	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/vmihailenco/msgpack/v5"
)

// SQLiteDBStore implements btree.Storer
type SQLiteDBStore[K any, V any] struct {
	dbpath string
	db     *sqlite.SQLiteCache
}

var dbstoreSchema = []string{
	// It's safe to rely on the implicit id<=>ROWID alias here, ROWID will only
	// be reused in case of DELETE which we don't do.
	`CREATE TABLE IF NOT EXISTS dbstore (
		id INTEGER PRIMARY KEY,
		node BLOB NOT NULL
	);`,
}

func NewSQLiteDBStore[K, V any](storePath, storeName string) (*SQLiteDBStore[K, V], error) {
	db, err := sqlite.New(storePath, storeName, &sqlite.Options{
		DeleteOnClose: true,
		Compressed:    false,
		Schema:        dbstoreSchema,
	})
	if err != nil {
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
