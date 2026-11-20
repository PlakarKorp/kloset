package caching

import (
	"fmt"
	"slices"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/vmihailenco/msgpack/v5"
)

// SQLiteDBStore implements btree.Storer
type SQLiteDBStore[K any, V any] struct {
	db *sqlite.SQLiteCache
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

// A CoW store used by PITR backups. It's overlaying the backing store (in
// repository) with live changes.
type CowSQLiteDBStore[K any] struct {
	db      *sqlite.SQLiteCache
	backing btree.Storer[K, objects.MAC, objects.MAC]
}

func NewCowSQLiteDBStore[K any](storePath, storeName string, root objects.MAC, backingRoot btree.Storer[K, objects.MAC, objects.MAC]) (*CowSQLiteDBStore[K], error) {
	schema := []string{`CREATE TABLE IF NOT EXISTS dbstore (
		id INTEGER PRIMARY KEY,
		mac BLOB UNIQUE,
		node BLOB
	);`}

	db, err := sqlite.New(storePath, storeName, &sqlite.Options{
		DeleteOnClose: true,
		Compressed:    false,
		Schema:        schema,
	})
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec("INSERT INTO dbstore(mac) VALUES (?)", root[:]); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to insert root in store %w", err)
	}

	return &CowSQLiteDBStore[K]{db, backingRoot}, nil
}

func (ds *CowSQLiteDBStore[K]) Get(idx int) (*btree.Node[K, int, objects.MAC], error) {
	var mac objects.MAC
	var bytes []byte
	if err := ds.db.QueryRow("SELECT mac, node FROM dbstore WHERE id = ?", idx).Scan(macScanner{&mac}, &bytes); err != nil {
		return nil, fmt.Errorf("failed to select node %w", err)
	}

	// Have this been visited.
	if bytes != nil {
		node := &btree.Node[K, int, objects.MAC]{}
		if err := msgpack.Unmarshal(bytes, node); err != nil {
			return nil, err
		}

		return node, nil
	}

	node, err := ds.backing.Get(mac)
	if err != nil {
		return nil, err
	}

	newNode := btree.Node[K, int, objects.MAC]{
		Version:  node.Version,
		Keys:     slices.Clone(node.Keys),
		Pointers: make([]int, len(node.Pointers)),
		Values:   slices.Clone(node.Values),
	}

	if node.Next != nil {
		if err := ds.db.QueryRow("INSERT INTO dbstore(mac) VALUES (?) RETURNING id", node.Next).Scan(&newNode.Next); err != nil {
			return nil, fmt.Errorf("failed to insert next node in store %w", err)
		}
	}

	for i := range node.Pointers {
		if err := ds.db.QueryRow("INSERT INTO dbstore(mac) VALUES (?) RETURNING id", node.Pointers[i]).Scan(&newNode.Pointers[i]); err != nil {
			return nil, fmt.Errorf("failed to insert child pointer %w", err)
		}
	}

	bytes, err = msgpack.Marshal(newNode)
	if err != nil {
		return nil, err
	}

	if _, err := ds.db.Exec("UPDATE dbstore SET node = ? WHERE id = ? ", bytes, idx); err != nil {
		return nil, fmt.Errorf("failed to update node content %w", err)
	}

	return &newNode, nil
}

func (ds *CowSQLiteDBStore[K]) Update(idx int, node *btree.Node[K, int, objects.MAC]) error {
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return err
	}

	if _, err = ds.db.Exec("UPDATE dbstore SET node = ? WHERE id = ? ", bytes, idx); err !=nil {
		return fmt.Errorf("failed to update node %w", err)
	}

	return nil
}

func (ds *CowSQLiteDBStore[K]) Put(node *btree.Node[K, int, objects.MAC]) (int, error) {
	var idx int
	bytes, err := msgpack.Marshal(node)
	if err != nil {
		return 0, err
	}

	if err := ds.db.QueryRow("INSERT INTO dbstore(blob) VALUES (?) RETURNING id", bytes).Scan(&idx); err != nil {
		return 0, fmt.Errorf("failed to put node %w", err)
	}

	return idx, nil
}

func (ds *CowSQLiteDBStore[K]) Close() error {
	return ds.db.Close()
}
