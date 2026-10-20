package caching

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"runtime"

	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

type SQLState struct {
	db *sqlite.SQLiteCache
}

// Used to flatten a map, because go is go.
type kv struct {
	mac  objects.MAC
	data []byte
}

var stateSchema = []string{
	`CREATE TABLE IF NOT EXISTS states (
		mac BLOB NOT NULL PRIMARY KEY,
		payload BLOB NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS packfiles (
		mac BLOB NOT NULL PRIMARY KEY,
		payload BLOB NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS deltas (
		mac BLOB NOT NULL,
		type INTEGER NOT NULL,
		packfile BLOB NOT NULL,
		offset INTEGER NOT NULL,
		length INTEGER NOT NULL,
		PRIMARY KEY(type, mac, packfile)
	) WITHOUT ROWID, STRICT;`,

	`CREATE TABLE IF NOT EXISTS deleteds (
		mac BLOB NOT NULL,
		type INTEGER NOT NULL,
		payload BLOB NOT NULL,
		UNIQUE(type, mac)
	);`,

	`CREATE TABLE IF NOT EXISTS configurations (
		key TEXT NOT NULL PRIMARY KEY,
		data BLOB NOT NULL
	);`,
}

// Small scanner for objects.MAC so we avoid going through the []byte dance.
type macScanner struct{ dst *objects.MAC }

func (m macScanner) Scan(src any) error {
	b, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("mac: incompatible type %T", src)
	}

	if len(b) != len(m.dst) {
		return fmt.Errorf("mac: wrong length %d expected %d", len(b), len(m.dst))
	}

	copy(m.dst[:], b)
	return nil
}

// We can't use the state own type because it's a circular dep.
type Blob struct {
	MAC      objects.MAC
	Type     resources.Type
	Packfile objects.MAC
	Offset   uint64
	Length   uint32
}

func NewSQLState(path string, ro bool) (*SQLState, error) {
	db, err := sqlite.New(path, "state.db", &sqlite.Options{
		DeleteOnClose: false,
		Compressed:    true,
		ReadOnly:      ro,
		Shared:        true,
		Schema:        stateSchema,
	})
	if err != nil {
		return nil, err
	}

	if ro {
		// This should be ctx.MaxConcurrency, but this is an import cycle and
		// breaking it is out of the scope.
		db.SetMaxOpenConns(runtime.NumCPU())
		db.SetMaxIdleConns(runtime.NumCPU() / 2)
	}

	return &SQLState{db}, nil
}

func (c *SQLState) PutState(stateID objects.MAC, data []byte) error {
	_, err := c.db.Exec("INSERT INTO states(mac, payload) VALUES(?, ?)", stateID[:], data)
	return err
}

func (c *SQLState) HasState(stateID objects.MAC) (bool, error) {
	query := "SELECT 1 FROM states WHERE mac = ? LIMIT 1;"
	var dummy int
	err := c.db.QueryRow(query, stateID[:]).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *SQLState) GetState(stateID objects.MAC) ([]byte, error) {
	query := "SELECT payload FROM states WHERE mac = ? LIMIT 1;"
	var payload []byte
	err := c.db.QueryRow(query, stateID[:]).Scan(&payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}

		return nil, err
	}

	return payload, nil
}

func (c *SQLState) GetLatestState() (objects.MAC, error) {
	query := "SELECT mac FROM states ORDER BY ROWID DESC LIMIT 1"
	var mac objects.MAC
	err := c.db.QueryRow(query).Scan(macScanner{&mac})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return objects.NilMac, nil
		}

		return objects.NilMac, err
	}

	return mac, nil
}

func (c *SQLState) GetStates() (map[objects.MAC][]byte, error) {
	query := "SELECT mac, payload FROM states;"

	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	result := make(map[objects.MAC][]byte)
	for rows.Next() {
		var mac objects.MAC
		var payload []byte

		if err := rows.Scan(macScanner{&mac}, &payload); err != nil {
			return nil, err
		}

		result[mac] = payload
	}

	return result, nil
}

func (c *SQLState) DelState(stateID objects.MAC) error {
	_, err := c.db.Exec("DELETE FROM states WHERE mac = ?;", stateID[:])

	return err
}

func (c *SQLState) GetDelta(blobType resources.Type, blobMAC objects.MAC) iter.Seq2[Blob, error] {
	return func(yield func(Blob, error) bool) {
		query := `SELECT mac, type, packfile, offset, length
					FROM deltas
					WHERE mac = ? AND type = ?;`

		rows, err := c.db.Query(query, blobMAC[:], blobType)
		if err != nil {
			// We can't proceed here. Ideally this should be pre-iteration.
			yield(Blob{}, err)
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]Blob, 0)
		for rows.Next() {
			b := Blob{}

			if err := rows.Scan(macScanner{&b.MAC}, &b.Type, macScanner{&b.Packfile}, &b.Offset, &b.Length); err != nil {
				if !yield(Blob{}, err) {
					return
				}
			}

			result = append(result, b)
		}
		rows.Close()

		for _, b := range result {
			if !yield(b, nil) {
				return
			}
		}
	}
}

func (c *SQLState) PutDelta(blobType resources.Type, blobMAC, packfileMAC objects.MAC, offset uint64, length uint32) error {
	_, err := c.db.Exec("INSERT OR IGNORE INTO deltas(mac, type, packfile, offset, length) VALUES(?, ?, ?, ?, ?);", blobMAC[:], blobType, packfileMAC[:], offset, length)

	return err
}

func (c *SQLState) GetDeltasByType(blobType resources.Type) iter.Seq2[Blob, error] {
	return func(yield func(Blob, error) bool) {
		query := `SELECT mac, type, packfile, offset, length
					FROM deltas
					WHERE type = ?;`
		rows, err := c.db.Query(query, blobType)

		if err != nil {
			// We can't proceed here. Ideally this should be pre-iteration.
			yield(Blob{}, err)
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]Blob, 0)
		for rows.Next() {
			b := Blob{}

			if err := rows.Scan(macScanner{&b.MAC}, &b.Type, macScanner{&b.Packfile}, &b.Offset, &b.Length); err != nil {
				if !yield(Blob{}, err) {
					return
				}
			}

			result = append(result, b)
		}
		rows.Close()

		for _, b := range result {
			if !yield(b, nil) {
				return
			}
		}
	}
}

func (c *SQLState) GetDeltas() iter.Seq2[Blob, error] {
	return func(yield func(Blob, error) bool) {
		query := `SELECT mac, type, packfile, offset, length
					FROM deltas;`

		rows, err := c.db.Query(query)
		if err != nil {
			// We can't proceed here. Ideally this should be pre-iteration.
			yield(Blob{}, err)
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]Blob, 0)
		for rows.Next() {
			b := Blob{}

			if err := rows.Scan(macScanner{&b.MAC}, &b.Type, macScanner{&b.Packfile}, &b.Offset, &b.Length); err != nil {
				if !yield(Blob{}, err) {
					return
				}
			}

			result = append(result, b)
		}
		rows.Close()

		for _, b := range result {
			if !yield(b, nil) {
				return
			}
		}
	}
}

func (c *SQLState) DelDelta(blobType resources.Type, blobMAC, packfileMAC objects.MAC) error {
	_, err := c.db.Exec("DELETE FROM deltas WHERE mac = ? AND type = ? AND packfile = ?;", blobMAC[:], blobType, packfileMAC[:])
	return err
}

func (c *SQLState) PutColoured(blobType resources.Type, blobMAC objects.MAC, data []byte) error {
	_, err := c.db.Exec("INSERT OR IGNORE INTO deleteds(mac, type, payload) VALUES(?, ?, ?)", blobMAC[:], blobType, data)
	return err
}

func (c *SQLState) HasColoured(blobType resources.Type, blobMAC objects.MAC) (bool, error) {
	query := "SELECT 1 FROM deleteds WHERE mac = ? AND type = ? LIMIT 1;"
	var dummy int
	err := c.db.QueryRow(query, blobMAC[:], blobType).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *SQLState) GetColouredEntries() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload FROM deleteds;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]kv, 0)
		for rows.Next() {
			var kv kv

			if err := rows.Scan(macScanner{&kv.mac}, &kv.data); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result = append(result, kv)
		}
		rows.Close()

		for _, r := range result {
			if !yield(r.mac, r.data) {
				return
			}
		}
	}
}

func (c *SQLState) GetColouredEntriesByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload  FROM deleteds WHERE type = ?;"

		rows, err := c.db.Query(query, blobType)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]kv, 0)
		for rows.Next() {
			var kv kv

			if err := rows.Scan(macScanner{&kv.mac}, &kv.data); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result = append(result, kv)
		}
		rows.Close()

		for _, r := range result {
			if !yield(r.mac, r.data) {
				return
			}
		}
	}
}

func (c *SQLState) DelColoured(blobType resources.Type, blobMAC objects.MAC) error {
	_, err := c.db.Exec("DELETE FROM deleteds WHERE mac = ? AND type = ?;", blobMAC[:], blobType)
	return err
}

func (c *SQLState) PutPackfile(packfileMAC objects.MAC, data []byte) error {
	_, err := c.db.Exec("INSERT OR IGNORE INTO packfiles(mac, payload) VALUES(?,  ?)", packfileMAC[:], data)
	return err
}

func (c *SQLState) HasPackfile(packfileMAC objects.MAC) (bool, error) {
	query := "SELECT 1 FROM packfiles WHERE mac = ? LIMIT 1;"
	var dummy int
	err := c.db.QueryRow(query, packfileMAC[:]).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *SQLState) DelPackfile(packfileMAC objects.MAC) error {
	_, err := c.db.Exec("DELETE FROM packfiles WHERE mac = ?;", packfileMAC[:])

	if err != nil {
		return err
	}

	// XXX: This is a big layer violation, but the common interface is getting
	// out of hand. When we end up splitting the various State implementation
	// the local state will deal with cascading deletion of packfiles into the
	// blob it contained. For now we do it here to ease the implementation.
	_, err = c.db.Exec("DELETE FROM deltas WHERE packfile = ?;", packfileMAC[:])

	return err
}

func (c *SQLState) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload FROM packfiles;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]kv, 0)
		for rows.Next() {
			var kv kv

			if err := rows.Scan(macScanner{&kv.mac}, &kv.data); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result = append(result, kv)
		}

		rows.Close()

		for _, r := range result {
			if !yield(r.mac, r.data) {
				return
			}
		}
	}
}

// Configuration handling
func (c *SQLState) PutConfiguration(key string, data []byte) error {
	_, err := c.db.Exec("INSERT INTO configurations(key, data) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET data=excluded.data", key, data)
	return err
}

func (c *SQLState) GetConfiguration(key string) ([]byte, error) {
	query := "SELECT data FROM configurations WHERE key = ?"
	var data []byte
	if err := c.db.QueryRow(query, key).Scan(&data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	return data, nil
}

func (c *SQLState) GetConfigurations() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		query := "SELECT key, data FROM configurations;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([][]byte, 0)
		for rows.Next() {
			var key string
			var data []byte

			if err := rows.Scan(&key, &data); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result = append(result, data)
		}

		rows.Close()

		for _, d := range result {
			if !yield(d) {
				return
			}
		}
	}
}
