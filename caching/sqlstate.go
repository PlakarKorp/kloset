package caching

import (
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"time"

	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

type SQLState struct {
	db *sqlite.SQLiteCache
}

var stateSchema = []string{
	`CREATE TABLE IF NOT EXISTS states (
		mac BLOB NOT NULL PRIMARY KEY,
		payload BLOB NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS packfiles (
		mac BLOB NOT NULL PRIMARY KEY,
		state BLOB NOT NULL,
		timestamp INTEGER NOT NULL
	);`,

	`CREATE TABLE IF NOT EXISTS deltas (
		mac BLOB NOT NULL,
		type INTEGER NOT NULL,
		packfile BLOB NOT NULL,
		offset INTEGER NOT NULL,
		length INTEGER NOT NULL,
		PRIMARY KEY(type, mac, packfile)
	) WITHOUT ROWID, STRICT;`,

	`CREATE TABLE IF NOT EXISTS coloured (
		mac BLOB NOT NULL,
		type INTEGER NOT NULL,
		timestamp INTEGER NOT NULL,
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

type ColouredBlob struct {
	MAC  objects.MAC
	Type resources.Type
	When time.Time
}

type Packfile struct {
	MAC       objects.MAC
	State     objects.MAC
	Timestamp time.Time
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
func (c *SQLState) PutDelta(blobType resources.Type, blobMAC, packfileMAC objects.MAC, offset uint64, length uint32) error {
	_, err := c.db.Exec("INSERT OR IGNORE INTO deltas(mac, type, packfile, offset, length) VALUES(?, ?, ?, ?, ?);", blobMAC[:], blobType, packfileMAC[:], offset, length)

	return err
}

// Internal helper, the layout of the query must follow the Scan obviously
func (c *SQLState) getDeltas(query string, args ...any) iter.Seq2[Blob, error] {
	return func(yield func(Blob, error) bool) {
		rows, err := c.db.Query(query, args...)
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

func (c *SQLState) GetDelta(blobType resources.Type, blobMAC objects.MAC) iter.Seq2[Blob, error] {
	query := `SELECT mac, type, packfile, offset, length
					FROM deltas
					WHERE mac = ? AND type = ?;`

	return c.getDeltas(query, blobMAC[:], blobType)
}

func (c *SQLState) GetDeltasByType(blobType resources.Type) iter.Seq2[Blob, error] {
	query := `SELECT mac, type, packfile, offset, length
					FROM deltas
					WHERE type = ?;`
	return c.getDeltas(query, blobType)
}

func (c *SQLState) GetDeltas() iter.Seq2[Blob, error] {
	query := `SELECT mac, type, packfile, offset, length
					FROM deltas;`

	return c.getDeltas(query)
}

func (c *SQLState) DelDelta(blobType resources.Type, blobMAC, packfileMAC objects.MAC) error {
	_, err := c.db.Exec("DELETE FROM deltas WHERE mac = ? AND type = ? AND packfile = ?;", blobMAC[:], blobType, packfileMAC[:])
	return err
}

func (c *SQLState) PutColoured(blobType resources.Type, blobMAC objects.MAC, when time.Time) error {
	_, err := c.db.Exec("INSERT OR IGNORE INTO coloured(mac, type, timestamp) VALUES(?, ?, ?)", blobMAC[:], blobType, when.UnixNano())
	return err
}

func (c *SQLState) HasColoured(blobType resources.Type, blobMAC objects.MAC) (bool, error) {
	query := "SELECT 1 FROM coloured WHERE mac = ? AND type = ? LIMIT 1;"
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

func (c *SQLState) getColouredEntries(query string, args ...any) iter.Seq2[ColouredBlob, error] {
	return func(yield func(ColouredBlob, error) bool) {

		rows, err := c.db.Query(query, args...)
		if err != nil {
			// We can't proceed here. Ideally this should be pre-iteration.
			yield(ColouredBlob{}, err)
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]ColouredBlob, 0)
		for rows.Next() {
			cb := ColouredBlob{}
			var when int64

			if err := rows.Scan(macScanner{&cb.MAC}, &cb.Type, &when); err != nil {
				if !yield(ColouredBlob{}, err) {
					return
				}

				continue
			}

			cb.When = time.Unix(0, when)

			result = append(result, cb)
		}
		rows.Close()

		for _, cb := range result {
			if !yield(cb, nil) {
				return
			}
		}
	}
}

func (c *SQLState) GetColouredEntries() iter.Seq2[ColouredBlob, error] {
	query := "SELECT mac, type, timestamp FROM coloured ;"
	return c.getColouredEntries(query)
}

func (c *SQLState) GetColouredEntriesByType(blobType resources.Type) iter.Seq2[ColouredBlob, error] {
	query := "SELECT mac, type, timestamp FROM coloured WHERE type = ?;"
	return c.getColouredEntries(query, blobType)
}

func (c *SQLState) DelColoured(blobType resources.Type, blobMAC objects.MAC) error {
	_, err := c.db.Exec("DELETE FROM coloured WHERE mac = ? AND type = ?;", blobMAC[:], blobType)
	return err
}

func (c *SQLState) PutPackfile(packfileMAC, stateMAC objects.MAC, timestamp time.Time) error {
	_, err := c.db.Exec("INSERT OR IGNORE INTO packfiles(mac, state, timestamp) VALUES(?,  ?, ?)", packfileMAC[:], stateMAC[:], timestamp.UnixNano())
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

func (c *SQLState) GetPackfiles() iter.Seq2[Packfile, error] {
	return func(yield func(Packfile, error) bool) {
		query := "SELECT mac, state, timestamp FROM packfiles;"

		rows, err := c.db.Query(query)
		if err != nil {
			yield(Packfile{}, err)
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make([]Packfile, 0)
		for rows.Next() {
			p := Packfile{}
			var t int64

			if err := rows.Scan(macScanner{&p.MAC}, macScanner{&p.State}, &t); err != nil {
				if !yield(Packfile{}, err) {
					return
				}

				continue
			}

			p.Timestamp = time.Unix(0, t)
			result = append(result, p)
		}

		rows.Close()

		for _, p := range result {
			if !yield(p, nil) {
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
