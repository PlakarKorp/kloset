package caching

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"runtime"
	"sync"

	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

type SQLState struct {
	db *sqlite.SQLiteCache

	mtx  sync.RWMutex
	blob map[objects.MAC]bool
}

// XXX: could we reuse something?!
type sDelta struct {
	mac     string
	typ     int
	pack    string
	payload []byte
}

type sqlStateBatch struct {
	parent *SQLState

	deltas []sDelta
}

func NewSQLState(path string, ro bool) (*SQLState, error) {
	db, err := sqlite.New(path, "state.db", &sqlite.Options{
		DeleteOnClose: false,
		Compressed:    true,
		ReadOnly:      ro,
		Shared:        true,
	})
	if err != nil {
		return nil, err
	}

	if !ro {
		for _, typ := range []string{"states", "packfiles"} {
			create := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			mac TEXT NOT NULL PRIMARY KEY,
			payload BLOB NOT NULL
		);`, typ)

			if _, err := db.Exec(create); err != nil {
				return nil, err
			}
		}

		create := `CREATE TABLE IF NOT EXISTS deltas (
			mac TEXT NOT NULL,
			type INTEGER NOT NULL,
			packfile TEXT NOT NULL,
			payload BLOB NOT NULL,
			UNIQUE(type, mac, packfile)
		);`
		if _, err := db.Exec(create); err != nil {
			return nil, err
		}

		create = `CREATE TABLE IF NOT EXISTS deleteds (
			mac TEXT NOT NULL,
			type INTEGER NOT NULL,
			payload BLOB NOT NULL,
			UNIQUE(type, mac)
		);`

		if _, err := db.Exec(create); err != nil {
			return nil, err
		}

		create = `CREATE TABLE IF NOT EXISTS configurations (
		key TEXT NOT NULL PRIMARY KEY,
		data BLOB NOT NULL
	);`

		if _, err := db.Exec(create); err != nil {
			return nil, err
		}
	}
	if ro {
		// This should be ctx.MaxConcurrency, but this is an import cycle and
		// breaking it is out of the scope.
		db.SetMaxOpenConns(runtime.NumCPU())
		db.SetMaxIdleConns(runtime.NumCPU() / 2)
	}

	return &SQLState{db, sync.RWMutex{}, make(map[objects.MAC]bool)}, nil
}

func (c *SQLState) NewBatch() StateBatch {
	return &sqlStateBatch{c, make([]sDelta, 0)}
}

func (c *sqlStateBatch) Put([]byte, []byte) error {
	panic("NOT IMPLEMENTED")
}

func (c *sqlStateBatch) Count() uint32 {
	return uint32(len(c.deltas))
}

func (c *sqlStateBatch) Commit() error {
	if len(c.deltas) == 0 {
		return nil
	}

	tx, err := c.parent.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT OR IGNORE INTO deltas (mac, type, packfile, payload) VALUES (?, ?, ?, ?);`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, rec := range c.deltas {
		if _, err := stmt.Exec(rec.mac, rec.typ, rec.pack, rec.payload); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	c.deltas = nil
	return nil
}

// States handling, mostly unused.
func (c *SQLState) PutState(stateID objects.MAC, data []byte) error {
	stateHex := hex.EncodeToString(stateID[:])

	_, err := c.db.Exec("INSERT INTO states(mac, payload) VALUES(?, ?)", stateHex, data)
	return err
}

func (c *SQLState) HasState(stateID objects.MAC) (bool, error) {
	stateHex := hex.EncodeToString(stateID[:])

	query := "SELECT 1 FROM states WHERE mac = ? LIMIT 1;"
	var dummy int
	err := c.db.QueryRow(query, stateHex).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *SQLState) GetState(stateID objects.MAC) ([]byte, error) {
	stateHex := hex.EncodeToString(stateID[:])

	query := "SELECT payload FROM states WHERE mac = ? LIMIT 1;"
	var payload []byte
	err := c.db.QueryRow(query, stateHex).Scan(&payload)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}

		return nil, err
	}

	return payload, nil
}
func (c *SQLState) GetStates() (map[objects.MAC][]byte, error) {
	query := "SELECT mac, payload FROM states;"

	rows, err := c.db.Query(query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// Normalize in memory, before yielding to avoid deadlocks in sqlite.
	result := make(map[objects.MAC][]byte)
	for rows.Next() {
		var strMac string
		var payload []byte

		if err := rows.Scan(&strMac, &payload); err != nil {
			return nil, err
		}

		mac, err := hex.DecodeString(strMac)
		if err != nil {
			return nil, err
		}

		result[objects.MAC(mac)] = payload
	}

	return result, nil
}

func (c *SQLState) DelState(stateID objects.MAC) error {
	stateHex := hex.EncodeToString(stateID[:])
	_, err := c.db.Exec("DELETE FROM states WHERE mac = ?;", stateHex)

	return err
}

// Deltas handling
func (c *sqlStateBatch) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	blobMACHex := hex.EncodeToString(blobCsum[:])
	packMACHex := hex.EncodeToString(packfile[:])
	c.deltas = append(c.deltas, sDelta{blobMACHex, int(blobType), packMACHex, data})

	c.parent.mtx.Lock()
	c.parent.blob[blobCsum] = true
	c.parent.mtx.Unlock()

	return nil
}

func (c *SQLState) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := `SELECT mac, payload
					FROM deltas
					WHERE mac = ? AND type = ?;
				`

		blobMACHex := hex.EncodeToString(blobCsum[:])
		rows, err := c.db.Query(query, blobMACHex, blobType)
		if err != nil {
			// XXX :/
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make(map[string][]byte)
		for rows.Next() {
			var mac string
			var payload []byte

			if err := rows.Scan(&mac, &payload); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[mac] = payload
		}

		rows.Close()
		for hexMac, payload := range result {
			mac, _ := hex.DecodeString(hexMac)
			if !yield(objects.MAC(mac), payload) {
				return
			}
		}

	}
}

func (c *SQLState) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	blobMACHex := hex.EncodeToString(blobCsum[:])
	packMACHex := hex.EncodeToString(packfile[:])

	_, err := c.db.Exec("INSERT OR IGNORE INTO deltas(mac, type, packfile, payload) VALUES(?, ?, ?, ?);", blobMACHex, blobType, packMACHex, data)

	return err
}

func (c *SQLState) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload  FROM deltas WHERE type = ?;"

		rows, err := c.db.Query(query, blobType)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make(map[string][]byte)
		for rows.Next() {
			var mac string
			var payload []byte

			if err := rows.Scan(&mac, &payload); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[mac] = payload
		}
		rows.Close()

		for hexMac, payload := range result {
			mac, _ := hex.DecodeString(hexMac)
			if !yield(objects.MAC(mac), payload) {
				return
			}
		}
	}
}

func (c *SQLState) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload FROM deltas;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make(map[string][]byte)
		for rows.Next() {
			var mac string
			var payload []byte

			if err := rows.Scan(&mac, &payload); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[mac] = payload
		}
		rows.Close()

		for hexMac, payload := range result {
			mac, _ := hex.DecodeString(hexMac)
			if !yield(objects.MAC(mac), payload) {
				return
			}
		}
	}
}

func (c *SQLState) DelDelta(blobType resources.Type, blobCsum, packfileMAC objects.MAC) error {
	blobMACHex := hex.EncodeToString(blobCsum[:])
	packMACHex := hex.EncodeToString(packfileMAC[:])
	_, err := c.db.Exec("DELETE FROM deltas WHERE mac = ? AND type = ? AND packfile = ?;", blobMACHex, blobType, packMACHex)
	return err
}

// Deleted handling.
func (c *SQLState) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	blobMACHex := hex.EncodeToString(blobCsum[:])

	_, err := c.db.Exec("INSERT OR IGNORE INTO deleteds(mac, type, payload) VALUES(?, ?, ?)", blobMACHex, blobType, data)
	return err
}

func (c *SQLState) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	blobMACHex := hex.EncodeToString(blobCsum[:])

	query := "SELECT 1 FROM deleteds WHERE mac = ? AND type = ? LIMIT 1;"
	var dummy int
	err := c.db.QueryRow(query, blobMACHex, blobType).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *SQLState) GetDeleteds() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload FROM deleteds;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make(map[string][]byte)
		for rows.Next() {
			var mac string
			var payload []byte

			if err := rows.Scan(&mac, &payload); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[mac] = payload
		}
		rows.Close()

		for hexMac, payload := range result {
			mac, _ := hex.DecodeString(hexMac)
			if !yield(objects.MAC(mac), payload) {
				return
			}
		}
	}
}

func (c *SQLState) GetDeletedsByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload  FROM deleteds WHERE type = ?;"

		rows, err := c.db.Query(query, blobType)
		if err != nil {
			return
		}

		// Normalize in memory, before yielding to avoid deadlocks in sqlite.
		result := make(map[string][]byte)
		for rows.Next() {
			var mac string
			var payload []byte

			if err := rows.Scan(&mac, &payload); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[mac] = payload
		}

		rows.Close()

		for hexMac, payload := range result {
			mac, _ := hex.DecodeString(hexMac)
			if !yield(objects.MAC(mac), payload) {
				return
			}
		}
	}
}

func (c *SQLState) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	blobMACHex := hex.EncodeToString(blobCsum[:])
	_, err := c.db.Exec("DELETE FROM deleteds WHERE mac = ? AND type = ?;", blobMACHex, blobType)
	return err
}

// Packfile handling
func (c *SQLState) PutPackfile(packfile objects.MAC, data []byte) error {
	packMACHex := hex.EncodeToString(packfile[:])

	_, err := c.db.Exec("INSERT OR IGNORE INTO packfiles(mac, payload) VALUES(?,  ?)", packMACHex, data)
	return err
}

func (c *SQLState) HasPackfile(packfile objects.MAC) (bool, error) {
	packMACHex := hex.EncodeToString(packfile[:])

	query := "SELECT 1 FROM packfiles WHERE mac = ? LIMIT 1;"
	var dummy int
	err := c.db.QueryRow(query, packMACHex).Scan(&dummy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (c *SQLState) DelPackfile(packfile objects.MAC) error {
	packMACHex := hex.EncodeToString(packfile[:])
	_, err := c.db.Exec("DELETE FROM packfiles WHERE mac = ?;", packMACHex)
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
		result := make(map[string][]byte)
		for rows.Next() {
			var mac string
			var payload []byte

			if err := rows.Scan(&mac, &payload); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[mac] = payload
		}

		rows.Close()

		for hexMac, payload := range result {
			mac, _ := hex.DecodeString(hexMac)
			if !yield(objects.MAC(mac), payload) {
				return
			}
		}
	}
}

// Configuration handling
func (c *SQLState) PutConfiguration(key string, data []byte) error {
	_, err := c.db.Exec("INSERT INTO configurations(key, data) VALUES(?,  ?)", key, data)
	return err
}

func (c *SQLState) GetConfiguration(key string) ([]byte, error) {
	query := "SELECT key, data FROM configurations WHERE key = ?"
	var data []byte
	if err := c.db.QueryRow(query, key).Scan(&data); err != nil {
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
		result := make(map[string][]byte)
		for rows.Next() {
			var key string
			var data []byte

			if err := rows.Scan(&key, &data); err != nil {
				// XXX: who the f designed that interface :(
				continue
			}

			result[key] = data
		}

		rows.Close()

		for _, v := range result {
			if !yield(v) {
				return
			}
		}
	}
}
