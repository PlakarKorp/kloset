package caching

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"iter"
	"os"
	"path"
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	_ "github.com/mattn/go-sqlite3"
)

type SQLState struct {
	db *sql.DB

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
	db     *sql.DB
	parent *SQLState

	deltas []sDelta
}

func NewSQLState(_ Constructor, snapshotID [32]byte) (*SQLState, error) {
	dbpath := fmt.Sprintf("/tmp/%x/", snapshotID)
	if err := os.MkdirAll(dbpath, 0700); err != nil {
		return nil, err
	}

	db, err := sql.Open("sqlite3", path.Join(dbpath, "deltastate.db"))
	if err != nil {
		return nil, err
	}

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

	for _, typ := range []string{"states", "packfiles"} {
		create := fmt.Sprintf(`CREATE TABLE %s (
			mac TEXT NOT NULL PRIMARY KEY,
			payload BLOB NOT NULL
		);`, typ)

		if _, err := db.Exec(create); err != nil {
			return nil, err
		}
	}

	create := `CREATE TABLE deltas (
			mac TEXT NOT NULL,
			type INTEGER NOT NULL,
			packfile TEXT NOT NULL,
			payload BLOB NOT NULL,
			UNIQUE(mac, type, packfile)
		);`
	if _, err := db.Exec(create); err != nil {
		return nil, err
	}

	create = `CREATE TABLE deleteds (
			mac TEXT NOT NULL,
			type INTEGER NOT NULL,
			payload BLOB NOT NULL,
			UNIQUE(mac, type)
		);`

	if _, err := db.Exec(create); err != nil {
		return nil, err
	}

	create = `CREATE TABLE configurations (
		key TEXT NOT NULL PRIMARY KEY,
		data BLOB NOT NULL
	);`

	if _, err := db.Exec(create); err != nil {
		return nil, err
	}

	return &SQLState{db, sync.RWMutex{}, make(map[objects.MAC]bool)}, nil
}

func (c *SQLState) NewBatch() StateBatch {
	return &sqlStateBatch{c.db, c, make([]sDelta, 0)}
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

	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT INTO deltas (mac, type, packfile, payload) VALUES (?, ?, ?, ?)`)
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

	_, err := c.db.Exec("INSERT INTO deltas(mac, payload) VALUES(?, ?)", stateHex, data)
	return err
}

func (c *SQLState) HasState(stateID objects.MAC) (bool, error) {
	panic("HasState should never be used on the SQLState backend")
}

func (c *SQLState) GetState(stateID objects.MAC) ([]byte, error) {
	panic("GetState should never be used on the SQLState backend")
}

func (c *SQLState) GetStates() (map[objects.MAC][]byte, error) {
	panic("GetStates should never be used on the SQLState backend")
}

func (c *SQLState) DelState(stateID objects.MAC) error {
	panic("DelStates should never be used on the SQLState backend")
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
		c.mtx.RLock()
		defer c.mtx.RUnlock()
		_, ok := c.blob[blobCsum]
		if ok {
			if !yield(blobCsum, nil) {
				return
			}
		}
	}
	/*
		return func(yield func(objects.MAC, []byte) bool) {
			query := `SELECT mac, payload
					FROM deltas
					WHERE mac = ? AND type = ?;
				`

			blobMACHex := hex.EncodeToString(blobCsum[:])
			rows, err := c.db.Query(query, blobMACHex, blobType)
			if err != nil {
				return
			}

			defer rows.Close()

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

			for hexMac, payload := range result {
				mac, _ := hex.DecodeString(hexMac)
				if !yield(objects.MAC(mac), payload) {
					return
				}
			}

		}
	*/
}

func (c *SQLState) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	blobMACHex := hex.EncodeToString(blobCsum[:])
	packMACHex := hex.EncodeToString(packfile[:])

	_, err := c.db.Exec("INSERT INTO deltas(mac, type, packfile, payload) VALUES(?, ?, ?, ?)", blobMACHex, blobCsum, packMACHex, data)
	return err
}

func (c *SQLState) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		query := "SELECT mac, payload  FROM deltas WHERE type = ?;"

		rows, err := c.db.Query(query, blobType)
		if err != nil {
			return
		}

		defer rows.Close()

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

		defer rows.Close()

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

	_, err := c.db.Exec("INSERT INTO deleteds(mac, type, payload) VALUES(?, ?, ?)", blobMACHex, blobCsum, data)
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

		defer rows.Close()

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

		defer rows.Close()

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
	_, err := c.db.Exec("DELETE FROM deleteds WHERE mac = ? AND type = ? AND packfile = ?;", blobMACHex, blobType)
	return err
}

// Packfile handling
func (c *SQLState) PutPackfile(packfile objects.MAC, data []byte) error {
	packMACHex := hex.EncodeToString(packfile[:])

	_, err := c.db.Exec("INSERT INTO packfiles(mac, payload) VALUES(?,  ?)", packMACHex, data)
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
		query := "SELECT mac, payload FROM ;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		defer rows.Close()

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
		query := "SELECT mac, payload FROM configurations;"

		rows, err := c.db.Query(query)
		if err != nil {
			return
		}

		defer rows.Close()

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

		for _, v := range result {
			if !yield(v) {
				return
			}
		}
	}
}
