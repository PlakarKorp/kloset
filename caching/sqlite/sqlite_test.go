package sqlite_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/stretchr/testify/require"
)

func TestNewInMemory(t *testing.T) {
	db, err := sqlite.New("", ":memory:", nil)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewOnDisk(t *testing.T) {
	dir := t.TempDir()
	db, err := sqlite.New(dir, "test.db", nil)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestNewWithOptions(t *testing.T) {
	dir := t.TempDir()
	opts := &sqlite.Options{
		DeleteOnClose: true,
		Compressed:    false,
		ReadOnly:      false,
	}
	db, err := sqlite.New(dir, "opts.db", opts)
	require.NoError(t, err)
	require.NotNil(t, db)
	err = db.Close()
	require.NoError(t, err)
}

func TestNewReadOnly(t *testing.T) {
	dir := t.TempDir()

	// Create the database first
	db, err := sqlite.New(dir, "readonly.db", nil)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Open as read-only
	opts := &sqlite.Options{ReadOnly: true}
	db2, err := sqlite.New(dir, "readonly.db", opts)
	require.NoError(t, err)
	require.NotNil(t, db2)
	defer db2.Close()
}

func TestNewReadOnlyCreatesFileIfMissing(t *testing.T) {
	dir := t.TempDir()

	// Should create the file and open read-only
	opts := &sqlite.Options{ReadOnly: true}
	db, err := sqlite.New(dir, "new_readonly.db", opts)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

func TestEncodeDecodeNoCompression(t *testing.T) {
	db, err := sqlite.New("", ":memory:", &sqlite.Options{Compressed: false})
	require.NoError(t, err)
	defer db.Close()

	data := []byte("hello, world!")
	encoded := db.Encode(data)
	require.Equal(t, data, encoded)

	decoded, err := db.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
}

func TestEncodeDecodeWithCompression(t *testing.T) {
	db, err := sqlite.New("", ":memory:", &sqlite.Options{Compressed: true})
	require.NoError(t, err)
	defer db.Close()

	data := []byte("this is some compressible data that repeats: aaaaaaaaaaaaaaaaaaaaaaaa")
	encoded := db.Encode(data)
	// Encoded should differ from original (compressed)
	require.NotEqual(t, data, encoded)

	decoded, err := db.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
}

func TestEncodeDecodeEmpty(t *testing.T) {
	db, err := sqlite.New("", ":memory:", &sqlite.Options{Compressed: true})
	require.NoError(t, err)
	defer db.Close()

	encoded := db.Encode([]byte{})
	require.Empty(t, encoded)

	decoded, err := db.Decode([]byte{})
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestClose(t *testing.T) {
	db, err := sqlite.New("", ":memory:", nil)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}

func TestUsableAsDatabase(t *testing.T) {
	db, err := sqlite.New("", ":memory:", nil)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO test (val) VALUES (?)`, "hello")
	require.NoError(t, err)

	var val string
	err = db.QueryRow(`SELECT val FROM test WHERE id = 1`).Scan(&val)
	require.NoError(t, err)
	require.Equal(t, "hello", val)
}

func TestDeleteOnClose(t *testing.T) {
	dir := t.TempDir()
	opts := &sqlite.Options{DeleteOnClose: true}

	db, err := sqlite.New(dir, "deleteme.db", opts)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
}
