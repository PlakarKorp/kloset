package sqlite_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/caching/sqlite"
	"github.com/stretchr/testify/require"
)

// TestNewWithSharedOption exercises the Shared option field.
func TestNewWithSharedOption(t *testing.T) {
	dir := t.TempDir()
	opts := &sqlite.Options{
		Shared: true,
	}
	db, err := sqlite.New(dir, "shared.db", opts)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()
}

// TestNewOnDiskReadOnlyExistingFile verifies that Read-only on an existing file
// works without creating a tmp file.
func TestNewOnDiskReadOnlyExistingFile(t *testing.T) {
	dir := t.TempDir()

	// Create first.
	db, err := sqlite.New(dir, "existing.db", nil)
	require.NoError(t, err)
	_, err = db.Exec("CREATE TABLE t (id INTEGER)")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Re-open read-only (file exists → skips the create-tmp-file branch).
	opts := &sqlite.Options{ReadOnly: true}
	db2, err := sqlite.New(dir, "existing.db", opts)
	require.NoError(t, err)
	require.NotNil(t, db2)
	defer db2.Close()
}

// TestNewInMemoryWithCompression verifies the in-memory path with Compressed=true.
func TestNewInMemoryWithCompression(t *testing.T) {
	db, err := sqlite.New("", ":memory:", &sqlite.Options{Compressed: true})
	require.NoError(t, err)
	require.NotNil(t, db)
	defer db.Close()

	// Encode/decode should still work.
	data := []byte("compress me: aaaaaaaaa bbbbbbbbb")
	encoded := db.Encode(data)
	decoded, err := db.Decode(encoded)
	require.NoError(t, err)
	require.Equal(t, data, decoded)
}

// TestDecodeInvalidSnappy verifies that decoding corrupted snappy data returns an error.
func TestDecodeInvalidSnappy(t *testing.T) {
	db, err := sqlite.New("", ":memory:", &sqlite.Options{Compressed: true})
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Decode([]byte{0xff, 0xfe, 0xfd}) // not valid snappy
	require.Error(t, err)
}
