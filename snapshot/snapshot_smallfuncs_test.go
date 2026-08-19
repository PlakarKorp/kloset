package snapshot_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewReaderFile reads a regular file out of a snapshot.
func TestNewReaderFile(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	rd, err := snap.NewReader("/docs/readme.txt")
	require.NoError(t, err)
	defer rd.Close()

	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, "README content", string(data))
}

// TestNewReaderDirectory verifies NewReader rejects directories.
func TestNewReaderDirectory(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	_, err := snap.NewReader("/docs")
	require.Error(t, err)
}

// TestNewReaderMissing verifies NewReader surfaces a not-found error.
func TestNewReaderMissing(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	_, err := snap.NewReader("/does/not/exist")
	require.Error(t, err)
}

// TestDirPackAccessor exercises DirPackRoot/DirPack.
func TestDirPackAccessor(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	_, found := snap.DirPackRoot()
	tree, err := snap.DirPack()
	require.NoError(t, err)
	if !found {
		require.Nil(t, tree)
	}
}
