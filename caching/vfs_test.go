package caching

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestVFSCache(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new VFS cache
	repoID := uuid.New()
	scheme := "test"
	origin := "test-origin"
	cache, err := newVFSCache(manager, repoID, scheme, origin, false)
	require.NoError(t, err)
	defer cache.Close()

	// Test filename operations
	t.Run("Filename Operations", func(t *testing.T) {
		pathname := "/test/file.txt"
		data := []byte("test filename data")

		// Test PutFilename
		err := cache.PutPathinfo(pathname, data)
		require.NoError(t, err)

		// Test GetFilename
		retrievedData, err := cache.GetPathinfo(pathname)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetFilename with non-existent path
		_, err = cache.GetPathinfo("/non/existent/file.txt")
		require.NoError(t, err) // Returns nil, nil for non-existent paths
	})

	// Test object operations
	t.Run("Object Operations", func(t *testing.T) {
		mac := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test object data")

		// Test PutObject
		err := cache.PutCachedObject(mac, data)
		require.NoError(t, err)

		// Test GetObject
		retrievedData, err := cache.GetCachedObject(mac)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetObject with non-existent MAC
		nonExistentMAC := [32]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		_, err = cache.GetCachedObject(nonExistentMAC)
		require.NoError(t, err) // Returns nil, nil for non-existent MACs
	})

	// Test multiple operations with same keys
	t.Run("Multiple Operations", func(t *testing.T) {
		pathname := "/test/multiple"
		//dirData := []byte("directory data")
		filenameData := []byte("filename data")

		// Test putting different types of data for the same pathname
		//err := cache.PutDirectory(pathname, dirData)
		//require.NoError(t, err)
		err = cache.PutPathinfo(pathname, filenameData)
		require.NoError(t, err)

		// Verify all data can be retrieved correctly
		//retrievedDirData, err := cache.GetDirectory(pathname)
		//require.NoError(t, err)
		//require.Equal(t, dirData, retrievedDirData)

		retrievedFilenameData, err := cache.GetPathinfo(pathname)
		require.NoError(t, err)
		require.Equal(t, filenameData, retrievedFilenameData)
	})
}
