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
		err := cache.PutCachedPath(pathname, data)
		require.NoError(t, err)

		// Test GetFilename
		retrievedData, err := cache.GetCachedPath(pathname)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetFilename with non-existent path
		_, err = cache.GetCachedPath("/non/existent/file.txt")
		require.NoError(t, err) // Returns nil, nil for non-existent paths
	})

	// Test multiple operations with same keys
	t.Run("Multiple Operations", func(t *testing.T) {
		pathname := "/test/multiple"
		filenameData := []byte("filename data")

		// Test putting different types of data for the same pathname
		err = cache.PutCachedPath(pathname, filenameData)
		require.NoError(t, err)

		// Verify all data can be retrieved correctly
		retrievedFilenameData, err := cache.GetCachedPath(pathname)
		require.NoError(t, err)
		require.Equal(t, filenameData, retrievedFilenameData)
	})
}
