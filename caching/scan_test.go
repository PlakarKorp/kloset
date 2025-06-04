package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

func TestScanCache(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new scan cache
	snapshotID := [32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	cache, err := newScanCache(manager, snapshotID)
	require.NoError(t, err)

	// Test file operations
	t.Run("File Operations", func(t *testing.T) {
		source := 1
		file := "/test/file.txt"
		data := []byte("test file data")

		// Test PutFile
		err := cache.PutFile(source, file, data)
		require.NoError(t, err)

		// Test GetFile
		retrievedData, err := cache.GetFile(source, file)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetFile with non-existent file
		_, err = cache.GetFile(source, "/non/existent/file.txt")
		require.NoError(t, err) // Returns nil, nil for non-existent files
	})

	// Test directory operations
	t.Run("Directory Operations", func(t *testing.T) {
		source := 1
		directory := "/test/directory"
		data := []byte("test directory data")

		// Test PutDirectory
		err := cache.PutDirectory(source, directory, data)
		require.NoError(t, err)

		// Test GetDirectory
		retrievedData, err := cache.GetDirectory(source, directory)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetDirectory with non-existent directory
		_, err = cache.GetDirectory(source, "/non/existent/directory")
		require.NoError(t, err) // Returns nil, nil for non-existent directories
	})

	// Test summary operations
	t.Run("Summary Operations", func(t *testing.T) {
		source := 1
		pathname := "/test/path"
		data := []byte("test summary data")

		// Test PutSummary with trailing slash
		err := cache.PutSummary(source, pathname+"/", data)
		require.NoError(t, err)

		// Test GetSummary without trailing slash
		retrievedData, err := cache.GetSummary(source, pathname)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test PutSummary with empty pathname
		err = cache.PutSummary(source, "", data)
		require.NoError(t, err)

		// Test GetSummary with empty pathname
		retrievedData, err = cache.GetSummary(source, "")
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetSummary with non-existent pathname
		_, err = cache.GetSummary(source, "/non/existent/path")
		require.NoError(t, err) // Returns nil, nil for non-existent paths
	})

	// Test state operations that should panic
	t.Run("State Operations Panic", func(t *testing.T) {
		stateID := objects.MAC{1, 2, 3}
		data := []byte("test state data")

		// Test PutState (this one should work)
		err := cache.PutState(stateID, data)
		require.NoError(t, err)

		// Test HasState (should panic)
		require.Panics(t, func() {
			cache.HasState(stateID)
		})

		// Test GetState (should panic)
		require.Panics(t, func() {
			cache.GetState(stateID)
		})

		// Test GetStates (should panic)
		require.Panics(t, func() {
			cache.GetStates()
		})

		// Test DelState (should panic)
		require.Panics(t, func() {
			cache.DelState(stateID)
		})
	})

	// Test delta operations
	t.Run("Delta Operations", func(t *testing.T) {
		blobType := resources.RT_OBJECT
		blobCsum := objects.MAC{4, 5, 6}
		packfile := objects.MAC{7, 8, 9}
		data := []byte("test delta data")

		// Test PutDelta
		err := cache.PutDelta(blobType, blobCsum, packfile, data)
		require.NoError(t, err)

		// Test GetDelta
		deltas := cache.GetDelta(blobType, blobCsum)
		var found bool
		for mac, deltaData := range deltas {
			if mac == packfile {
				require.Equal(t, data, deltaData)
				found = true
				break
			}
		}
		require.True(t, found)

		// Test GetDeltasByType
		deltasByType := cache.GetDeltasByType(blobType)
		found = false
		for mac, deltaData := range deltasByType {
			if mac == packfile {
				require.Equal(t, data, deltaData)
				found = true
				break
			}
		}
		require.True(t, found)

		// Test GetDeltas
		allDeltas := cache.GetDeltas()
		found = false
		for mac, deltaData := range allDeltas {
			if mac == packfile {
				require.Equal(t, data, deltaData)
				found = true
				break
			}
		}
		require.True(t, found)

		// Test DelDelta
		err = cache.DelDelta(blobType, blobCsum, packfile)
		require.NoError(t, err)
	})

	// Test deleted operations
	t.Run("Deleted Operations", func(t *testing.T) {
		blobType := resources.RT_OBJECT
		blobCsum := objects.MAC{10, 11, 12}
		data := []byte("test deleted data")

		// Test PutDeleted
		err := cache.PutDeleted(blobType, blobCsum, data)
		require.NoError(t, err)

		// Test HasDeleted
		exists, err := cache.HasDeleted(blobType, blobCsum)
		require.NoError(t, err)
		require.True(t, exists)

		// Test GetDeleteds
		deleteds := cache.GetDeleteds()
		var found bool
		for mac, deletedData := range deleteds {
			if mac == blobCsum {
				require.Equal(t, data, deletedData)
				found = true
				break
			}
		}
		require.True(t, found)

		// Test GetDeletedsByType
		deletedsByType := cache.GetDeletedsByType(blobType)
		found = false
		for mac, deletedData := range deletedsByType {
			if mac == blobCsum {
				require.Equal(t, data, deletedData)
				found = true
				break
			}
		}
		require.True(t, found)

		// Test DelDeleted
		err = cache.DelDeleted(blobType, blobCsum)
		require.NoError(t, err)

		// Verify deletion
		exists, err = cache.HasDeleted(blobType, blobCsum)
		require.NoError(t, err)
		require.False(t, exists)
	})

	// Test packfile operations
	t.Run("Packfile Operations", func(t *testing.T) {
		packfile := objects.MAC{13, 14, 15}
		data := []byte("test packfile data")

		// Test PutPackfile
		err := cache.PutPackfile(packfile, data)
		require.NoError(t, err)

		// Test HasPackfile
		exists, err := cache.HasPackfile(packfile)
		require.NoError(t, err)
		require.True(t, exists)

		// Test GetPackfiles
		packfiles := cache.GetPackfiles()
		var found bool
		for mac, packfileData := range packfiles {
			if mac == packfile {
				require.Equal(t, data, packfileData)
				found = true
				break
			}
		}
		require.True(t, found)

		// Test DelPackfile
		err = cache.DelPackfile(packfile)
		require.NoError(t, err)

		// Verify deletion
		exists, err = cache.HasPackfile(packfile)
		require.NoError(t, err)
		require.False(t, exists)
	})

	// Test configuration operations
	t.Run("Configuration Operations", func(t *testing.T) {
		key := "test_config"
		data := []byte("test configuration data")

		// Test PutConfiguration
		err := cache.PutConfiguration(key, data)
		require.NoError(t, err)

		// Test GetConfiguration
		retrievedData, err := cache.GetConfiguration(key)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetConfigurations
		configs := cache.GetConfigurations()
		var found bool
		for configData := range configs {
			if string(configData) == string(data) {
				found = true
				break
			}
		}
		require.True(t, found)
	})

	// Test key enumeration
	t.Run("Key Enumeration", func(t *testing.T) {
		// Clear the cache by closing and reopening it
		cache.Close()
		cache, err = newScanCache(manager, snapshotID)
		require.NoError(t, err)
		defer cache.Close()

		// Add some test data
		err := cache.PutFile(1, "/test/file1.txt", []byte("data1"))
		require.NoError(t, err)
		err = cache.PutFile(1, "/test/file2.txt", []byte("data2"))
		require.NoError(t, err)
		err = cache.PutFile(2, "/test/file3.txt", []byte("data3"))
		require.NoError(t, err)

		// Test forward enumeration
		keys := make(map[string][]byte)
		for key, value := range cache.EnumerateKeysWithPrefix("__file__:1:", false) {
			keys[key] = value
		}
		require.Len(t, keys, 2)
		expected := map[string][]byte{"/test/file1.txt": []byte("data1"), "/test/file2.txt": []byte("data2")}
		for k, v := range expected {
			require.Contains(t, keys, k)
			require.Equal(t, v, keys[k])
		}

		// Test reverse enumeration
		keys = make(map[string][]byte)
		for key, value := range cache.EnumerateKeysWithPrefix("__file__:1:", true) {
			valCopy := make([]byte, len(value))
			copy(valCopy, value)
			keys[key] = valCopy
		}
		require.Len(t, keys, 2)
		for k, v := range expected {
			require.Contains(t, keys, k)
			require.Equal(t, v, keys[k])
		}
	})
}
