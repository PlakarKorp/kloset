package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRepositoryCache(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new repository cache
	repoID := uuid.New()
	cache, err := newRepositoryCache(manager, repoID)
	require.NoError(t, err)
	defer cache.Close()

	// Test state operations
	t.Run("State Operations", func(t *testing.T) {
		stateID := objects.MAC{1, 2, 3}
		data := []byte("test state data")

		// Test PutState
		err := cache.PutState(stateID, data)
		require.NoError(t, err)

		// Test HasState
		exists, err := cache.HasState(stateID)
		require.NoError(t, err)
		require.True(t, exists)

		// Test GetState
		retrievedData, err := cache.GetState(stateID)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test GetStates
		states, err := cache.GetStates()
		require.NoError(t, err)
		require.Len(t, states, 1)
		require.Equal(t, data, states[stateID])

		// Test DelState
		err = cache.DelState(stateID)
		require.NoError(t, err)

		// Verify deletion
		exists, err = cache.HasState(stateID)
		require.NoError(t, err)
		require.False(t, exists)
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

	// Test snapshot operations
	t.Run("Snapshot Operations", func(t *testing.T) {
		stateID := objects.MAC{16, 17, 18}
		data := []byte("test snapshot data")

		// Test PutSnapshot
		err := cache.PutSnapshot(stateID, data)
		require.NoError(t, err)

		// Test HasSnapshot
		exists, err := cache.HasSnapshot(stateID)
		require.NoError(t, err)
		require.True(t, exists)

		// Test GetSnapshot
		retrievedData, err := cache.GetSnapshot(stateID)
		require.NoError(t, err)
		require.Equal(t, data, retrievedData)

		// Test DelSnapshot
		err = cache.DelSnapshot(stateID)
		require.NoError(t, err)

		// Verify deletion
		exists, err = cache.HasSnapshot(stateID)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
