package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func TestCheckCache(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new check cache
	cache, err := newCheckCache(manager)
	require.NoError(t, err)
	defer cache.Close()

	// Test packfile status operations
	t.Run("Packfile Status Operations", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3}
		errData := []byte("test packfile error")

		// Test PutPackfileStatus
		err := cache.PutPackfileStatus(mac, errData)
		require.NoError(t, err)

		// Test GetPackfileStatus
		retrievedData, err := cache.GetPackfileStatus(mac)
		require.NoError(t, err)
		require.Equal(t, errData, retrievedData)
	})

	// Test VFS status operations
	t.Run("VFS Status Operations", func(t *testing.T) {
		mac := objects.MAC{4, 5, 6}
		errData := []byte("test VFS error")

		// Test PutVFSStatus
		err := cache.PutVFSStatus(mac, errData)
		require.NoError(t, err)

		// Test GetVFSStatus
		retrievedData, err := cache.GetVFSStatus(mac)
		require.NoError(t, err)
		require.Equal(t, errData, retrievedData)
	})

	// Test VFS entry status operations
	t.Run("VFS Entry Status Operations", func(t *testing.T) {
		mac := objects.MAC{7, 8, 9}
		errData := []byte("test VFS entry error")

		// Test PutVFSEntryStatus
		err := cache.PutVFSEntryStatus(mac, errData)
		require.NoError(t, err)

		// Test GetVFSEntryStatus
		retrievedData, err := cache.GetVFSEntryStatus(mac)
		require.NoError(t, err)
		require.Equal(t, errData, retrievedData)
	})

	// Test object status operations
	t.Run("Object Status Operations", func(t *testing.T) {
		mac := objects.MAC{10, 11, 12}
		errData := []byte("test object error")

		// Test PutObjectStatus
		err := cache.PutObjectStatus(mac, errData)
		require.NoError(t, err)

		// Test GetObjectStatus
		retrievedData, err := cache.GetObjectStatus(mac)
		require.NoError(t, err)
		require.Equal(t, errData, retrievedData)
	})

	// Test chunk status operations
	t.Run("Chunk Status Operations", func(t *testing.T) {
		mac := objects.MAC{13, 14, 15}
		errData := []byte("test chunk error")

		// Test PutChunkStatus
		err := cache.PutChunkStatus(mac, errData)
		require.NoError(t, err)

		// Test GetChunkStatus
		retrievedData, err := cache.GetChunkStatus(mac)
		require.NoError(t, err)
		require.Equal(t, errData, retrievedData)
	})
}
