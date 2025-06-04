package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestMaintenanceCache(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new maintenance cache
	repositoryID := uuid.New()
	cache, err := newMaintenanceCache(manager, repositoryID)
	require.NoError(t, err)
	defer cache.Close()

	// Test snapshot operations
	t.Run("Snapshot Operations", func(t *testing.T) {
		snapshotID := objects.MAC{1, 2, 3}
		data := []byte("test snapshot data")

		// Test PutSnapshot
		err := cache.PutSnapshot(snapshotID, data)
		require.NoError(t, err)

		// Test HasSnapshot
		exists, err := cache.HasSnapshot(snapshotID)
		require.NoError(t, err)
		require.True(t, exists)

		// Test DeleteSnapshot
		err = cache.DeleteSnapshot(snapshotID)
		require.NoError(t, err)

		// Verify deletion
		exists, err = cache.HasSnapshot(snapshotID)
		require.NoError(t, err)
		require.False(t, exists)
	})

	// Test packfile operations
	t.Run("Packfile Operations", func(t *testing.T) {
		snapshotID := objects.MAC{4, 5, 6}
		packfileMAC := objects.MAC{7, 8, 9}

		// Test PutPackfile
		err := cache.PutPackfile(snapshotID, packfileMAC)
		require.NoError(t, err)

		// Test HasPackfile
		exists := cache.HasPackfile(packfileMAC)
		require.True(t, exists)

		// Test GetPackfiles
		packfiles := cache.GetPackfiles(snapshotID)
		var found bool
		for mac := range packfiles {
			if mac == packfileMAC {
				found = true
				break
			}
		}
		require.True(t, found)

		// Test DeleletePackfiles
		err = cache.DeleletePackfiles(snapshotID)
		require.NoError(t, err)

		// Verify deletion
		exists = cache.HasPackfile(packfileMAC)
		require.False(t, exists)
	})
}
