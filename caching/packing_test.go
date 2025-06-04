package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

func TestPackingCache(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new packing cache
	cache, err := newPackingCache(manager)
	require.NoError(t, err)
	defer cache.Close()

	// Test blob operations
	t.Run("Blob Operations", func(t *testing.T) {
		blobType := resources.RT_OBJECT
		blobMAC := objects.MAC{1, 2, 3}

		// Test PutBlob
		err := cache.PutBlob(blobType, blobMAC)
		require.NoError(t, err)

		// Test HasBlob
		exists, err := cache.HasBlob(blobType, blobMAC)
		require.NoError(t, err)
		require.True(t, exists)

		// Test HasBlob with non-existent blob
		nonExistentMAC := objects.MAC{4, 5, 6}
		exists, err = cache.HasBlob(blobType, nonExistentMAC)
		require.NoError(t, err)
		require.False(t, exists)
	})

	// Test index blob operations
	t.Run("Index Blob Operations", func(t *testing.T) {
		blobType := resources.RT_OBJECT
		blobMAC := objects.MAC{7, 8, 9}
		data := []byte("test index data")

		// Test PutIndexBlob
		err := cache.PutIndexBlob(blobType, blobMAC, data)
		require.NoError(t, err)

		// Test GetIndexesBlob
		indexes := cache.GetIndexesBlob()
		var found bool
		for indexData := range indexes {
			if string(indexData) == string(data) {
				found = true
				break
			}
		}
		require.True(t, found)
	})
}
