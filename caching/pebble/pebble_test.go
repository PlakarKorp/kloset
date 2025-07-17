package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPebbleCache(t *testing.T) {
	// Create a temporary directory for the Pebble database
	tmpDir := t.TempDir()

	// Initialize a new PebbleCache
	cache, err := New(tmpDir)
	require.NoError(t, err)
	defer cache.Close()

	// Test put operation
	t.Run("Put Operation", func(t *testing.T) {
		key := []byte("test:prefix")
		data := []byte("test data")

		// Put data into the cache
		err := cache.Put(key, data)
		require.NoError(t, err)
	})

	// Test has operation
	t.Run("Has Operation", func(t *testing.T) {
		key := []byte("test:prefix")

		// Check if the key exists
		exists, err := cache.Has(key)
		require.NoError(t, err)
		require.True(t, exists)
	})

	// Test get operation
	t.Run("Get Operation", func(t *testing.T) {
		key := []byte("test:prefix")
		expectedData := []byte("test data")

		// Get data from the cache
		retrievedData, err := cache.Get(key)
		require.NoError(t, err)
		require.Equal(t, expectedData, retrievedData)
	})

	// Test delete operation
	t.Run("Delete Operation", func(t *testing.T) {
		key := []byte("test:prefix")

		// Delete the key from the cache
		err := cache.Delete(key)
		require.NoError(t, err)

		// Verify the key is deleted
		exists, err := cache.Has(key)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
