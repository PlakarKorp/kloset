package caching

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
		prefix := "test_prefix"
		key := "test_key"
		data := []byte("test data")

		// Put data into the cache
		err := cache.put(prefix, key, data)
		require.NoError(t, err)
	})

	// Test has operation
	t.Run("Has Operation", func(t *testing.T) {
		prefix := "test_prefix"
		key := "test_key"

		// Check if the key exists
		exists, err := cache.has(prefix, key)
		require.NoError(t, err)
		require.True(t, exists)
	})

	// Test get operation
	t.Run("Get Operation", func(t *testing.T) {
		prefix := "test_prefix"
		key := "test_key"
		expectedData := []byte("test data")

		// Get data from the cache
		retrievedData, err := cache.get(prefix, key)
		require.NoError(t, err)
		require.Equal(t, expectedData, retrievedData)
	})

	// Test delete operation
	t.Run("Delete Operation", func(t *testing.T) {
		prefix := "test_prefix"
		key := "test_key"

		// Delete the key from the cache
		err := cache.delete(prefix, key)
		require.NoError(t, err)

		// Verify the key is deleted
		exists, err := cache.has(prefix, key)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
