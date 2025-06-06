package lru

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCache(t *testing.T) {
	t.Run("New Cache", func(t *testing.T) {
		cache := New[int, string](10, nil)
		require.NotNil(t, cache)
		require.Equal(t, 10, cache.target)
		require.NotNil(t, cache.items)
		require.Equal(t, 0, cache.size)
	})

	t.Run("Put and Get", func(t *testing.T) {
		cache := New[int, string](3, nil)
		defer cache.Close()

		// Test putting and getting a value
		err := cache.Put(1, "one")
		require.NoError(t, err)

		val, ok := cache.Get(1)
		require.True(t, ok)
		require.Equal(t, "one", val)

		// Test getting non-existent value
		val, ok = cache.Get(2)
		require.False(t, ok)
		require.Empty(t, val)
	})

	t.Run("Eviction", func(t *testing.T) {
		cache := New[int, string](2, nil)
		defer cache.Close()

		// Fill the cache
		err := cache.Put(1, "one")
		require.NoError(t, err)
		err = cache.Put(2, "two")
		require.NoError(t, err)

		// Add one more item, should evict the oldest (1)
		err = cache.Put(3, "three")
		require.NoError(t, err)

		// Check that 1 was evicted
		val, ok := cache.Get(1)
		require.False(t, ok)
		require.Empty(t, val)

		// Check that 2 and 3 are still there
		val, ok = cache.Get(2)
		require.True(t, ok)
		require.Equal(t, "two", val)

		val, ok = cache.Get(3)
		require.True(t, ok)
		require.Equal(t, "three", val)
	})

	t.Run("OnEvict Callback", func(t *testing.T) {
		evicted := make(map[int]string)
		onEvict := func(key int, val string) error {
			evicted[key] = val
			return nil
		}

		cache := New[int, string](2, onEvict)
		defer cache.Close()

		// Fill the cache
		err := cache.Put(1, "one")
		require.NoError(t, err)
		err = cache.Put(2, "two")
		require.NoError(t, err)

		// Add one more item, should evict 1
		err = cache.Put(3, "three")
		require.NoError(t, err)

		// Check that 1 was evicted and onEvict was called
		require.Equal(t, "one", evicted[1])
	})

	t.Run("OnEvict Error", func(t *testing.T) {
		expectedErr := errors.New("eviction error")
		onEvict := func(key int, val string) error {
			return expectedErr
		}

		cache := New[int, string](2, onEvict)
		defer cache.Close()

		// Fill the cache
		err := cache.Put(1, "one")
		require.NoError(t, err)
		err = cache.Put(2, "two")
		require.NoError(t, err)

		// Try to add one more item, should fail due to eviction error
		err = cache.Put(3, "three")
		require.Equal(t, expectedErr, err)
	})

	t.Run("Stats", func(t *testing.T) {
		cache := New[int, string](3, nil)
		defer cache.Close()

		// Test initial stats
		hits, misses, size := cache.Stats()
		require.Equal(t, uint64(0), hits)
		require.Equal(t, uint64(0), misses)
		require.Equal(t, uint64(0), size)

		// Add some items
		err := cache.Put(1, "one")
		require.NoError(t, err)
		err = cache.Put(2, "two")
		require.NoError(t, err)

		// Test some gets
		_, ok := cache.Get(1) // hit
		require.True(t, ok)
		_, ok = cache.Get(3) // miss
		require.False(t, ok)

		// Check updated stats
		hits, misses, size = cache.Stats()
		require.Equal(t, uint64(1), hits)
		require.Equal(t, uint64(1), misses)
		require.Equal(t, uint64(2), size)
	})

	t.Run("Update Existing", func(t *testing.T) {
		cache := New[int, string](3, nil)
		defer cache.Close()

		// Add initial value
		err := cache.Put(1, "one")
		require.NoError(t, err)

		// Update existing value
		err = cache.Put(1, "ONE")
		require.NoError(t, err)

		// Check updated value
		val, ok := cache.Get(1)
		require.True(t, ok)
		require.Equal(t, "ONE", val)

		// Check size hasn't increased
		hits, misses, size := cache.Stats()
		require.Equal(t, uint64(1), hits)
		require.Equal(t, uint64(0), misses)
		require.Equal(t, uint64(1), size)
	})
}
