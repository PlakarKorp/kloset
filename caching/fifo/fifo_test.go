package fifo

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	cache := New[int, string](10, nil)
	require.NotNil(t, cache)
	require.Equal(t, 11, cache.size) // works in modulo, so size is bumped
	require.NotNil(t, cache.buffer)
	require.NotNil(t, cache.items)
}

func TestPutAndGet(t *testing.T) {
	cache := New[int, string](2, nil)

	err := cache.Put(1, "one")
	require.NoError(t, err)

	// validate head and tail
	require.Equal(t, 1, cache.head)
	require.Equal(t, 0, cache.tail)

	val, ok := cache.Get(1)
	require.True(t, ok)
	require.Equal(t, "one", val)

	// non-existent
	val, ok = cache.Get(2)
	require.False(t, ok)
	require.Empty(t, val)
}

func TestPutOverrides(t *testing.T) {
	cache := New[int, string](2, nil)

	var err error

	err = cache.Put(1, "one")
	require.NoError(t, err)

	err = cache.Put(1, "ONE")
	require.NoError(t, err)

	val, ok := cache.Get(1)
	require.True(t, ok)
	require.Equal(t, "ONE", val)
}

func TestPutEvicts(t *testing.T) {
	cache := New[int, string](2, nil)

	var err error

	// Fill the cache
	err = cache.Put(1, "one")
	require.NoError(t, err)
	err = cache.Put(2, "two")
	require.NoError(t, err)

	// validate head and tail
	require.Equal(t, 2, cache.head)
	require.Equal(t, 0, cache.tail)

	// inserting 3 should work
	err = cache.Put(3, "three")
	require.NoError(t, err)

	// one should be evicted
	val, ok := cache.Get(1)
	require.False(t, ok)
	require.Empty(t, val)

	// but two and three should still be there
	val, ok = cache.Get(2)
	require.True(t, ok)
	require.Equal(t, "two", val)

	val, ok = cache.Get(3)
	require.True(t, ok)
	require.Equal(t, "three", val)

	// (re)validate head and tail
	require.Equal(t, 0, cache.head)
	require.Equal(t, 1, cache.tail)

}

func TestPutCallsOnEvict(t *testing.T) {
	evicted := make(map[int]string)
	onevict := func(key int, val string) error {
		evicted[key] = val
		return nil
	}

	cache := New(2, onevict)

	var err error

	// Fill the cache
	err = cache.Put(1, "one")
	require.NoError(t, err)
	err = cache.Put(2, "two")
	require.NoError(t, err)

	// inserting 3 should evict one
	err = cache.Put(3, "three")
	require.NoError(t, err)

	// check that the eviction happened, and only one was evicted.
	require.Equal(t, "one", evicted[1])
	require.Equal(t, 1, len(evicted))
}

func TestPutFailsOnEvictFailure(t *testing.T) {
	expectedErr := errors.New("eviction error")

	cache := New(2, func(key int, val string) error { return expectedErr })

	var err error

	// Fill the cache
	err = cache.Put(1, "one")
	require.NoError(t, err)
	err = cache.Put(2, "two")
	require.NoError(t, err)

	// This should fail since it hits onevict
	err = cache.Put(3, "three")
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

func TestCloseFailsOnEvictFailure(t *testing.T) {
	expectedErr := errors.New("eviction error")

	cache := New(2, func(key int, val string) error { return expectedErr })

	var err error

	// Fill the cache
	err = cache.Put(1, "one")
	require.NoError(t, err)
	err = cache.Put(2, "two")
	require.NoError(t, err)

	// This should fail since it hits onevict
	err = cache.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

func TestStats(t *testing.T) {
	cache := New[int, string](2, nil)

	hits, misses, size := cache.Stats()
	require.Zero(t, hits)
	require.Zero(t, misses)
	require.Zero(t, size)

	var (
		err error
		val string
		ok  bool
	)

	// Add some items
	err = cache.Put(1, "one")
	require.NoError(t, err)

	// test updated stats
	hits, misses, size = cache.Stats()
	require.Zero(t, hits)
	require.Zero(t, misses)
	require.Equal(t, uint64(1), size)

	err = cache.Put(2, "two")
	require.NoError(t, err)

	hits, misses, size = cache.Stats()
	require.Zero(t, hits)
	require.Zero(t, misses)
	require.Equal(t, uint64(2), size)

	// test some gets
	val, ok = cache.Get(1) // hit
	require.True(t, ok)
	require.Equal(t, "one", val)

	val, ok = cache.Get(3) // miss
	require.False(t, ok)
	require.Zero(t, val)

	// test updated stats
	hits, misses, size = cache.Stats()
	require.Equal(t, uint64(1), hits)
	require.Equal(t, uint64(1), misses)
	require.Equal(t, uint64(2), size)
}
