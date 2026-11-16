package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func TestPackfileIDCacheClearsWhenFull(t *testing.T) {
	c := newPackfileIDCache()

	mac := func(i int) objects.MAC {
		var m objects.MAC
		m[0], m[1], m[2] = byte(i), byte(i>>8), byte(i>>16)
		return m
	}

	_, ok := c.Get(mac(0))
	require.False(t, ok)

	for i := 0; i < cacheSz; i++ {
		c.Put(mac(i), int64(i))
	}
	v, ok := c.Get(mac(42))
	require.True(t, ok)
	require.Equal(t, int64(42), v)

	// One past the threshold: the map is cleared and only the new entry is
	// left.
	c.Put(mac(cacheSz), int64(cacheSz))
	require.Len(t, c.m, 1)

	v, ok = c.Get(mac(cacheSz))
	require.True(t, ok)
	require.Equal(t, int64(cacheSz), v)

	_, ok = c.Get(mac(42))
	require.False(t, ok)
}
