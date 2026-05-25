package state

import (
	"errors"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// TestCacheStateLifecycle covers the state-handling methods on the cache:
// PutState, HasState, GetState, GetStates, DelState, GetLatestState.
func TestCacheStateLifecycle(t *testing.T) {
	c := newCache()

	// Empty cache.
	latest, err := c.GetLatestState()
	require.NoError(t, err)
	require.Equal(t, objects.NilMac, latest)

	id1 := objects.MAC{0x01}
	id2 := objects.MAC{0x02}

	require.NoError(t, c.PutState(id1, []byte("one")))
	require.NoError(t, c.PutState(id2, []byte("two")))

	has, err := c.HasState(id1)
	require.NoError(t, err)
	require.True(t, has)

	has, err = c.HasState(objects.MAC{0xFF})
	require.NoError(t, err)
	require.False(t, has)

	data, err := c.GetState(id1)
	require.NoError(t, err)
	require.Equal(t, []byte("one"), data)

	data, err = c.GetState(objects.MAC{0xFF})
	require.NoError(t, err)
	require.Nil(t, data)

	latest, err = c.GetLatestState()
	require.NoError(t, err)
	require.Equal(t, id2, latest)

	all, err := c.GetStates()
	require.NoError(t, err)
	require.Len(t, all, 2)

	require.NoError(t, c.DelState(id1))
	has, err = c.HasState(id1)
	require.NoError(t, err)
	require.False(t, has)

	// Deleting a non-existent state is a no-op.
	require.NoError(t, c.DelState(objects.MAC{0xCC}))
}

// TestCacheUnsupportedMethods exercises every cache method that simply
// returns ErrUnsupported / nil, locking in their existence even though they
// have no behaviour.
func TestCacheUnsupportedMethods(t *testing.T) {
	c := newCache()
	require.ErrorIs(t, c.PutDelta(resources.RT_CHUNK, objects.MAC{}, objects.MAC{}, nil), errors.ErrUnsupported)
	require.Nil(t, c.GetDelta(resources.RT_CHUNK, objects.MAC{}))
	require.Nil(t, c.GetDeltasByType(resources.RT_CHUNK))
	require.Nil(t, c.GetDeltas())
	require.ErrorIs(t, c.DelDelta(resources.RT_CHUNK, objects.MAC{}, objects.MAC{}), errors.ErrUnsupported)
	require.ErrorIs(t, c.PutColoured(resources.RT_CHUNK, objects.MAC{}, nil), errors.ErrUnsupported)
	_, err := c.HasColoured(resources.RT_CHUNK, objects.MAC{})
	require.ErrorIs(t, err, errors.ErrUnsupported)
	require.ErrorIs(t, c.DelColoured(resources.RT_CHUNK, objects.MAC{}), errors.ErrUnsupported)
	require.Nil(t, c.GetColouredEntriesByType(resources.RT_CHUNK))
	require.Nil(t, c.GetColouredEntries())
	require.ErrorIs(t, c.PutPackfile(objects.MAC{}, nil), errors.ErrUnsupported)
	require.ErrorIs(t, c.DelPackfile(objects.MAC{}), errors.ErrUnsupported)
	_, err = c.HasPackfile(objects.MAC{})
	require.ErrorIs(t, err, errors.ErrUnsupported)
	require.Nil(t, c.GetPackfiles())
	require.ErrorIs(t, c.PutConfiguration("k", nil), errors.ErrUnsupported)
	_, err = c.GetConfiguration("k")
	require.ErrorIs(t, err, errors.ErrUnsupported)
	require.Nil(t, c.GetConfigurations())
}

// TestCacheBatch exercises the batch type returned by NewBatch.
func TestCacheBatch(t *testing.T) {
	c := newCache()
	b := c.NewBatch()
	require.NotNil(t, b)

	require.ErrorIs(t, b.Put(nil, nil), errors.ErrUnsupported)
	require.ErrorIs(t, b.PutDelta(resources.RT_CHUNK, objects.MAC{}, objects.MAC{}, nil), errors.ErrUnsupported)
	require.Equal(t, uint32(0), b.Count())
	require.NoError(t, b.Commit())
	// batch.Close is unexported on the StateBatch interface, but the concrete
	// type implements it — we exercise it via type assertion if available.
	if closer, ok := b.(interface{ Close() error }); ok {
		require.NoError(t, closer.Close())
	}
}

// TestCachePutDeletedPanics asserts that PutDeleted panics ("nop") — locking
// in the contract that this helper does not support deletions.
func TestCachePutDeletedPanics(t *testing.T) {
	c := newCache()
	require.Panics(t, func() { _ = c.PutDeleted(0, objects.MAC{}, nil) })
}

// TestCacheGetDeletedEntriesPanics asserts that GetDeletedEntries panics.
func TestCacheGetDeletedEntriesPanics(t *testing.T) {
	c := newCache()
	require.Panics(t, func() { _ = c.GetDeletedEntries() })
}
