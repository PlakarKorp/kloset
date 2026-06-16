package state

import (
	"bytes"
	"iter"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// TestDeserializeFromStreamV100ReadErrorsAtEveryOffset drives the v1.0.0
// deserializer against a reader that faults at every byte offset of a valid
// v100 stream, covering its read-error branches.
func TestDeserializeFromStreamV100ReadErrorsAtEveryOffset(t *testing.T) {
	valid := buildV100Stream(t).Bytes()
	require.Greater(t, len(valid), 0)

	for cutoff := 0; cutoff < len(valid); cutoff++ {
		cache := newMockStateCache()
		st, err := NewLocalState(cache)
		require.NoError(t, err)

		r := &failAfterReader{data: valid, max: cutoff}
		err = st.deserializeFromStreamv100(r)
		require.Error(t, err, "cutoff=%d should fail to deserialize v100", cutoff)
	}

	// Sanity: full stream still deserializes.
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	require.NoError(t, st.deserializeFromStreamv100(bytes.NewReader(valid)))
}

// corruptFromCache yields a malformed buffer for each entry-type iterator, so
// mergeFromCache hits every "failed to deserialize ... entry" branch depending
// on which iterators are populated.
type corruptFromCache struct {
	*mockStateCache
	delta, coloured, packfile, configuration bool
}

func (c *corruptFromCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		if c.delta {
			yield(objects.NilMac, []byte{0xFF})
		}
	}
}

func (c *corruptFromCache) GetColouredEntries() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		if c.coloured {
			yield(objects.NilMac, []byte{0xFF})
		}
	}
}

func (c *corruptFromCache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		if c.packfile {
			yield(objects.NilMac, []byte{0xFF})
		}
	}
}

func (c *corruptFromCache) GetConfigurations() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if c.configuration {
			yield([]byte{0xFF})
		}
	}
}

// TestMergeFromCacheParseErrors exercises each "failed to deserialize ... entry"
// branch of mergeFromCache by feeding a corrupt buffer through one iterator at
// a time.
func TestMergeFromCacheParseErrors(t *testing.T) {
	cases := []struct {
		name string
		from *corruptFromCache
	}{
		{"delta", &corruptFromCache{mockStateCache: newMockStateCache(), delta: true}},
		{"coloured", &corruptFromCache{mockStateCache: newMockStateCache(), coloured: true}},
		{"packfile", &corruptFromCache{mockStateCache: newMockStateCache(), packfile: true}},
		{"configuration", &corruptFromCache{mockStateCache: newMockStateCache(), configuration: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dstCache := newMockStateCache()
			dst, err := NewLocalState(dstCache)
			require.NoError(t, err)

			err = dst.mergeFromCache(tc.from)
			require.Error(t, err)
		})
	}
}

// stateOpErrCache lets individual StateCache state operations be forced to
// error, so the HasState / PutState error returns in MergeState / PutState /
// MergeStateFromCache are reached.
type stateOpErrCache struct {
	*mockStateCache
	failHasState bool
	failPutState bool
}

func (c *stateOpErrCache) HasState(stateID objects.MAC) (bool, error) {
	if c.failHasState {
		return false, errFault
	}
	return c.mockStateCache.HasState(stateID)
}

func (c *stateOpErrCache) PutState(stateID objects.MAC, data []byte) error {
	if c.failPutState {
		return errFault
	}
	return c.mockStateCache.PutState(stateID, data)
}

// TestPutStateCacheError covers the cache.PutState error return of PutState.
func TestPutStateCacheError(t *testing.T) {
	cache := &stateOpErrCache{mockStateCache: newMockStateCache(), failPutState: true}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	require.ErrorIs(t, st.PutState(objects.MAC{0x01}), errFault)
}

// TestMergeStateHasStateError covers the HasState error return at the top of
// MergeState (and, by extension, MergeStateFromCache).
func TestMergeStateHasStateError(t *testing.T) {
	cache := &stateOpErrCache{mockStateCache: newMockStateCache(), failHasState: true}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.MergeState(objects.MAC{0x02}, bytes.NewReader(nil), versioning.FromString(VERSION))
	require.ErrorIs(t, err, errFault)

	err = st.MergeStateFromCache(objects.MAC{0x03}, newMockStateCache())
	require.ErrorIs(t, err, errFault)
}

// TestMergeStatePutStateError covers the PutState error return at the bottom of
// MergeState: a valid stream deserializes, but publishing the merged state
// fails.
func TestMergeStatePutStateError(t *testing.T) {
	cache := &stateOpErrCache{mockStateCache: newMockStateCache(), failPutState: true}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, seedAllEntryTypes(t).SerializeToStream(&buf))

	err = st.MergeState(objects.MAC{0x04}, &buf, versioning.FromString(VERSION))
	require.ErrorIs(t, err, errFault)
}

var (
	_ caching.StateCache = (*corruptFromCache)(nil)
	_ caching.StateCache = (*stateOpErrCache)(nil)
	_ resources.Type     = resources.RT_CHUNK
)
