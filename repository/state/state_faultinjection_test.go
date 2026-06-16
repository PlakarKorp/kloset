package state

import (
	"bytes"
	"errors"
	"io"
	"iter"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// errFault is the sentinel error returned by the fault-injecting reader/writer.
var errFault = errors.New("injected fault")

// failAfterWriter writes successfully until the cumulative byte count exceeds
// `after`, after which every Write returns errFault. This lets a single test
// drive SerializeToStream's write-error branches at any depth by choosing the
// cutoff just before the targeted write.
type failAfterWriter struct {
	after   int
	written int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.written >= w.after {
		return 0, errFault
	}
	remaining := w.after - w.written
	if len(p) <= remaining {
		w.written += len(p)
		return len(p), nil
	}
	// Partial write up to the cutoff, then fault.
	w.written += remaining
	return remaining, errFault
}

// failAfterReader reads successfully until the cumulative byte count exceeds
// `after`, then returns errFault. Mirrors failAfterWriter for the deserialize
// read-error branches.
type failAfterReader struct {
	data []byte
	pos  int
	max  int
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.pos >= r.max {
		return 0, errFault
	}
	end := r.pos + len(p)
	if end > r.max {
		end = r.max
	}
	if end > len(r.data) {
		end = len(r.data)
	}
	n := copy(p, r.data[r.pos:end])
	r.pos += n
	if n == 0 {
		return 0, errFault
	}
	return n, nil
}

// seedAllEntryTypes populates a state with one entry of every serialized type
// (DELETE, LOCATIONS, COLOURED, PACKFILE, CONFIGURATION) so that
// SerializeToStream walks all of its data-bearing loops.
func seedAllEntryTypes(t *testing.T) *LocalState {
	t.Helper()
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	require.NoError(t, st.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Blob: objects.MAC{0x01},
		Location: Location{Packfile: objects.MAC{0x10}},
	}))
	require.NoError(t, st.ColourResource(resources.RT_OBJECT, objects.MAC{0x02}))
	require.NoError(t, st.PutPackfile(objects.MAC{0x03}, objects.MAC{0x30}))
	require.NoError(t, st.DelDelta(resources.RT_OBJECT, objects.MAC{0x04}, objects.MAC{0x40}))
	require.NoError(t, st.SetConfiguration("sk", []byte("sv")))

	return st
}

// TestSerializeToStreamWriteErrorsAtEveryOffset drives SerializeToStream
// against a writer that faults at every possible byte offset of a fully
// populated state. Each cutoff exercises a different "failed to write ..."
// branch (header, each entry type's type/length/payload, and the metadata
// trailer fields).
func TestSerializeToStreamWriteErrorsAtEveryOffset(t *testing.T) {
	// Learn the full serialized length first.
	var probe bytes.Buffer
	require.NoError(t, seedAllEntryTypes(t).SerializeToStream(&probe))
	total := probe.Len()
	require.Greater(t, total, 0)

	for cutoff := 0; cutoff < total; cutoff++ {
		st := seedAllEntryTypes(t)
		w := &failAfterWriter{after: cutoff}
		err := st.SerializeToStream(w)
		require.ErrorIs(t, err, errFault, "cutoff=%d should fault mid-serialize", cutoff)
	}

	// Sanity: with no fault, serialization succeeds.
	st := seedAllEntryTypes(t)
	var ok bytes.Buffer
	require.NoError(t, st.SerializeToStream(&ok))
	require.Equal(t, total, ok.Len())
}

// TestDeserializeFromStreamReadErrorsAtEveryOffset drives deserializeFromStream
// against a reader that faults at every byte offset of a valid serialized
// state, exercising every "failed to read ..." branch.
func TestDeserializeFromStreamReadErrorsAtEveryOffset(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, seedAllEntryTypes(t).SerializeToStream(&buf))
	valid := buf.Bytes()
	require.Greater(t, len(valid), 0)

	for cutoff := 0; cutoff < len(valid); cutoff++ {
		cache := newMockStateCache()
		st, err := NewLocalState(cache)
		require.NoError(t, err)

		r := &failAfterReader{data: valid, max: cutoff}
		err = st.deserializeFromStream(r)
		require.Error(t, err, "cutoff=%d should fail to deserialize", cutoff)
	}

	// Sanity: the full stream deserializes cleanly.
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	require.NoError(t, st.deserializeFromStream(bytes.NewReader(valid)))
}

// corruptDeltaCache is a StateCache whose delta iterators always yield a
// malformed buffer, so the DeltaEntryFromBytes error branch of the List*
// iterators is exercised.
type corruptDeltaCache struct {
	*mockStateCache
}

func (c *corruptDeltaCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		yield(objects.NilMac, []byte{0x00, 0x01})
	}
}

func (c *corruptDeltaCache) GetDeltasByType(resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		yield(objects.NilMac, []byte{0x00, 0x01})
	}
}

// TestListIteratorsParseError feeds a corrupt delta buffer through the List*
// iterators and asserts the DeltaEntryFromBytes error is surfaced.
func TestListIteratorsParseError(t *testing.T) {
	cache := &corruptDeltaCache{newMockStateCache()}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	t.Run("ListSnapshots", func(t *testing.T) {
		var sawErr bool
		for _, err := range st.ListSnapshots() {
			if err != nil {
				sawErr = true
			}
		}
		require.True(t, sawErr)
	})

	t.Run("ListObjectsOfType", func(t *testing.T) {
		var sawErr bool
		for _, err := range st.ListObjectsOfType(resources.RT_OBJECT) {
			if err != nil {
				sawErr = true
			}
		}
		require.True(t, sawErr)
	})

	t.Run("ListOrphanDeltas", func(t *testing.T) {
		var sawErr bool
		for _, err := range st.ListOrphanDeltas() {
			if err != nil {
				sawErr = true
			}
		}
		require.True(t, sawErr)
	})
}

// hasPackfileErrCache returns an error from HasPackfile, exercising the
// HasPackfile-error yield branches in the List* iterators. The delta iterators
// yield one well-formed entry so the iterator reaches the HasPackfile call.
type hasPackfileErrCache struct {
	*mockStateCache
	validDelta []byte
}

func (c *hasPackfileErrCache) HasPackfile(objects.MAC) (bool, error) {
	return false, errFault
}

func (c *hasPackfileErrCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) { yield(objects.NilMac, c.validDelta) }
}

func (c *hasPackfileErrCache) GetDeltasByType(resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) { yield(objects.NilMac, c.validDelta) }
}

// TestListIteratorsHasPackfileError exercises the HasPackfile-error branch of
// each List* iterator.
func TestListIteratorsHasPackfileError(t *testing.T) {
	delta := &DeltaEntry{
		Type: resources.RT_SNAPSHOT, Blob: objects.MAC{0xAA},
		Location: Location{Packfile: objects.MAC{0xBB}},
	}
	cache := &hasPackfileErrCache{mockStateCache: newMockStateCache(), validDelta: delta.ToBytes()}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	for _, lister := range []func() bool{
		func() bool {
			for _, e := range st.ListSnapshots() {
				if e != nil {
					return true
				}
			}
			return false
		},
		func() bool {
			for _, e := range st.ListObjectsOfType(resources.RT_SNAPSHOT) {
				if e != nil {
					return true
				}
			}
			return false
		},
		func() bool {
			for _, e := range st.ListOrphanDeltas() {
				if e != nil {
					return true
				}
			}
			return false
		},
	} {
		require.True(t, lister(), "iterator should surface HasPackfile error")
	}
}

// Ensure the wrapper caches still satisfy the full StateCache interface.
var (
	_ caching.StateCache = (*corruptDeltaCache)(nil)
	_ caching.StateCache = (*hasPackfileErrCache)(nil)
	_ io.Writer          = (*failAfterWriter)(nil)
	_ io.Reader          = (*failAfterReader)(nil)
)
