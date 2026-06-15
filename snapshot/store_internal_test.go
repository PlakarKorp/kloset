package snapshot

import (
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// TestSnapshotStoreReadOnly covers the read-only SnapshotStore methods that the
// public-API tests never reach: Update always rejects, Put without a builder
// rejects, and Close is a no-op.
func TestSnapshotStoreReadOnly(t *testing.T) {
	s := &SnapshotStore[string, objects.MAC]{
		blobtype: resources.RT_VFS_NODE,
	}

	node := &btree.Node[string, objects.MAC, objects.MAC]{}

	err := s.Update(objects.MAC{}, node)
	require.ErrorIs(t, err, ErrReadOnly)

	_, err = s.Put(node)
	require.ErrorIs(t, err, ErrReadOnly)

	require.NoError(t, s.Close())
}

// TestMatchmime exercises every branch of the matchmime predicate directly.
func TestMatchmime(t *testing.T) {
	// Empty match list matches anything.
	require.True(t, matchmime(nil, "text/plain"))

	// Full type/subtype equality on the first component (the m[0]==t[0] branch).
	require.True(t, matchmime([]string{"text/plain"}, "text/plain"))

	// Top-level type match only ("text" matches "text/html").
	require.True(t, matchmime([]string{"text"}, "text/html"))

	// No match.
	require.False(t, matchmime([]string{"image"}, "text/plain"))
	require.False(t, matchmime([]string{"image/png"}, "text/plain"))
}
