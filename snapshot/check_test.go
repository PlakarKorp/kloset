package snapshot_test

import (
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	fs, err := snap.Filesystem()
	require.NoError(t, err)
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	err = snap.Check(filepath, &snapshot.CheckOptions{})
	require.NoError(t, err)
}

// TestCheckFullSnapshot runs Check against the snapshot root, exercising the
// walk over every entry instead of a single file.
func TestCheckFullSnapshot(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
}

// TestCheckFastMode runs Check with FastCheck enabled.  The fast path skips
// the chunk-level MAC recomputation and just verifies that blobs exist.
func TestCheckFastMode(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{FastCheck: true}))
}

// TestCheckCachedRerun runs Check twice on the same snapshot so that the
// second run hits the cached-status fast paths in Check / checkEntry / etc.
func TestCheckCachedRerun(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
}

// TestCheckMissingPath verifies that asking Check to walk a path that does
// not exist returns an error.
func TestCheckMissingPath(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Check("/does/not/exist", &snapshot.CheckOptions{})
	require.Error(t, err)
}
