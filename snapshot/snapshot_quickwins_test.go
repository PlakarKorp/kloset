package snapshot_test

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestVerifyMissingSignature drives Snapshot.Verify past the uuid.Nil early
// return: with a non-nil identity identifier but no signature blob stored,
// GetBlobBytes must fail and Verify must surface that error.
func TestVerifyMissingSignature(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	// Attach a non-nil identity so Verify proceeds to fetch the signature
	// blob, which does not exist for this unsigned snapshot.
	snap.Header.Identity.Identifier = uuid.New()

	ok, err := snap.Verify()
	require.Error(t, err)
	require.False(t, ok)
}

// TestFilesystemWithCacheCached calls FilesystemWithCache twice; the second
// call must hit the already-resolved early-return branch and return the same
// instance.
func TestFilesystemWithCacheCached(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	fs1, err := snap.FilesystemWithCache()
	require.NoError(t, err)
	require.NotNil(t, fs1)

	fs2, err := snap.FilesystemWithCache()
	require.NoError(t, err)
	require.Same(t, fs1, fs2, "second call should return the cached filesystem")
}

// TestFilesystemCachedSameInstance does the same for Filesystem(): the second
// call returns the memoized instance.
func TestFilesystemCachedSameInstance(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	fs1, err := snap.Filesystem()
	require.NoError(t, err)
	require.NotNil(t, fs1)

	fs2, err := snap.Filesystem()
	require.NoError(t, err)
	require.Same(t, fs1, fs2)
}

// TestSearchMimesWithNameFilter exercises the NameFilter branch *inside*
// visitmimes (recursive + mimes + a name filter), which the plain mime test
// does not reach.
func TestSearchMimesWithNameFilter(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{
		Recursive:  true,
		Mimes:      []string{"text/plain"},
		NameFilter: "*.txt",
	}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	for _, err := range it {
		require.NoError(t, err)
	}
}

// TestSearchMimesWithPrefixFilter drives the prefix-mismatch `continue` inside
// visitmimes: a prefix that excludes the matched entries means the iterator
// walks the index but yields nothing matching.
func TestSearchMimesWithPrefixFilter(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{
		Recursive: true,
		Mimes:     []string{"text/plain"},
		Prefix:    "/nonexistent",
	}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	count := 0
	for _, err := range it {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 0, count, "no entry should match a prefix outside the tree")
}

// TestSearchMimesExactNameFilter hits the exact-match (path.Base == NameFilter)
// arm of the NameFilter block inside visitmimes.
func TestSearchMimesExactNameFilter(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{
		Recursive:  true,
		Mimes:      []string{"text/plain"},
		NameFilter: "readme.txt",
	}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	for _, err := range it {
		require.NoError(t, err)
	}
}
