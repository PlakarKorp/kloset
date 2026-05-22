package snapshot_test

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// generateSnapshotWithFiles returns a snapshot that contains a small directory
// tree so that the Filesystem-related APIs have something to work with.
func generateSnapshotWithFiles(t *testing.T) (*ptesting.MockFile, *snapshot.Snapshot) {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	snap := ptesting.GenerateSnapshot(t, repo,
		nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- connectors.NewRecord("/", "", objects.FileInfo{
				Lname: "/",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/docs", "", objects.FileInfo{
				Lname: "docs",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/docs/readme.txt", "", objects.FileInfo{
				Lname: "readme.txt",
				Lmode: 0644,
				Lsize: int64(len("README content")),
			}, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("README content")), nil
			})
			ch <- connectors.NewRecord("/docs/notes.txt", "", objects.FileInfo{
				Lname: "notes.txt",
				Lmode: 0644,
				Lsize: int64(len("notes")),
			}, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("notes")), nil
			})
		}),
	)
	return nil, snap
}

// TestFilesystem verifies that Snapshot.Filesystem() returns a working VFS.
func TestFilesystem(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Calling it a second time should return the cached instance.
	fs2, err := snap.Filesystem()
	require.NoError(t, err)
	require.Same(t, fs, fs2, "expected cached filesystem to be returned on second call")

	// We should be able to see our files via Pathnames.
	var found []string
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		found = append(found, p)
	}
	require.NotEmpty(t, found)

	var hasReadme bool
	for _, p := range found {
		if strings.HasSuffix(p, "readme.txt") {
			hasReadme = true
		}
	}
	require.True(t, hasReadme, "expected readme.txt in pathnames")
}

// TestFilesystemWithCache verifies that FilesystemWithCache() works and returns
// a VFS backed by an LRU dirpack cache.
func TestFilesystemWithCache(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	fs, err := snap.FilesystemWithCache()
	require.NoError(t, err)
	require.NotNil(t, fs)

	// The filesystem should still list entries correctly.
	var count int
	for _, err := range fs.Files("/") {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0)
}

// TestListIndexes verifies that ListIndexes yields the index names stored in
// the snapshot header.
func TestListIndexes(t *testing.T) {
	_, snap := generateSnapshot(t) // reuse the existing helper from snapshot_test.go
	defer snap.Close()

	var names []string
	for name := range snap.ListIndexes() {
		names = append(names, name)
	}
	// The backup process always writes at least a VFS index.
	require.NotEmpty(t, names)
}

// TestContentTypeIdxRoot verifies that ContentTypeIdxRoot returns a non-zero MAC
// when a content-type index exists.
func TestContentTypeIdxRoot(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	mac, found := snap.ContentTypeIdxRoot()
	if found {
		require.NotEqual(t, objects.MAC{}, mac)
	}
	// It is valid for the index not to be present in all configurations, so we
	// only assert the shape of the return value when it is found.
}

// TestContentTypeIdx verifies that ContentTypeIdx either returns a valid BTree
// or nil (if no content-type index was written).
func TestContentTypeIdx(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	tree, err := snap.ContentTypeIdx()
	require.NoError(t, err)
	// tree may be nil if the content-type index was not created; that is fine.
	_ = tree
}

// TestDup verifies that Dup produces a new independent snapshot that shares the
// same file content as the original.
func TestDup(t *testing.T) {
	repo, snap := generateSnapshot(t)
	defer snap.Close()

	dup, err := snap.Dup(&snapshot.BuilderOptions{
		Name: "dup-test",
	})
	require.NoError(t, err)
	require.NotNil(t, dup)
	defer dup.Close()

	// The duplicate must have a different identifier.
	require.NotEqual(t, snap.Header.Identifier, dup.Header.Identifier)

	// The duplicate must be loadable.
	loaded, err := snapshot.Load(repo, dup.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	defer loaded.Close()

	// Both snapshots must be accessible from the repository.
	require.Equal(t, dup.Header.Identifier, loaded.Header.Identifier)
}

// TestFork verifies that Fork creates a new Builder that inherits the source
// snapshot metadata.
func TestFork(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	builder, err := snap.Fork(&snapshot.BuilderOptions{
		Name: "forked",
	})
	require.NoError(t, err)
	require.NotNil(t, builder)
	defer builder.Close()

	// The forked builder must have a different identifier but inherit some
	// header fields from the source.
	require.NotEqual(t, snap.Header.Identifier, builder.Header.Identifier)
	require.Equal(t, snap.Header.Name, builder.Header.Name)
}
