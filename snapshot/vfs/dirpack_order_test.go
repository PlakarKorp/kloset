package vfs_test

import (
	"fmt"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func listNames(t *testing.T, fs *vfs.Filesystem, dir string) []string {
	t.Helper()
	e, err := fs.GetEntry(dir)
	require.NoError(t, err)
	it, err := e.Getdents(fs)
	require.NoError(t, err)

	var names []string
	for child, err := range it {
		require.NoError(t, err)
		names = append(names, child.Name())
	}
	return names
}

func TestDirpackCachedListingOrder(t *testing.T) {
	const files = 64

	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
		ch <- dirRec("/")
		ch <- dirRec("/dir")
		for f := range files {
			p := fmt.Sprintf("/dir/file%02d", f)
			ch <- fileRec(p, "content of "+p)
		}
	}))
	defer func() { _ = base.Close() }()

	snap, err := snapshot.Load(repo, base.Header.Identifier)
	require.NoError(t, err)
	defer func() { _ = snap.Close() }()
	coldFS, err := snap.Filesystem()
	require.NoError(t, err)
	want := listNames(t, coldFS, "/dir")
	require.Len(t, want, files)

	cachedFS := freshCacheFS(t, repo, base.Header.Identifier)
	for range 3 {
		require.Equal(t, want, listNames(t, cachedFS, "/dir"))
	}
}
