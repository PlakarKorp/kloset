package snapshot_test

import (
	"context"
	"io"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// uncleanRootSnapshot rebuilds the layout of snapshots taken before
// NewSource cleaned the importer root: the s3 importer reported "/prefix/"
// for a location ending in a slash, and emits files only.
func uncleanRootSnapshot(t *testing.T, keys ...string) *snapshot.Snapshot {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{})
	require.NoError(t, err)

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{}, "mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).SetGenerator(func(ch chan<- *connectors.Record) {
		for _, key := range keys {
			fi := objects.FileInfo{Lname: path.Base(key), Lsize: int64(len(key)), Lmode: 0o700}
			ch <- connectors.NewRecord(key, "", fi, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(key)), nil
			})
		}
	})

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)
	snapshot.SetSourceRoot(src, "/prefix/")

	require.NoError(t, builder.Backup(src))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, repo.RebuildState())

	snap, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)
	require.Equal(t, "/prefix/", snap.Header.GetSource(0).Importer.Directory)

	checkCache, err := repo.AppContext().GetCache().Check()
	require.NoError(t, err)
	snap.SetCheckCache(checkCache)

	return snap
}

func childNames(t *testing.T, fs *vfs.Filesystem, dir string) []string {
	entry, err := fs.GetEntry(dir)
	require.NoError(t, err)

	dents, err := entry.Getdents(fs)
	require.NoError(t, err)

	var names []string
	for child, err := range dents {
		require.NoError(t, err)
		names = append(names, child.Name())
	}
	slices.Sort(names)
	return names
}

func walkedPaths(t *testing.T, fs *vfs.Filesystem, root string) []string {
	var paths []string
	err := fs.WalkDirpack(context.Background(), root, nil, func(p string, _ *vfs.Entry, err error) error {
		require.NoError(t, err)
		paths = append(paths, p)
		return nil
	})
	require.NoError(t, err)
	slices.Sort(paths)
	return paths
}

// indexPaths lists the snapshot through its vfs index rather than dirpacks.
func indexPaths(t *testing.T, fs *vfs.Filesystem) (pathnames, files []string) {
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		pathnames = append(pathnames, p)
	}
	slices.Sort(pathnames)

	for e, err := range fs.Files("/") {
		require.NoError(t, err)
		files = append(files, e.Path())
	}
	slices.Sort(files)

	return pathnames, files
}

func readDirNames(t *testing.T, fs *vfs.Filesystem, dir string) []string {
	dents, err := fs.ReadDir(dir)
	require.NoError(t, err)

	var names []string
	for _, d := range dents {
		names = append(names, d.Name())
	}
	slices.Sort(names)
	return names
}

func TestUncleanRootPhantom(t *testing.T) {
	snap := uncleanRootSnapshot(t, "/prefix/a.txt", "/prefix/sub/b.txt")

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	require.Equal(t, []string{"a.txt", "sub"}, childNames(t, fs, "/prefix"))
	require.Equal(t, []string{"/prefix", "/prefix/a.txt", "/prefix/sub", "/prefix/sub/b.txt"},
		walkedPaths(t, fs, "/prefix"))
	require.Equal(t, []string{"a.txt", "sub"}, readDirNames(t, fs, "/prefix"))

	pathnames, files := indexPaths(t, fs)
	all := []string{"/", "/prefix", "/prefix/a.txt", "/prefix/sub", "/prefix/sub/b.txt"}
	require.Equal(t, all, pathnames)
	require.Equal(t, all, files)

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
	require.NoError(t, snap.Check(snap.Header.GetSource(0).Importer.Directory, &snapshot.CheckOptions{}))
}

// An importer reporting an unclean root now gets it cleaned by NewSource: the
// backup carries no phantom, including next to a child named like the root.
func TestUncleanRootCleanedOnBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{})
	require.NoError(t, err)

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{}, "mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).FakeRoot = "/foo/bar/"
	imp.(*ptesting.MockImporter).SetGenerator(func(ch chan<- *connectors.Record) {
		for _, key := range []string{"/foo/bar/bar", "/foo/bar/baz"} {
			fi := objects.FileInfo{Lname: path.Base(key), Lsize: int64(len(key)), Lmode: 0o700}
			ch <- connectors.NewRecord(key, "", fi, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(key)), nil
			})
		}
	})

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)
	require.Equal(t, "/foo/bar", src.Root())

	require.NoError(t, builder.Backup(src))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, repo.RebuildState())

	snap, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)
	require.Equal(t, "/foo/bar", snap.Header.GetSource(0).Importer.Directory)

	checkCache, err := repo.AppContext().GetCache().Check()
	require.NoError(t, err)
	snap.SetCheckCache(checkCache)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	require.Equal(t, []string{"bar", "baz"}, childNames(t, fs, "/foo/bar"))
	require.Equal(t, []string{"/foo/bar", "/foo/bar/bar", "/foo/bar/baz"}, walkedPaths(t, fs, "/foo/bar"))

	pathnames, files := indexPaths(t, fs)
	all := []string{"/", "/foo", "/foo/bar", "/foo/bar/bar", "/foo/bar/baz"}
	require.Equal(t, all, pathnames)
	require.Equal(t, all, files)

	bar, err := fs.GetEntry("/foo/bar/bar")
	require.NoError(t, err)
	require.True(t, bar.Stat().Mode().IsRegular(), "the child keeps its own entry")

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
}

// A real directory named like the root is synthesized the same way as the
// phantom, down to the MAC: exactly one of the two must remain.
func TestUncleanRootSameNameDir(t *testing.T) {
	snap := uncleanRootSnapshot(t, "/prefix/a.txt", "/prefix/prefix/b.txt")

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	require.Equal(t, []string{"a.txt", "prefix"}, childNames(t, fs, "/prefix"))
	require.Equal(t, []string{"/prefix", "/prefix/a.txt", "/prefix/prefix", "/prefix/prefix/b.txt"},
		walkedPaths(t, fs, "/prefix"))
	require.Equal(t, []string{"a.txt", "prefix"}, readDirNames(t, fs, "/prefix"))

	pathnames, files := indexPaths(t, fs)
	all := []string{"/", "/prefix", "/prefix/a.txt", "/prefix/prefix", "/prefix/prefix/b.txt"}
	require.Equal(t, all, pathnames)
	require.Equal(t, all, files)

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
}
