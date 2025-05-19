package vfs_test

import (
	"io"
	iofs "io/fs"
	"log"
	"slices"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func ppslice(s []string) {
	for _, x := range s {
		log.Println("->", x)
	}
}

func TestPathCmp(t *testing.T) {
	suite := []struct {
		input  []string
		expect []string
	}{
		{
			input:  []string{"/"},
			expect: []string{"/"},
		},
		{
			input:  []string{"c", "a", "b", "z"},
			expect: []string{"a", "b", "c", "z"},
		},
		{
			input:  []string{"c", "a", "b", "z", "z/a", "z/z", "z/a/b/c", "z/z/z", "z/a/z"},
			expect: []string{"a", "b", "c", "z", "z/a", "z/z", "z/a/z", "z/z/z", "z/a/b/c"},
		},
		{
			input:  []string{"/etc/", "/"},
			expect: []string{"/", "/etc/"},
		},
		{
			input:  []string{"/etc/zzz", "/etc/foo/bar", "/etc/foo"},
			expect: []string{"/etc/foo", "/etc/zzz", "/etc/foo/bar"},
		},
		{
			input:  []string{"/etc/zzz", "/etc/foo/bar", "/etc/foo", "/etc/aaa"},
			expect: []string{"/etc/aaa", "/etc/foo", "/etc/zzz", "/etc/foo/bar"},
		},
		{
			input:  []string{"/home/op", "/etc/zzz", "/etc/foo/bar", "/etc/foo", "/home/op/.kshrc"},
			expect: []string{"/etc/foo", "/etc/zzz", "/home/op", "/etc/foo/bar", "/home/op/.kshrc"},
		},
		{
			input: []string{
				"/",
				"/home",
				"/home/op",
				"/home/op/w",
				"/home/op/w/plakar",
				"/home/op/w/plakar/btree",
				"/home/op/w/plakar/btree/btree.go",
				"/home/op/w/plakar/storage",
				"/home/op/w/plakar/storage/backends",
				"/home/op/w/plakar/storage/backends/database",
				"/home/op/w/plakar/storage/backends/database/database.go",
				"/home/op/w/plakar/storage/backends/null",
				"/home/op/w/plakar/storage/backends/null/null.go",
				"/home/op/w/plakar/storage/backends/s3",
				"/home/op/w/plakar/storage/backends/s3/s3.go",
				"/home/op/w/plakar/snapshot",
				"/home/op/w/plakar/snapshot/backup.go",
				"/home/op/w/plakar/snapshot/exporter",
				"/home/op/w/plakar/snapshot/exporter/exporter.go",
				"/home/op/w/plakar/snapshot/exporter/fs",
				"/home/op/w/plakar/snapshot/exporter/fs/fs.go",
				"/home/op/w/plakar/snapshot/vfs",
				"/home/op/w/plakar/snapshot/vfs/vfs.go",
				"/home/op/w/plakar/snapshot/vfs/entry.go",
			},
			expect: []string{
				"/",
				"/home",
				"/home/op",
				"/home/op/w",
				"/home/op/w/plakar",
				"/home/op/w/plakar/btree",
				"/home/op/w/plakar/snapshot",
				"/home/op/w/plakar/storage",
				"/home/op/w/plakar/btree/btree.go",
				"/home/op/w/plakar/snapshot/backup.go",
				"/home/op/w/plakar/snapshot/exporter",
				"/home/op/w/plakar/snapshot/vfs",
				"/home/op/w/plakar/storage/backends",
				"/home/op/w/plakar/snapshot/exporter/exporter.go",
				"/home/op/w/plakar/snapshot/exporter/fs",
				"/home/op/w/plakar/snapshot/vfs/entry.go",
				"/home/op/w/plakar/snapshot/vfs/vfs.go",
				"/home/op/w/plakar/storage/backends/database",
				"/home/op/w/plakar/storage/backends/null",
				"/home/op/w/plakar/storage/backends/s3",
				"/home/op/w/plakar/snapshot/exporter/fs/fs.go",
				"/home/op/w/plakar/storage/backends/database/database.go",
				"/home/op/w/plakar/storage/backends/null/null.go",
				"/home/op/w/plakar/storage/backends/s3/s3.go",
			},
		},
	}

	for _, test := range suite {
		sorted := make([]string, len(test.input))
		copy(sorted, test.input)

		slices.SortFunc(sorted, vfs.PathCmp)
		if slices.Compare(test.expect, sorted) != 0 {
			t.Error("expected:")
			ppslice(test.expect)
			t.Error("got:")
			ppslice(sorted)
		}

		for _, path := range test.input {
			if _, found := slices.BinarySearchFunc(sorted, path, vfs.PathCmp); !found {
				t.Error("item not found by binary search:", path)
			}
		}
	}
}

func generateSnapshot(t *testing.T) (*repository.Repository, *snapshot.Snapshot) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockFile("subdir/dummy.txt", 0644, "hello"),
	})
	return repo, snap
}

func TestFiles(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	for entry, err := range fs.Files("/") {
		require.NoError(t, err)
		if !entry.Type().IsRegular() {
			continue
		}
		require.Contains(t, entry.Path(), "dummy.txt")
	}
}

func TestPathnames(t *testing.T) {
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
}

func TestOpen(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	f, err := fs.Open(filepath)
	require.NoError(t, err)
	defer f.Close()

	content, err := io.ReadAll(f)
	require.NoError(t, err)
	require.Equal(t, "hello", string(content))
}

func TestGetEntry(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	entry, err := fs.GetEntry(filepath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "dummy.txt", entry.Name())
}

func _TestReadDir(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entries, err := fs.ReadDir("/")
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))

	entry := entries[0]
	require.Equal(t, "tmp", entry.Name())
	require.True(t, entry.Type().IsDir())
	require.True(t, entry.IsDir())

	fileinfo, err := entry.Info()
	require.NoError(t, err)
	require.Implements(t, (*iofs.FileInfo)(nil), fileinfo)
}

func TestGetdents(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "subdir") {
			filepath = pathname
			break
		}
	}
	require.NotEmpty(t, filepath)

	entry, err := fs.GetEntry(filepath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.True(t, entry.IsDir())

	dents, err := entry.Getdents(fs)
	require.NoError(t, err)
	for d, err := range dents {
		require.NoError(t, err)
		require.Equal(t, "dummy.txt", d.Name())
	}
}

func TestChildren(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	iter, err := fs.Children("/subdir/")
	require.NoError(t, err)
	require.NotNil(t, iter)

	expectedChildren := []string{"/subdir/dummy.txt"}
	var childNames []string
	for child, err := range iter {
		require.NoError(t, err)
		require.NotNil(t, child)
		childNames = append(childNames, child.Path())
	}
	require.ElementsMatch(t, expectedChildren, childNames)
}

func TestFileMacs(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	iter, err := fs.FileMacs()
	require.NoError(t, err)
	require.NotNil(t, iter)

	macs := make(map[objects.MAC]struct{})
	for m, err := range iter {
		require.NoError(t, err)
		require.NotNil(t, m)
		macs[m] = struct{}{}
	}

	require.Equal(t, 3, len(macs))
}
