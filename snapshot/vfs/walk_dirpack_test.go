package vfs_test

import (
	"io"
	iofs "io/fs"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

// generateWalkSnapshot builds a tree exercising the lexical-order wrinkle:
// "/usr.bak" sorts between "/usr" and "/usr/..." because '.' < '/'.
func generateWalkSnapshot(t *testing.T) *snapshot.Snapshot {
	t.Helper()

	dir := func(p string) *connectors.Record {
		return connectors.NewRecord(p, "", objects.FileInfo{
			Lname: path.Base(p),
			Lmode: os.ModeDir | 0755,
		}, nil, nil)
	}
	file := func(p, content string) *connectors.Record {
		return connectors.NewRecord(p, "", objects.FileInfo{
			Lname: path.Base(p),
			Lmode: 0644,
		}, nil, func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(content)), nil
		})
	}

	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- dir("/")
			ch <- file("/README", "readme")
			ch <- dir("/etc")
			ch <- file("/etc/passwd", "root")
			ch <- dir("/etc/ssl")
			ch <- file("/etc/ssl/cert.pem", "cert")
			ch <- dir("/usr")
			ch <- dir("/usr/bin")
			ch <- file("/usr/bin/ls", "ls")
			ch <- dir("/usr/lib")
			ch <- file("/usr/lib/libc.so", "libc")
			ch <- dir("/usr.bak")
			ch <- file("/usr.bak/old", "old")
		}),
	)

	_, found := snap.DirPackRoot()
	require.True(t, found, "snapshot has no dirpack index; WalkDirpack would fall back to WalkDir")

	return snap
}

func collectWalk(t *testing.T, walk func(string, vfs.WalkDirFunc) error, root string) []string {
	t.Helper()
	var paths []string
	err := walk(root, func(p string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		paths = append(paths, p)
		return nil
	})
	require.NoError(t, err)
	return paths
}

func TestWalkDirpackMatchesWalkDir(t *testing.T) {
	snap := generateWalkSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	viaWalkDir := collectWalk(t, fs.WalkDir, "/")
	viaDirpack := collectWalk(t, fs.WalkDirpack, "/")

	require.ElementsMatch(t, viaWalkDir, viaDirpack)

	// parents must be seen before anything below them
	seen := make(map[string]struct{})
	for _, p := range viaDirpack {
		if p != "/" {
			_, ok := seen[path.Dir(p)]
			require.True(t, ok, "%s emitted before its parent", p)
		}
		seen[p] = struct{}{}
	}
}

func TestWalkDirpackSubtree(t *testing.T) {
	snap := generateWalkSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// "/usr.bak" sorts inside the scan range of "/usr" but is not below it
	paths := collectWalk(t, fs.WalkDirpack, "/usr")
	require.ElementsMatch(t, []string{
		"/usr", "/usr/bin", "/usr/bin/ls", "/usr/lib", "/usr/lib/libc.so",
	}, paths)

	// single file root
	paths = collectWalk(t, fs.WalkDirpack, "/etc/passwd")
	require.Equal(t, []string{"/etc/passwd"}, paths)
}

func TestWalkDirpackSkipDir(t *testing.T) {
	snap := generateWalkSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var paths []string
	err = fs.WalkDirpack("/", func(p string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		paths = append(paths, p)
		if p == "/usr" {
			return iofs.SkipDir
		}
		return nil
	})
	require.NoError(t, err)

	require.Contains(t, paths, "/usr")
	require.Contains(t, paths, "/usr.bak/old")
	for _, p := range paths {
		require.False(t, strings.HasPrefix(p, "/usr/"), "%s emitted below skipped /usr", p)
	}
}
