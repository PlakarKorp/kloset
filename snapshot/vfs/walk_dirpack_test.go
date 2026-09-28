package vfs_test

import (
	"context"
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

// window sizes: default, single-dir windows, and windows straddling dirs
var walkWindows = []string{"", "1", "2"}

func TestWalkDirpackMatchesWalkDir(t *testing.T) {
	for _, window := range walkWindows {
		t.Run("window="+window, func(t *testing.T) {
			if window != "" {
				t.Setenv("PLAKAR_DIRPACK_WALK_WINDOW", window)
			}

			snap := generateWalkSnapshot(t)
			defer snap.Close()

			fs, err := snap.Filesystem()
			require.NoError(t, err)

			dirpackWalk := func(root string, fn vfs.WalkDirFunc) error {
				return fs.WalkDirpack(context.Background(), root, fn)
			}

			viaWalkDir := collectWalk(t, fs.WalkDir, "/")
			viaDirpack := collectWalk(t, dirpackWalk, "/")

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
		})
	}
}

func TestWalkDirpackSubtree(t *testing.T) {
	for _, window := range walkWindows {
		t.Run("window="+window, func(t *testing.T) {
			if window != "" {
				t.Setenv("PLAKAR_DIRPACK_WALK_WINDOW", window)
			}

			snap := generateWalkSnapshot(t)
			defer snap.Close()

			fs, err := snap.Filesystem()
			require.NoError(t, err)

			dirpackWalk := func(root string, fn vfs.WalkDirFunc) error {
				return fs.WalkDirpack(context.Background(), root, fn)
			}

			// "/usr.bak" sorts inside the scan range of "/usr" but is not below it
			paths := collectWalk(t, dirpackWalk, "/usr")
			require.ElementsMatch(t, []string{
				"/usr", "/usr/bin", "/usr/bin/ls", "/usr/lib", "/usr/lib/libc.so",
			}, paths)

			// single file root
			paths = collectWalk(t, dirpackWalk, "/etc/passwd")
			require.Equal(t, []string{"/etc/passwd"}, paths)
		})
	}
}

func TestWalkDirpackSkipDir(t *testing.T) {
	for _, window := range walkWindows {
		t.Run("window="+window, func(t *testing.T) {
			if window != "" {
				t.Setenv("PLAKAR_DIRPACK_WALK_WINDOW", window)
			}

			snap := generateWalkSnapshot(t)
			defer snap.Close()

			fs, err := snap.Filesystem()
			require.NoError(t, err)

			var paths []string
			err = fs.WalkDirpack(context.Background(), "/", func(p string, e *vfs.Entry, err error) error {
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
		})
	}
}

func TestWalkDirpackCancel(t *testing.T) {
	snap := generateWalkSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = fs.WalkDirpack(ctx, "/", func(p string, e *vfs.Entry, err error) error {
		return err
	})
	require.ErrorIs(t, err, context.Canceled)
}
