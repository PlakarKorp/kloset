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
			// same content as /usr/bin/ls: both share one object
			ch <- file("/usr.bak/ls", "ls")
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

func dirpackWalk(fs *vfs.Filesystem, opts *vfs.WalkDirpackOpts) func(string, vfs.WalkDirFunc) error {
	return func(root string, fn vfs.WalkDirFunc) error {
		return fs.WalkDirpack(context.Background(), root, opts, fn)
	}
}

var walkDirpackCases = []struct {
	name      string
	opts      *vfs.WalkDirpackOpts
	batchSize int64
}{
	{name: "default"},
	{name: "window=1", opts: &vfs.WalkDirpackOpts{Window: 1}},
	{name: "window=2", opts: &vfs.WalkDirpackOpts{Window: 2}},
	{name: "resolve", opts: &vfs.WalkDirpackOpts{ResolveObjects: true}},
	// every non-empty directory is above the budget and streamed
	{name: "stream", batchSize: 1},
	// directories straddle several batches
	{name: "batches", opts: &vfs.WalkDirpackOpts{ResolveObjects: true}, batchSize: 256},
}

func TestWalkDirpackMatchesWalkDir(t *testing.T) {
	for _, tc := range walkDirpackCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.batchSize != 0 {
				vfs.SetDirpackBatchSize(t, tc.batchSize)
			}

			snap := generateWalkSnapshot(t)
			defer snap.Close()

			fs, err := snap.Filesystem()
			require.NoError(t, err)

			viaWalkDir := collectWalk(t, fs.WalkDir, "/")
			viaDirpack := collectWalk(t, dirpackWalk(fs, tc.opts), "/")

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
	for _, tc := range walkDirpackCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.batchSize != 0 {
				vfs.SetDirpackBatchSize(t, tc.batchSize)
			}

			snap := generateWalkSnapshot(t)
			defer snap.Close()

			fs, err := snap.Filesystem()
			require.NoError(t, err)

			walk := dirpackWalk(fs, tc.opts)

			// "/usr.bak" sorts inside the scan range of "/usr" but is not below it
			paths := collectWalk(t, walk, "/usr")
			require.ElementsMatch(t, []string{
				"/usr", "/usr/bin", "/usr/bin/ls", "/usr/lib", "/usr/lib/libc.so",
			}, paths)

			// single file root
			paths = collectWalk(t, walk, "/etc/passwd")
			require.Equal(t, []string{"/etc/passwd"}, paths)
		})
	}
}

func TestWalkDirpackSkipDir(t *testing.T) {
	for _, tc := range walkDirpackCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.batchSize != 0 {
				vfs.SetDirpackBatchSize(t, tc.batchSize)
			}

			snap := generateWalkSnapshot(t)
			defer snap.Close()

			fs, err := snap.Filesystem()
			require.NoError(t, err)

			var paths []string
			err = dirpackWalk(fs, tc.opts)("/", func(p string, e *vfs.Entry, err error) error {
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

func TestWalkDirpackResolveObjects(t *testing.T) {
	snap := generateWalkSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	contents := make(map[string]string)
	err = dirpackWalk(fs, &vfs.WalkDirpackOpts{ResolveObjects: true})("/", func(p string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		if !e.FileInfo.Mode().IsRegular() {
			return nil
		}
		require.NotNil(t, e.ResolvedObject, "%s not resolved", p)

		f, err := e.Open(fs)
		require.NoError(t, err)
		defer f.Close()

		data, err := io.ReadAll(f)
		require.NoError(t, err)
		contents[p] = string(data)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, "ls", contents["/usr/bin/ls"])
	require.Equal(t, "ls", contents["/usr.bak/ls"])
	require.Equal(t, "root", contents["/etc/passwd"])
}

func TestWalkDirpackCancel(t *testing.T) {
	snap := generateWalkSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = fs.WalkDirpack(ctx, "/", nil, func(p string, e *vfs.Entry, err error) error {
		return err
	})
	require.ErrorIs(t, err, context.Canceled)
}
