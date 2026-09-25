package vfs_test

import (
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
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

	err = fs.WalkDir(filepath, func(path string, d *vfs.Entry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path == filepath {
			return nil
		}

		if n := d.Name(); n != "dummy.txt" && n != "big" {
			t.Error("unexpected file", n)
		}

		return nil
	})
	require.NoError(t, err)
}

// walkTree is a snapshot whose directory index (lexicographic) and walk order
// (depth-first) disagree around "a-b" and "a.c", with more directories than
// the walk lists ahead.
func walkTree(t *testing.T) (*snapshot.Snapshot, []string) {
	t.Helper()

	dirs := []string{"/", "/a", "/a/b", "/a/b/c", "/a-b", "/a-b/x", "/a.c", "/wide"}
	for i := range 300 {
		d := fmt.Sprintf("/wide/d%03d", i)
		dirs = append(dirs, d, d+"/s")
	}
	var files []string
	for _, d := range dirs {
		files = append(files, path.Join(d, "f"))
	}

	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
		for _, d := range dirs {
			ch <- connectors.NewRecord(d, "", objects.FileInfo{
				Lname: path.Base(d),
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
		}
		for _, f := range files {
			ch <- connectors.NewRecord(f, "", objects.FileInfo{
				Lname: path.Base(f),
				Lmode: 0644,
				Lsize: int64(len(f)),
			}, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(f)), nil
			})
		}
	}))
	t.Cleanup(func() { snap.Close() })

	// Depth-first, parents first, siblings by name.
	children := map[string][]string{}
	for _, p := range append(dirs[1:], files...) {
		children[path.Dir(p)] = append(children[path.Dir(p)], p)
	}
	var want []string
	var visit func(string)
	visit = func(p string) {
		want = append(want, p)
		c := children[p]
		slices.SortFunc(c, func(a, b string) int { return strings.Compare(path.Base(a), path.Base(b)) })
		for _, child := range c {
			visit(child)
		}
	}
	visit("/")
	return snap, want
}

func TestWalkDirVisitsDepthFirst(t *testing.T) {
	snap, want := walkTree(t)
	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var got []string
	err = fs.WalkDir("/", func(path string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		got = append(got, path)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestWalkDirFromSubdirectory(t *testing.T) {
	snap, want := walkTree(t)
	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var got []string
	err = fs.WalkDir("/a", func(path string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		got = append(got, path)
		return nil
	})
	require.NoError(t, err)

	var below []string
	for _, p := range want {
		if p == "/a" || strings.HasPrefix(p, "/a/") {
			below = append(below, p)
		}
	}
	require.Equal(t, below, got)
}

func TestWalkDirSkipsPrefetchedSubtree(t *testing.T) {
	snap, want := walkTree(t)
	fs, err := snap.Filesystem()
	require.NoError(t, err)

	skipped := func(p string) bool {
		return p == "/wide" || strings.HasPrefix(p, "/wide/") || strings.HasPrefix(p, "/a/b/")
	}

	var got []string
	err = fs.WalkDir("/", func(path string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		got = append(got, path)
		if path == "/wide" || path == "/a/b" {
			return iofs.SkipDir
		}
		return nil
	})
	require.NoError(t, err)

	var expect []string
	for _, p := range want {
		if !skipped(p) || p == "/wide" {
			expect = append(expect, p)
		}
	}
	require.Equal(t, expect, got)
}

func TestWalkDirStopsOnError(t *testing.T) {
	snap, want := walkTree(t)
	fs, err := snap.Filesystem()
	require.NoError(t, err)

	stop := errors.New("stop")
	var visited int
	err = fs.WalkDir("/", func(path string, e *vfs.Entry, err error) error {
		visited++
		if path == "/wide/d010" {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)

	// The walk must not have run on after the error.
	require.Equal(t, slices.Index(want, "/wide/d010")+1, visited)
}
