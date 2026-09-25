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

type indexTree struct {
	snap     *snapshot.Snapshot
	dirs     []string
	children map[string][]string
}

// newIndexTree backs up a tree where the index order and the depth-first
// order disagree ("/a-b" and "/a.c" sort before "/a/b"), with more
// directories than the walk lists ahead. Every directory holds a file "f".
func newIndexTree(t *testing.T) *indexTree {
	t.Helper()

	dirs := []string{"/", "/a", "/a/b", "/a/b/c", "/a-b", "/a-b/x", "/a.c", "/wide", "/z"}
	for i := range 300 {
		d := fmt.Sprintf("/wide/d%03d", i)
		dirs = append(dirs, d, d+"/s")
	}

	tree := &indexTree{dirs: dirs, children: map[string][]string{}}
	for _, d := range dirs {
		if d != "/" {
			tree.children[path.Dir(d)] = append(tree.children[path.Dir(d)], d)
		}
		tree.children[d] = append(tree.children[d], path.Join(d, "f"))
	}
	for _, c := range tree.children {
		slices.Sort(c)
	}

	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	tree.snap = ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
		for _, d := range dirs {
			ch <- connectors.NewRecord(d, "", objects.FileInfo{
				Lname: path.Base(d),
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
		}
		for _, d := range dirs {
			f := path.Join(d, "f")
			ch <- connectors.NewRecord(f, "", objects.FileInfo{
				Lname: "f",
				Lmode: 0644,
				Lsize: int64(len(f)),
			}, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(f)), nil
			})
		}
	}))
	t.Cleanup(func() { tree.snap.Close() })
	return tree
}

func isBelow(root, p string) bool {
	return root == "/" || p == root || strings.HasPrefix(p, root+"/")
}

// indexOrder is root, then the children of every directory below it, a
// directory at a time in lexicographic order of the directories.
func (tree *indexTree) indexOrder(root string) []string {
	dirs := slices.Clone(tree.dirs)
	slices.Sort(dirs)

	order := []string{root}
	for _, d := range dirs {
		if isBelow(root, d) {
			order = append(order, tree.children[d]...)
		}
	}
	return order
}

func (tree *indexTree) walk(t *testing.T, root string, fn func(string, *vfs.Entry) error) ([]string, error) {
	t.Helper()
	fs, err := tree.snap.Filesystem()
	require.NoError(t, err)

	var got []string
	err = fs.WalkIndexOrder(root, func(p string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		got = append(got, p)
		if fn != nil {
			return fn(p, e)
		}
		return nil
	})
	return got, err
}

func TestWalkIndexOrder(t *testing.T) {
	tree := newIndexTree(t)

	got, err := tree.walk(t, "/", nil)
	require.NoError(t, err)
	require.Equal(t, tree.indexOrder("/"), got)

	seen := map[string]bool{"/": true}
	for _, p := range got[1:] {
		require.True(t, seen[path.Dir(p)], "%s visited before its parent", p)
		seen[p] = true
	}

	fs, err := tree.snap.Filesystem()
	require.NoError(t, err)
	var depthFirst []string
	require.NoError(t, fs.WalkDir("/", func(p string, e *vfs.Entry, err error) error {
		depthFirst = append(depthFirst, p)
		return nil
	}))
	require.ElementsMatch(t, depthFirst, got)
}

func TestWalkIndexOrderFromSubdirectory(t *testing.T) {
	tree := newIndexTree(t)

	got, err := tree.walk(t, "/a", nil)
	require.NoError(t, err)
	require.Equal(t, tree.indexOrder("/a"), got)
	require.NotContains(t, got, "/a-b/f")
}

func TestWalkIndexOrderSkipDir(t *testing.T) {
	tree := newIndexTree(t)

	got, err := tree.walk(t, "/", func(p string, e *vfs.Entry) error {
		if p == "/wide" || p == "/a/f" {
			return iofs.SkipDir
		}
		return nil
	})
	require.NoError(t, err)

	var want []string
	for _, p := range tree.indexOrder("/") {
		if !strings.HasPrefix(p, "/wide/") {
			want = append(want, p)
		}
	}
	// SkipDir on a file skips nothing: /a/b and /z are still walked.
	require.Equal(t, want, got)
}

func TestWalkIndexOrderStops(t *testing.T) {
	tree := newIndexTree(t)
	want := tree.indexOrder("/")
	stopAt := slices.Index(want, "/wide/d010/f")

	stop := errors.New("stop")
	got, err := tree.walk(t, "/", func(p string, e *vfs.Entry) error {
		if p == "/wide/d010/f" {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, want[:stopAt+1], got)

	got, err = tree.walk(t, "/", func(p string, e *vfs.Entry) error {
		if p == "/wide/d010/f" {
			return iofs.SkipAll
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want[:stopAt+1], got)
}

func TestWalkIndexOrderFile(t *testing.T) {
	tree := newIndexTree(t)

	got, err := tree.walk(t, "/a/b/f", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"/a/b/f"}, got)
}
