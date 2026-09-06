package filestore

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/errgroup"
)

// order 50, string keys and MAC values mirror a real backup index;
// building benchTrees of them concurrently mirrors
// snapshot.Builder.makeBackupIndexes.
const (
	benchOrder   = 50
	benchTrees   = 6
	benchPerTree = 80_000
)

func benchKey(tree, i int) string {
	return fmt.Sprintf("/some/realistic/looking/path/%03d/dir-%05d/file-%05d.dat", tree, i/100, i)
}

func benchMAC(i int) objects.MAC {
	var m objects.MAC
	binary.LittleEndian.PutUint64(m[:8], uint64(i))
	return m
}

// dirSize sums the size of every file directly inside dir (includes
// SQLite's -wal/-shm files).
func dirSize(tb testing.TB, dir string) int64 {
	tb.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		tb.Fatal(err)
	}
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
	}
	return total
}

// buildFileStoreTrees and buildSQLiteTrees build benchTrees trees
// concurrently and return them open, so callers can measure on-disk
// size before Close deletes everything.
func buildFileStoreTrees(dir string) ([]*btree.BTree[string, int, objects.MAC], error) {
	trees := make([]*btree.BTree[string, int, objects.MAC], benchTrees)

	g := errgroup.Group{}
	for t := 0; t < benchTrees; t++ {
		t := t
		g.Go(func() error {
			store, err := New[string, objects.MAC](dir, fmt.Sprintf("tree-%d", t))
			if err != nil {
				return err
			}
			tree, err := btree.New[string, int, objects.MAC](store, strings.Compare, benchOrder)
			if err != nil {
				return err
			}
			for i := 0; i < benchPerTree; i++ {
				if err := tree.Insert(benchKey(t, i), benchMAC(i)); err != nil {
					return err
				}
			}
			trees[t] = tree
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return trees, nil
}

func buildSQLiteTrees(dir string) ([]*btree.BTree[string, int, objects.MAC], error) {
	trees := make([]*btree.BTree[string, int, objects.MAC], benchTrees)

	g := errgroup.Group{}
	for t := 0; t < benchTrees; t++ {
		t := t
		g.Go(func() error {
			store, err := caching.NewSQLiteDBStore[string, objects.MAC](dir, fmt.Sprintf("tree-%d.db", t))
			if err != nil {
				return err
			}
			tree, err := btree.New[string, int, objects.MAC](store, strings.Compare, benchOrder)
			if err != nil {
				return err
			}
			for i := 0; i < benchPerTree; i++ {
				if err := tree.Insert(benchKey(t, i), benchMAC(i)); err != nil {
					return err
				}
			}
			trees[t] = tree
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return trees, nil
}

// runConcurrentBuild times the build only: disk accounting and Close
// run with the timer stopped, so they land in disk-B/op rather than
// ns/op.
func runConcurrentBuild(b *testing.B, build func(dir string) ([]*btree.BTree[string, int, objects.MAC], error)) {
	b.Helper()

	var totalDiskBytes int64

	for i := 0; i < b.N; i++ {
		dir := b.TempDir()

		trees, err := build(dir)
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		totalDiskBytes += dirSize(b, dir)
		for _, tree := range trees {
			if err := tree.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.StartTimer()
	}

	b.ReportMetric(float64(totalDiskBytes)/float64(b.N), "disk-B/op")
}

func BenchmarkConcurrentBuild_FileStore(b *testing.B) {
	runConcurrentBuild(b, buildFileStoreTrees)
}

func BenchmarkConcurrentBuild_SQLite(b *testing.B) {
	runConcurrentBuild(b, buildSQLiteTrees)
}

// TestConcurrentBuildDiskUsage reports wall time and on-disk footprint
// for one build per backend, sampled before Close deletes everything.
func TestConcurrentBuildDiskUsage(t *testing.T) {
	measure := func(name string, build func(dir string) ([]*btree.BTree[string, int, objects.MAC], error)) {
		dir := t.TempDir()

		start := time.Now()
		trees, err := build(dir)
		if err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(start)

		bytes := dirSize(t, dir)
		for _, tree := range trees {
			if err := tree.Close(); err != nil {
				t.Fatal(err)
			}
		}

		t.Logf("%s: %d trees x %d entries in %s, %d bytes on disk (%.1f bytes/entry)",
			name, benchTrees, benchPerTree, elapsed, bytes, float64(bytes)/float64(benchTrees*benchPerTree))
	}

	measure("FileStore", buildFileStoreTrees)
	measure("SQLite", buildSQLiteTrees)
}
