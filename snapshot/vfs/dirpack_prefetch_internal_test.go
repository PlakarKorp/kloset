package vfs

import (
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

// sliceCursor is a fake btree iterator over a fixed list of directory keys. It
// mirrors forwardIter semantics: Next() advances first, then reports validity,
// so it starts positioned before the first element.
type sliceCursor struct {
	keys []string
	idx  int
}

func newSliceCursor(keys []string) *sliceCursor { return &sliceCursor{keys: keys, idx: -1} }

func (c *sliceCursor) Next() bool {
	c.idx++
	return c.idx < len(c.keys)
}

func (c *sliceCursor) Current() (string, objects.MAC) { return c.keys[c.idx], objects.MAC{} }

func (c *sliceCursor) Err() error { return nil }

func recvJobPath(t *testing.T, jobs <-chan prefetchJob) (string, bool) {
	t.Helper()
	select {
	case job := <-jobs:
		return job.path, true
	case <-time.After(2 * time.Second):
		return "", false
	}
}

// TestDirpackFeederDeliversAllDirsInOrder drives the feeder directly (no repo,
// no workers — the test drains the jobs channel itself) and asserts every
// directory is enqueued, in cursor order, as consume signals arrive. This pins
// the feeder's enqueue logic: a feeder that stops after the first directory
// (e.g. an inverted enqueue() return check) fails here.
func TestDirpackFeederDeliversAllDirsInOrder(t *testing.T) {
	dirs := []string{"/a", "/b", "/c", "/d", "/e"}

	p := &dirpackPrefetcher{
		window:   3,
		jobs:     make(chan prefetchJob, len(dirs)),
		consumed: make(chan struct{}, len(dirs)),
		quit:     make(chan struct{}),
		// seen is unused by feed(); these tests don't call onConsume.
	}
	// Pre-load one consume signal per directory: priming covers `window`, the
	// remaining directories are pulled in by these signals until the cursor is
	// exhausted. Extra signals past exhaustion are harmless no-ops.
	for range dirs {
		p.consumed <- struct{}{}
	}

	p.feederWg.Add(1)
	go p.feed(newSliceCursor(dirs))

	var got []string
	for range dirs {
		path, ok := recvJobPath(t, p.jobs)
		require.Truef(t, ok, "timed out after %d/%d jobs: %v", len(got), len(dirs), got)
		got = append(got, path)
	}

	close(p.quit)
	p.feederWg.Wait()

	require.Equal(t, dirs, got)
}

// TestDirpackFeederRespectsWindow checks the feeder primes exactly `window`
// directories up front, then waits for a consume signal before advancing — it
// never lets more than `window` directories be in flight unprompted.
func TestDirpackFeederRespectsWindow(t *testing.T) {
	dirs := []string{"/a", "/b", "/c", "/d", "/e"}
	const window = 3

	p := &dirpackPrefetcher{
		window:   window,
		jobs:     make(chan prefetchJob, len(dirs)),
		consumed: make(chan struct{}, len(dirs)),
		quit:     make(chan struct{}),
		// seen is unused by feed(); these tests don't call onConsume.
	}

	p.feederWg.Add(1)
	go p.feed(newSliceCursor(dirs))

	// The first `window` directories are primed without any consume signal.
	for i := 0; i < window; i++ {
		_, ok := recvJobPath(t, p.jobs)
		require.Truef(t, ok, "feeder did not prime %d directories (stalled at %d)", window, i)
	}

	// With no consume signal sent, the feeder must not enqueue a (window+1)th.
	select {
	case extra := <-p.jobs:
		t.Fatalf("feeder enqueued %q beyond the window without a consume signal", extra.path)
	case <-time.After(200 * time.Millisecond):
	}

	// A single consume signal releases exactly one more directory.
	p.consumed <- struct{}{}
	_, ok := recvJobPath(t, p.jobs)
	require.True(t, ok, "feeder did not advance after a consume signal")

	close(p.quit)
	p.feederWg.Wait()
}
