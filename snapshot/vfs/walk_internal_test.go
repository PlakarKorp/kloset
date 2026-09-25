package vfs

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestWalkPrefetcher runs the feeder over dirs with a fake worker that
// completes every listing, and returns the prefetcher and a function waiting
// for the feeder to finish and returning the directories it prefetched.
func newTestWalkPrefetcher(t *testing.T, window int, dirs []string) (*walkPrefetcher, func() []string) {
	t.Helper()

	p := &walkPrefetcher{
		slots:   make(chan struct{}, window),
		jobs:    make(chan walkJob),
		quit:    make(chan struct{}),
		pending: make(map[string]*dirListing),
		listed:  make(map[string]struct{}),
	}

	var fetched []string
	var worker sync.WaitGroup
	worker.Go(func() {
		for job := range p.jobs {
			fetched = append(fetched, job.dir)
			close(job.listing.done)
		}
	})

	p.wg.Add(1)
	go p.feed(newSliceCursor(dirs), "/")

	return p, func() []string {
		p.wg.Wait()
		worker.Wait()
		return fetched
	}
}

// awaitPending waits for the feeder to have prefetched dirs.
func awaitPending(t *testing.T, p *walkPrefetcher, dirs ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, dir := range dirs {
			if _, ok := p.pending[dir]; !ok {
				return false
			}
		}
		return true
	}, 2*time.Second, time.Millisecond, "%v not prefetched", dirs)
}

func takeWhenPending(t *testing.T, p *walkPrefetcher, dir string) {
	t.Helper()
	awaitPending(t, p, dir)
	_, ok := p.take(dir)
	require.True(t, ok, dir)
}

// The walk reaches /a/b before the index does ("/a-b" sorts first): once
// listed on demand, the feeder must not prefetch it, or it would hold a slot
// of the window for the rest of the walk.
func TestWalkPrefetchSkipsDirectoriesListedOnDemand(t *testing.T) {
	p, wait := newTestWalkPrefetcher(t, 2, []string{"/", "/a", "/a-b", "/a/b", "/c"})

	takeWhenPending(t, p, "/")
	// "/a" and "/a-b" fill the window, "/a/b" cannot be prefetched yet.
	awaitPending(t, p, "/a", "/a-b")
	_, ok := p.take("/a/b")
	require.False(t, ok)
	for _, dir := range []string{"/a", "/a-b", "/c"} {
		takeWhenPending(t, p, dir)
	}

	require.Equal(t, []string{"/", "/a", "/a-b", "/c"}, wait())
	require.Empty(t, p.slots, "window not released")
	require.Empty(t, p.pending)
}

// Skipping a subtree releases what was prefetched below it, and the feeder
// must not prefetch the rest of it afterwards.
func TestWalkPrefetchSkipsSkippedSubtrees(t *testing.T) {
	p, wait := newTestWalkPrefetcher(t, 2, []string{"/", "/s", "/s/x", "/s/y", "/t"})

	takeWhenPending(t, p, "/")
	awaitPending(t, p, "/s", "/s/x")
	p.skip("/s")
	takeWhenPending(t, p, "/t")

	require.Equal(t, []string{"/", "/s", "/s/x", "/t"}, wait())
	require.Empty(t, p.slots, "window not released")
	require.Empty(t, p.pending)
}

func TestIsSelfOrBelow(t *testing.T) {
	require.True(t, isSelfOrBelow("/", "/"))
	require.True(t, isSelfOrBelow("/", "/a"))
	require.True(t, isSelfOrBelow("/a", "/a"))
	require.True(t, isSelfOrBelow("/a", "/a/b"))
	require.False(t, isSelfOrBelow("/a", "/a-b"))
	require.False(t, isSelfOrBelow("/a", "/ab"))
	require.False(t, isSelfOrBelow("/a/b", "/a"))
}
