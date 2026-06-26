package vfs

import (
	"sync"

	"github.com/PlakarKorp/kloset/caching/lru"
	"github.com/PlakarKorp/kloset/iterator"
	"github.com/PlakarKorp/kloset/objects"
)

// dirpackPrefetcher warms fsc.dirpackCache ahead of the backup walk.
// The feeder keeps at most `window` directories in flight: it primes the
// window, then advances the cursor by one each time the walk reports a fresh
// directory consumed (via onConsume). Loads route through the same
// dirpackSF singleflight group the walk uses, so a prefetch and an on-demand
// load of the same directory never both hit the backend.
type dirpackPrefetcher struct {
	fsc     *Filesystem
	window  int
	workers int

	jobs     chan prefetchJob
	consumed chan struct{}
	quit     chan struct{}

	feederWg sync.WaitGroup
	workerWg sync.WaitGroup

	// seen dedups consume signals to one per directory. It is bounded (FIFO):
	// it only needs to remember directories that may still have in-flight walk
	// records, and the importer emits each directory contiguously and never
	// revisits, so the in-flight span is ~walk concurrency. Sizing it to the
	// look-ahead window leaves a large margin while keeping memory flat on huge
	// sources (hundreds of millions of directories). A rare premature eviction
	// only costs one duplicate consume signal (one extra dir prefetched).
	seenMu sync.Mutex
	seen   *lru.Cache[string, struct{}]
}

type prefetchJob struct {
	path string
	mac  objects.MAC
}

// window is the maximum number of directories kept in flight
func (fsc *Filesystem) StartDirpackPrefetch(window, workers int) {
	if fsc.dirpack == nil || fsc.dirpackCache == nil || fsc.prefetcher != nil {
		return
	}

	cursor, err := fsc.dirpack.ScanFrom("/")
	if err != nil {
		// Not a fatal error, we just run without prefetching!
		return
	}

	fsc.prefetcher = &dirpackPrefetcher{
		fsc:      fsc,
		window:   window,
		workers:  workers,
		jobs:     make(chan prefetchJob, workers),
		consumed: make(chan struct{}, window),
		quit:     make(chan struct{}),
		seen:     lru.New[string, struct{}](window, nil),
	}

	// The dirpack cache is plain FIFO: a prefetched-but-not-yet-consumed
	// entry must not be evicted before the walk reaches it, so the cache
	// must hold at least window+workers entries. The cache is empty at this
	// point (the walk has not started), so resizing it here is safe.
	if need := 2 * (window + workers); need > fsc.dirpackCacheSize {
		fsc.dirpackCacheSize = need
		fsc.dirpackCache = lru.New[string, map[string]*Entry](need, nil)
	}

	fsc.prefetcher.workerWg.Add(workers)
	for range workers {
		go fsc.prefetcher.worker()
	}

	fsc.prefetcher.feederWg.Add(1)
	go fsc.prefetcher.feed(cursor)
}

func (fsc *Filesystem) StopDirpackPrefetch() {
	if fsc.prefetcher == nil {
		return
	}

	close(fsc.prefetcher.quit)
	fsc.prefetcher.feederWg.Wait()
	close(fsc.prefetcher.jobs)
	fsc.prefetcher.workerWg.Wait()

	fsc.prefetcher = nil
}

func (p *dirpackPrefetcher) feed(cursor iterator.Iterator[string, objects.MAC]) {
	defer p.feederWg.Done()

	// enqueue advances the cursor by one directory and dispatches it to a
	// worker. Returns false when the cursor is exhausted/errored or we are
	// shutting down.
	enqueue := func() bool {
		if !cursor.Next() {
			return false
		}
		dir, mac := cursor.Current()
		select {
		case p.jobs <- prefetchJob{path: dir, mac: mac}:
			return true
		case <-p.quit:
			return false
		}
	}

	exhausted := false
	// Fill the cache first.
	for range p.window {
		if !enqueue() {
			exhausted = true
			break
		}
	}

	// Now as consummer advance the cursor replenish the cache (since it's the
	// fifo older one gets eviced).
	// Note even when exhausted we keep looping to drain the channel.
	for {
		select {
		case <-p.quit:
			return
		case <-p.consumed:
			if !exhausted && !enqueue() {
				exhausted = true
			}
		}
	}
}

func (p *dirpackPrefetcher) worker() {
	defer p.workerWg.Done()
	for job := range p.jobs {
		_, _, _ = p.fsc.dirpackSF.Do(job.path, func() (any, error) {
			if m, exists := p.fsc.dirpackCache.Get(job.path); exists {
				return m, nil
			}
			return p.fsc.loadDirpackMapByMAC(job.path, job.mac)
		})
	}
}

// onConsume reports that the walk has reached directory dir. The first report
// for a directory advances the prefetch window by one; subsequent reports for
// the same directory are ignored.
func (p *dirpackPrefetcher) onConsume(dir string) {
	p.seenMu.Lock()
	if _, ok := p.seen.Get(dir); ok {
		p.seenMu.Unlock()
		return
	}
	_ = p.seen.Put(dir, struct{}{})
	p.seenMu.Unlock()

	select {
	case p.consumed <- struct{}{}:
	case <-p.quit:
	}
}
