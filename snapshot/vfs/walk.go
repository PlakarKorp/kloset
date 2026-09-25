package vfs

import (
	"io/fs"
	"iter"
	"strings"
	"sync"

	"github.com/PlakarKorp/kloset/iterator"
	"github.com/PlakarKorp/kloset/objects"
)

// Listing a directory costs a round trip to the store, which dominates a walk
// over many small directories. The window bounds how many listings are held
// ahead of the walk, the workers how many are fetched at once. The window
// counts directories, not entries: keep it small enough that a run of huge
// directories still fits in memory.
const (
	walkPrefetchWindow  = 256
	walkPrefetchWorkers = 64
)

type WalkDirFunc func(path string, entry *Entry, err error) error

type dirListing struct {
	done    chan struct{}
	entries []*Entry
	err     error
}

type walkJob struct {
	dir     string
	mac     objects.MAC
	listing *dirListing
}

// walkPrefetcher lists directories ahead of the walk by scanning the dirpack
// index. The index is in lexicographic order and the walk in depth-first
// order; they only disagree around names sorting before '/' ("a-b" comes
// before "a/b"), where the walk falls back to listing on demand.
//
// Every prefetched listing holds a slot of the window until the walk takes it,
// so the feeder must not prefetch what the walk has already listed or will
// never enter: those would hold their slot until the end of the walk.
type walkPrefetcher struct {
	fsc   *Filesystem
	slots chan struct{}
	jobs  chan walkJob
	quit  chan struct{}
	wg    sync.WaitGroup

	mu      sync.Mutex
	pending map[string]*dirListing
	listed  map[string]struct{} // listed on demand, not yet reached by the feeder
	skipped []string
	fed     bool // the feeder is done, nothing left to keep out of it
}

func newWalkPrefetcher(fsc *Filesystem, root string) *walkPrefetcher {
	if fsc.dirpack == nil {
		return nil
	}

	cursor, err := fsc.dirpack.ScanFrom(root)
	if err != nil {
		// Not fatal, the walk lists every directory on demand.
		return nil
	}

	p := &walkPrefetcher{
		fsc:     fsc,
		slots:   make(chan struct{}, walkPrefetchWindow),
		jobs:    make(chan walkJob, walkPrefetchWorkers),
		quit:    make(chan struct{}),
		pending: make(map[string]*dirListing),
		listed:  make(map[string]struct{}),
	}

	p.wg.Add(1 + walkPrefetchWorkers)
	go p.feed(cursor, root)
	for range walkPrefetchWorkers {
		go p.worker()
	}
	return p
}

func (p *walkPrefetcher) close() {
	close(p.quit)
	p.wg.Wait()
}

func (p *walkPrefetcher) feed(cursor iterator.Iterator[string, objects.MAC], root string) {
	defer p.wg.Done()
	defer close(p.jobs)
	defer func() {
		p.mu.Lock()
		p.fed = true
		p.listed = nil
		p.mu.Unlock()
	}()

	for cursor.Next() {
		dir, mac := cursor.Current()
		if !isSelfOrBelow(root, dir) {
			return
		}

		select {
		case p.slots <- struct{}{}:
		case <-p.quit:
			return
		}

		p.mu.Lock()
		if p.passed(dir) {
			p.mu.Unlock()
			<-p.slots
			continue
		}
		// Nobody can be waiting on a listing that never reaches a worker:
		// quit is only closed once the walk is over.
		listing := &dirListing{done: make(chan struct{})}
		p.pending[dir] = listing
		p.mu.Unlock()

		select {
		case p.jobs <- walkJob{dir: dir, mac: mac, listing: listing}:
		case <-p.quit:
			return
		}
	}
}

func (p *walkPrefetcher) worker() {
	defer p.wg.Done()

	for job := range p.jobs {
		select {
		case <-p.quit:
			continue
		default:
		}

		it, err := p.fsc.readDirpack(job.dir, job.mac)
		if err == nil {
			for entry, e := range it {
				if e != nil {
					err = e
					break
				}
				job.listing.entries = append(job.listing.entries, entry)
			}
		}
		job.listing.err = err
		close(job.listing.done)
	}
}

// passed reports whether the walk is already past dir. Called with mu held.
func (p *walkPrefetcher) passed(dir string) bool {
	if _, ok := p.listed[dir]; ok {
		delete(p.listed, dir)
		return true
	}
	for _, root := range p.skipped {
		if isSelfOrBelow(root, dir) {
			return true
		}
	}
	return false
}

// take hands over the listing of dir if it was prefetched, otherwise records
// that the walk lists it on demand.
func (p *walkPrefetcher) take(dir string) (*dirListing, bool) {
	p.mu.Lock()
	listing, ok := p.pending[dir]
	if ok {
		delete(p.pending, dir)
	} else if !p.fed {
		p.listed[dir] = struct{}{}
	}
	p.mu.Unlock()

	if !ok {
		return nil, false
	}
	<-listing.done
	<-p.slots
	return listing, true
}

// skip releases the listings prefetched for a subtree the walk will not enter,
// so that they do not hold the window.
func (p *walkPrefetcher) skip(dir string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.skipped = append(p.skipped, dir)
	for pending := range p.pending {
		if isSelfOrBelow(dir, pending) {
			delete(p.pending, pending)
			<-p.slots
		}
	}
}

func isSelfOrBelow(root, dir string) bool {
	if root == "/" || dir == root {
		return strings.HasPrefix(dir, root)
	}
	return strings.HasPrefix(dir, root+"/")
}

func (fsc *Filesystem) getdents(entry *Entry, prefetch *walkPrefetcher) (iter.Seq2[*Entry, error], error) {
	if prefetch != nil {
		if listing, ok := prefetch.take(entry.Path()); ok {
			return func(yield func(*Entry, error) bool) {
				for _, e := range listing.entries {
					if !yield(e, nil) {
						return
					}
				}
				if listing.err != nil {
					yield(nil, listing.err)
				}
			}, nil
		}
	}
	return entry.Getdents(fsc)
}

func (fsc *Filesystem) walkdir(entry *Entry, fn WalkDirFunc, prefetch *walkPrefetcher) error {
	path := entry.Path()
	if err := fn(path, entry, nil); err != nil {
		return err
	}

	if !entry.FileInfo.Mode().IsDir() {
		return nil
	}

	children, err := fsc.getdents(entry, prefetch)
	if err != nil {
		return fn(path, nil, err)
	}

	for entry, err := range children {
		if err != nil {
			return fn(path, nil, err)
		}

		if err := fsc.walkdir(entry, fn, prefetch); err != nil {
			if err == fs.SkipDir {
				if prefetch != nil && entry.IsDir() {
					prefetch.skip(entry.Path())
				}
				continue
			}
			return err
		}
	}

	return nil
}

func (fsc *Filesystem) WalkDir(root string, fn WalkDirFunc) error {
	entry, err := fsc.GetEntry(root)
	if err != nil {
		return fn(root, nil, err)
	}

	var prefetch *walkPrefetcher
	if entry.IsDir() {
		prefetch = newWalkPrefetcher(fsc, entry.Path())
		if prefetch != nil {
			defer prefetch.close()
		}
	}

	if err = fsc.walkdir(entry, fn, prefetch); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}
