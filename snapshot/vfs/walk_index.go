package vfs

import (
	"errors"
	"fmt"
	"io/fs"
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
	indexWalkWindow  = 256
	indexWalkWorkers = 64
)

type indexListing struct {
	dir     string
	mac     objects.MAC
	done    chan struct{}
	entries []*Entry
	err     error
}

// WalkIndexOrder calls fn for root and every entry below it, like WalkDir, but
// visits directories in the order of the dirpack index rather than depth
// first: the entries of a directory are reported together when its turn
// comes in the index, so "/a-b" and everything below it come before "/a/b".
// A directory is still reported before anything below it.
//
// Following the index lets directories be listed ahead of the walk, which
// matters on a remote store. Snapshots without a dirpack index are walked
// with WalkDir.
//
// Returning fs.SkipDir for a directory skips everything below it, for a file
// it does nothing. If a directory cannot be listed, or its listing breaks
// half-way, fn is called with the error and nothing below that directory is
// visited.
func (fsc *Filesystem) WalkIndexOrder(root string, fn WalkDirFunc) error {
	entry, err := fsc.GetEntry(root)
	if err != nil {
		return fn(root, nil, err)
	}
	if fsc.dirpack == nil || !entry.IsDir() {
		return fsc.WalkDir(root, fn)
	}

	root = entry.Path()
	if err := fn(root, entry, nil); err != nil {
		if errors.Is(err, fs.SkipDir) || errors.Is(err, fs.SkipAll) {
			return nil
		}
		return err
	}

	w := &indexWalker{
		fsc:     fsc,
		ordered: make(chan *indexListing, indexWalkWindow),
		jobs:    make(chan *indexListing, indexWalkWindow),
		quit:    make(chan struct{}),
	}
	defer w.close()

	w.wg.Add(1 + indexWalkWorkers)
	go w.feed(root)
	for range indexWalkWorkers {
		go w.worker()
	}

	var skipped skipList
	for listing := range w.ordered {
		<-listing.done
		if skipped.covers(listing.dir) {
			continue
		}

		for _, e := range listing.entries {
			err := fn(e.Path(), e, nil)
			if err == nil {
				continue
			}
			if errors.Is(err, fs.SkipDir) {
				if e.IsDir() {
					skipped.add(e.Path())
					w.skip(e.Path())
				}
				continue
			}
			if errors.Is(err, fs.SkipAll) {
				return nil
			}
			return err
		}

		if listing.err != nil {
			skipped.add(listing.dir)
			w.skip(listing.dir)
			if err := fn(listing.dir, nil, listing.err); err != nil &&
				!errors.Is(err, fs.SkipDir) {
				if errors.Is(err, fs.SkipAll) {
					return nil
				}
				return err
			}
		}
	}
	if w.err != nil {
		if err := fn(root, nil, w.err); err != nil &&
			!errors.Is(err, fs.SkipDir) && !errors.Is(err, fs.SkipAll) {
			return err
		}
	}
	return nil
}

// skipList holds the subtrees skipped by the walk. Keys come in index order
// and a subtree is contiguous in it, so an entry can be dropped as soon as a
// key past its subtree shows up.
type skipList []string

func (s *skipList) add(dir string) {
	*s = append(*s, dir)
}

func (s *skipList) covers(key string) bool {
	kept := (*s)[:0]
	covered := false
	for _, dir := range *s {
		switch {
		case key == dir || strings.HasPrefix(key, dir+"/"):
			covered = true
			kept = append(kept, dir)
		case key < dir+"/":
			kept = append(kept, dir)
		}
	}
	*s = kept
	return covered
}

type indexWalker struct {
	fsc     *Filesystem
	ordered chan *indexListing
	jobs    chan *indexListing
	quit    chan struct{}
	wg      sync.WaitGroup
	err     error // the index scan failed
	fed     int   // directories queued so far, touched by the feeder only

	mu      sync.Mutex
	skipped skipList
}

func (w *indexWalker) close() {
	close(w.quit)
	// Unblock workers and the feeder, then wait for them.
	for range w.ordered {
	}
	w.wg.Wait()
}

func (w *indexWalker) skip(dir string) {
	w.mu.Lock()
	w.skipped.add(dir)
	w.mu.Unlock()
}

func (w *indexWalker) feed(root string) {
	defer w.wg.Done()
	defer close(w.ordered)
	defer close(w.jobs)

	scan := func(from, prefix string) bool {
		cursor, err := w.fsc.dirpack.ScanFrom(from)
		if err != nil {
			w.err = err
			return false
		}
		return w.feedFrom(cursor, from, prefix)
	}

	// The directories below root are contiguous in the index, but not right
	// after root itself: "/a-b" sorts between "/a" and "/a/b".
	prefix := root + "/"
	if root == "/" {
		prefix = root
	}
	if scan(root, root) && w.fed == 0 && w.err == nil {
		w.err = fmt.Errorf("%w: %s", fs.ErrNotExist, root)
		return
	}
	if root != "/" && w.err == nil {
		scan(prefix, prefix)
	}
}

// feedFrom queues the directories of cursor starting with prefix, or only
// from itself when prefix is from and does not end with a slash. It reports
// whether the walk should go on.
func (w *indexWalker) feedFrom(cursor iterator.Iterator[string, objects.MAC], from, prefix string) bool {
	for cursor.Next() {
		dir, mac := cursor.Current()
		if !strings.HasPrefix(dir, prefix) || (!strings.HasSuffix(prefix, "/") && dir != from) {
			return true
		}

		w.mu.Lock()
		skip := w.skipped.covers(dir)
		w.mu.Unlock()
		if skip {
			continue
		}

		w.fed++
		listing := &indexListing{dir: dir, mac: mac, done: make(chan struct{})}
		select {
		case w.ordered <- listing:
		case <-w.quit:
			return false
		}
		select {
		case w.jobs <- listing:
		case <-w.quit:
			return false
		}
	}
	if err := cursor.Err(); err != nil {
		w.err = err
		return false
	}
	return true
}

func (w *indexWalker) worker() {
	defer w.wg.Done()
	for listing := range w.jobs {
		select {
		case <-w.quit:
		default:
			w.load(listing)
		}
		close(listing.done)
	}
}

func (w *indexWalker) load(listing *indexListing) {
	entries, err := w.fsc.readDirpack(listing.dir, listing.mac)
	if err != nil {
		listing.err = err
		return
	}
	for entry, err := range entries {
		if err != nil {
			listing.err = err
			return
		}
		listing.entries = append(listing.entries, entry)
	}
}
