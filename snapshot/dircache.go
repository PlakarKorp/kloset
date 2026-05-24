package snapshot

import (
	"path"
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/vmihailenco/msgpack/v5"
)

// dirCacheEntry is the persisted value stored in the per-repository
// dircache. It captures everything needed at the start of the next backup
// to decide whether a directory's dirpack can be reused verbatim.
//
// DirpackMAC is the MAC of the dirpack object produced for this directory
// in the previous successful backup. Children is the sorted list of
// immediate child names (file or subdirectory) that were packed into that
// dirpack, used to detect additions and deletions cheaply without having
// to fetch and parse the prior dirpack from the repository.
type dirCacheEntry struct {
	DirpackMAC objects.MAC `msgpack:"dirpack_mac"`
	Children   []string    `msgpack:"children"`
}

func (e *dirCacheEntry) serialize() ([]byte, error) {
	return msgpack.Marshal(e)
}

func dirCacheEntryFromBytes(b []byte) (*dirCacheEntry, error) {
	var e dirCacheEntry
	if err := msgpack.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return &e, nil
}

// dirtySet tracks which directories were touched during an import in a way
// that requires their dirpack to be rebuilt. A directory is dirty when any
// of its immediate children flowed through the slow (rechunk / rewrite)
// path during import, or when its child set has changed compared to the
// previous backup.
//
// The set is also used after import to propagate dirtiness from a
// directory up to all of its ancestors before dirpack reuse decisions are
// made.
type dirtySet struct {
	m sync.Map // path -> struct{}
}

// markChild marks the parent directory of pathname as dirty.
//
// This is called from the per-record slow path during import; pathname is
// the file or subdirectory that could not be reused. The parent is what
// must be rebuilt, hence the path.Dir.
func (d *dirtySet) markChild(pathname string) {
	d.m.Store(path.Dir(pathname), struct{}{})
}

// mark flags pathname itself as dirty.
func (d *dirtySet) mark(pathname string) {
	d.m.Store(pathname, struct{}{})
}

func (d *dirtySet) isDirty(pathname string) bool {
	_, ok := d.m.Load(pathname)
	return ok
}

// propagateToAncestors marks every ancestor of pathname dirty. Used after
// import to bubble dirtiness up to the root so any directory above a
// changed entry is correctly rebuilt.
func (d *dirtySet) propagateToAncestors(pathname string) {
	for p := path.Dir(pathname); p != "/" && p != "."; p = path.Dir(p) {
		d.m.Store(p, struct{}{})
	}
	if pathname != "/" {
		d.m.Store("/", struct{}{})
	}
}
