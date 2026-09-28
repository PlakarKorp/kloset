package vfs

import (
	"bytes"
	"context"
	"io/fs"
	"iter"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/PlakarKorp/kloset/objects"
)

type WalkDirFunc func(path string, entry *Entry, err error) error

func (fsc *Filesystem) walkdir(entry *Entry, fn WalkDirFunc) error {
	path := entry.Path()
	if err := fn(path, entry, nil); err != nil {
		return err
	}

	if !entry.FileInfo.Mode().IsDir() {
		return nil
	}

	iter, err := entry.Getdents(fsc)
	if err != nil {
		return fn(path, nil, err)
	}

	for entry, err := range iter {
		if err != nil {
			return fn(path, nil, err)
		}

		if err := fsc.walkdir(entry, fn); err != nil {
			if err == fs.SkipDir {
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

	if err = fsc.walkdir(entry, fn); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}

// dirpackWalkWindow is how many directories a walk fetches ahead of the
// one being emitted, per batch.
const dirpackWalkWindow = 1024

func dirpackWalkWindowSize() int {
	if s := os.Getenv("PLAKAR_DIRPACK_WALK_WINDOW"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return dirpackWalkWindow
}

// WalkDirpack visits a tree hierarchy like Walkdir but based on the dirpack
// index. The guarantee your get is that parents are always visited before
// children.
//
// Directory payloads are fetched one lookahead window at a time through
// fetchDirpacks, so the reads coalesce instead of paying one round-trip per
// directory.
func (fsc *Filesystem) WalkDirpack(ctx context.Context, root string, fn WalkDirFunc) error {
	if fsc.dirpack == nil {
		return fsc.WalkDir(root, fn)
	}

	entry, err := fsc.GetEntry(root)
	if err != nil {
		return fn(root, nil, err)
	}

	if err = fsc.walkDirpack(ctx, entry.Path(), entry, fn); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}

type dirpackWalkRef struct {
	path string
	mac  objects.MAC
}

type dirpackWalkBatch struct {
	dirs    []dirpackWalkRef
	payload map[string][]byte
}

func (fsc *Filesystem) walkDirpack(ctx context.Context, root string, rootEntry *Entry, fn WalkDirFunc) error {
	// the root's own record lives in its parent's payload, which is
	// outside the scan; emit it from the entry already resolved.
	if err := fn(root, rootEntry, nil); err != nil {
		return err
	}

	if !rootEntry.FileInfo.Mode().IsDir() {
		return nil
	}

	prefix := root
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	cursor, err := fsc.dirpack.ScanFrom(root)
	if err != nil {
		return fn(root, nil, err)
	}

	windowSize := dirpackWalkWindowSize()

	fctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The feeder owns the cursor: it slices the scan into windows and
	// fetches each window's payloads while the previous one is being
	// emitted. It stops when the scan is done or fctx is cancelled, and
	// its cursor error is read only after batches is closed.
	batches := make(chan dirpackWalkBatch, 1)
	var cursorErr error
	go func() {
		defer close(batches)

		done := false
		for !done {
			var dirs []dirpackWalkRef
			for len(dirs) < windowSize {
				if !cursor.Next() {
					cursorErr = cursor.Err()
					done = true
					break
				}
				dirpath, objectMac := cursor.Current()

				if dirpath != root && !strings.HasPrefix(dirpath, prefix) {
					// We are past the end, the index is lexical.
					if dirpath > prefix {
						done = true
						break
					}

					// Special case due to the lexical nature of the index, one of the
					// sibling of root sorts between root and root + "/" eg :
					// /usr.bak sorts between /root and /root/ but is outside of the
					// scope of this walkdir.
					continue
				}

				dirs = append(dirs, dirpackWalkRef{path: dirpath, mac: objectMac})
			}

			if len(dirs) == 0 {
				return
			}

			reqs := make(map[string]objects.MAC, len(dirs))
			for _, d := range dirs {
				reqs[d.path] = d.mac
			}

			batch := dirpackWalkBatch{dirs: dirs, payload: fsc.fetchDirpacks(fctx, reqs)}
			select {
			case batches <- batch:
			case <-fctx.Done():
				return
			}
		}
	}()

	// directories fn asked to skip: their descendants are separate
	// index keys, so each key is checked against its ancestors.
	skipped := make(map[string]struct{})

	for batch := range batches {
		for _, d := range batch.dirs {
			if err := ctx.Err(); err != nil {
				return err
			}

			if skippedAncestor(skipped, root, d.path) {
				continue
			}

			var dents iter.Seq2[*Entry, error]
			if data, ok := batch.payload[d.path]; ok {
				dents = dirpackEntriesIter(d.path, bytes.NewReader(data))
			} else {
				// fetchDirpacks is best effort: load this directory on
				// demand, so a fetch failure surfaces its real error here.
				dents, err = fsc.dirpackEntries(d.path, d.mac)
				if err != nil {
					if err := fn(d.path, nil, err); err != nil {
						return err
					}
					continue
				}
			}

			if err := fsc.walkDirpackPayload(d.path, dents, skipped, fn); err != nil {
				return err
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if cursorErr != nil {
		return fn(root, nil, cursorErr)
	}
	return nil
}

func (fsc *Filesystem) walkDirpackPayload(dirpath string, dents iter.Seq2[*Entry, error], skipped map[string]struct{}, fn WalkDirFunc) error {
	for entry, err := range dents {
		if err != nil {
			// the payload iterator terminates itself after an error
			if err := fn(dirpath, nil, err); err != nil {
				return err
			}
			continue
		}

		entrypath := entry.Path()
		if err := fn(entrypath, entry, nil); err != nil {
			if err == fs.SkipDir {
				if entry.FileInfo.Mode().IsDir() {
					skipped[entrypath] = struct{}{}
				}
				continue
			}
			return err
		}
	}
	return nil
}

func skippedAncestor(skipped map[string]struct{}, root, dirpath string) bool {
	if len(skipped) == 0 {
		return false
	}
	for p := dirpath; p != root && p != "/"; p = path.Dir(p) {
		if _, ok := skipped[p]; ok {
			return true
		}
	}
	return false
}
