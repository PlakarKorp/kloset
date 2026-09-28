package vfs

import (
	"io/fs"
	"path"
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

// WalkDirpack visits a tree hierarchy like Walkdir but based on the dirpack
// index. The guarantee your get is that parents are always visited before
// children.
func (fsc *Filesystem) WalkDirpack(root string, fn WalkDirFunc) error {
	if fsc.dirpack == nil {
		return fsc.WalkDir(root, fn)
	}

	entry, err := fsc.GetEntry(root)
	if err != nil {
		return fn(root, nil, err)
	}

	if err = fsc.walkDirpack(entry.Path(), entry, fn); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}

func (fsc *Filesystem) walkDirpack(root string, rootEntry *Entry, fn WalkDirFunc) error {
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

	iter, err := fsc.dirpack.ScanFrom(root)
	if err != nil {
		return fn(root, nil, err)
	}

	// directories fn asked to skip: their descendants are separate
	// index keys, so each key is checked against its ancestors.
	skipped := make(map[string]struct{})

	for iter.Next() {
		dirpath, objectMac := iter.Current()

		if dirpath != root && !strings.HasPrefix(dirpath, prefix) {
			// We are past the end, the index is lexical.
			if dirpath > prefix {
				break
			}

			// Special case due to the lexical nature of the index, one of the
			// sibling of root sorts between root and root + "/" eg :
			// /usr.bak sorts between /root and /root/ but is outside of the
			// scope of this walkdir.
			continue
		}

		if skippedAncestor(skipped, root, dirpath) {
			continue
		}

		if err := fsc.walkDirpackPayload(dirpath, objectMac, skipped, fn); err != nil {
			return err
		}
	}

	if err := iter.Err(); err != nil {
		return fn(root, nil, err)
	}
	return nil
}

func (fsc *Filesystem) walkDirpackPayload(dirpath string, objectMac objects.MAC, skipped map[string]struct{}, fn WalkDirFunc) error {
	dents, err := fsc.dirpackEntries(dirpath, objectMac)
	if err != nil {
		return fn(dirpath, nil, err)
	}

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
