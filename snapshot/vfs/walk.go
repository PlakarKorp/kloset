package vfs

import (
	"io/fs"
	"iter"
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

	var it iter.Seq2[*Entry, error]
	var err error
	if fsc.dirpack == nil {
		it, err = entry.Getdents(fsc)
	} else {
		//it, err = entry.Getdents2(fsc)
	}

	if err != nil {
		return fn(path, nil, err)
	}

	for entry, err := range it {
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
		return err
	}

	if err = fsc.walkdir(entry, fn); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}
