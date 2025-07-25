package vfs

import (
	"io/fs"
	"log"
	"path"
	"strings"

	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/errgroup"
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
	// entry, err := fsc.GetEntry(root)
	// if err != nil {
	// 	return err
	// }

	// if err = fsc.walkdir(entry, fn); err != nil {
	// 	if err == fs.SkipDir || err == fs.SkipAll {
	// 		err = nil
	// 	}
	// }
	// return err

	it, err := fsc.tree.ScanFrom(root)
	if err != nil {
		return err
	}

	for it.Next() {
		path, _ := it.Current()
		// entry, err := fsc.ResolveEntry(mac)
		// if err != nil {
		// 	return err
		// }

		if err := fn(path, nil, nil); err != nil {
			return err
		}
	}

	return nil
}

type Item struct {
	Entry *Entry
	Err   error
}

func (fsc *Filesystem) Walk(root string, maxConcurrency int) chan Item {
	maxConcurrency = max(maxConcurrency, 1)

	root = path.Clean(root)
	if root != "/" {
		root += "/"
	}

	log.Println("root is", root)

	it, err := fsc.tree.ScanFrom(root)
	if err != nil {
		return nil
	}

	prod := make(chan objects.MAC, maxConcurrency)
	go func() {
		defer close(prod)
		for it.Next() {
			p, mac := it.Current()

			if !strings.HasPrefix(p, root) {
				panic("XXX")
			}

			prod <- mac
		}
	}()

	var wg errgroup.Group
	entries := make(chan Item, maxConcurrency)
	for range maxConcurrency {
		wg.Go(func() error {
			for mac := range prod {
				entry, err := fsc.ResolveEntry(mac)
				entries <- Item{entry, err}
				if err != nil {
					log.Println("ERROR", err)
					return err
				}
			}
			return nil
		})
	}

	go func() {
		log.Println("about to wait entries pool")
		wg.Wait()
		log.Println("about to close(entries)")
		close(entries)
	}()

	return entries
}
