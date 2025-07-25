package snapshot

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"golang.org/x/sync/errgroup"
)

type RestoreOptions struct {
	MaxConcurrency uint64
	Strip          string
}

type restoreContext struct {
	hardlinks      map[string]string
	hardlinksMutex sync.Mutex
}

var t atomic.Int64

func snapshotRestorePath(snap *Snapshot, exp exporter.Exporter, target string, opts *RestoreOptions, restoreContext *restoreContext, it chan vfs.Item) error {
	vfs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	for item := range it {
		if item.Err != nil {
			// XXX
			snap.Event(events.PathErrorEvent(snap.Header.Identifier, target, item.Err.Error()))
			return item.Err
		}

		if err := snap.AppContext().Err(); err != nil {
			return err
		}

		e := item.Entry
		entrypath := e.Path()

		snap.Event(events.PathEvent(snap.Header.Identifier, entrypath))

		// Determine destination path by stripping the prefix.
		dest := path.Join(target, strings.TrimPrefix(entrypath, opts.Strip))

		// Directory processing.
		if false && e.IsDir() {
			snap.Event(events.DirectoryEvent(snap.Header.Identifier, entrypath))
			// Create directory if not root.
			if entrypath != "/" {
				if err := exp.CreateDirectory(dest); err != nil {
					err := fmt.Errorf("failed to create directory %q: %w", dest, err)
					snap.Event(events.DirectoryErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
					return err
				}
			}

			// WalkDir handles recursion so we don’t need to iterate children manually.
			if entrypath != "/" {
				if err := exp.SetPermissions(dest, e.Stat()); err != nil {
					err := fmt.Errorf("failed to set permissions on directory %q: %w", dest, err)
					snap.Event(events.DirectoryErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
					return err
				}
			}
			snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, entrypath))
			continue
		}

		// For non-directory entries, only process regular files.
		if !e.Stat().Mode().IsRegular() {
			//snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, "unexpected vfs entry type"))
			continue
		}

		snap.Event(events.FileEvent(snap.Header.Identifier, entrypath))
		// Handle hard links.
		if e.Stat().Nlink() > 1 {
			key := fmt.Sprintf("%d:%d", e.Stat().Dev(), e.Stat().Ino())
			restoreContext.hardlinksMutex.Lock()
			v, ok := restoreContext.hardlinks[key]
			restoreContext.hardlinksMutex.Unlock()
			if ok {
				// Create a new link and return.
				if err := os.Link(v, dest); err != nil {
					snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
				}
				return nil
			} else {
				restoreContext.hardlinksMutex.Lock()
				restoreContext.hardlinks[key] = dest
				restoreContext.hardlinksMutex.Unlock()
			}
		}

		rd := e.Open(vfs)

		// Ensure the parent directory exists.
		if err := exp.CreateDirectory(path.Dir(dest)); err != nil {
			err := fmt.Errorf("failed to create directory %q: %w", dest, err)
			snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
		}

		// Restore the file content.
		if err := exp.StoreFile(dest, rd, e.Size()); err != nil {
			err := fmt.Errorf("failed to write file %q at %q: %w", entrypath, dest, err)
			snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
		} else if err := exp.SetPermissions(dest, e.Stat()); err != nil {
			err := fmt.Errorf("failed to set permissions on file %q: %w", entrypath, err)
			snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
		} else {
			snap.Event(events.FileOKEvent(snap.Header.Identifier, entrypath, e.Size()))
		}

		rd.Close()
	}

	log.Println("quitting from one restore worker")

	return nil
}

func (snap *Snapshot) Restore(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
	opts.MaxConcurrency = max(opts.MaxConcurrency, 1)

	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.AppContext().MaxConcurrency)
	}

	restoreContext := &restoreContext{
		hardlinks:      make(map[string]string),
		hardlinksMutex: sync.Mutex{},
	}

	base = path.Clean(base)
	if base != "/" && !strings.HasSuffix(base, "/") {
		base = base + "/"
	}

	wg := errgroup.Group{}
	wg.SetLimit(int(maxConcurrency))

	it := fs.Walk(pathname, int(opts.MaxConcurrency))

	for range opts.MaxConcurrency {
		wg.Go(func() error {
			err := snapshotRestorePath(snap, exp, base, opts, restoreContext, it)
			if err != nil {
				log.Println("error during restore:", err)
			}
			return err
		})
	}

	log.Println("restore pool started", len(it))
	err = wg.Wait()
	log.Println("restore pool closed", len(it))

	print := false
	for range it {
		if !print {
			print = true
			log.Println("draining?")
		}
	}

	return err
}
