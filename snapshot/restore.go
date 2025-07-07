package snapshot

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"

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

func snapshotRestorePath(snap *Snapshot, exp exporter.Exporter, target string, opts *RestoreOptions, restoreContext *restoreContext, wg *errgroup.Group) func(entrypath string, e *vfs.Entry, err error) error {
	return func(entrypath string, e *vfs.Entry, err error) error {
		if err != nil {
			snap.Event(events.PathErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
			return err
		}

		if err := snap.AppContext().Err(); err != nil {
			return err
		}

		snap.Event(events.PathEvent(snap.Header.Identifier, entrypath))

		// Determine destination path by stripping the prefix.
		dest := path.Join(target, strings.TrimPrefix(entrypath, opts.Strip))

		// Directory processing.
		if e.IsDir() {
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
			return nil
		}

		// For non-directory entries, only process regular files.
		if !e.Stat().Mode().IsRegular() {
			snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, "unexpected vfs entry type"))
			return nil
		}

		snap.Event(events.FileEvent(snap.Header.Identifier, entrypath))
		wg.Go(func() error {
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

			rd, err := snap.NewReader(entrypath)
			if err != nil {
				err := fmt.Errorf("failed to get a reader to file %q: %w", entrypath, err)
				snap.Event(events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
				return nil
			}
			defer rd.Close()

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
			return nil
		})
		return nil
	}
}

func (snap *Snapshot) Restore(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
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
	defer wg.Wait()

	return fs.WalkDir(pathname, snapshotRestorePath(snap, exp, base, opts, restoreContext, &wg))
}
