package snapshot

import (
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"golang.org/x/sync/errgroup"
)

type RestoreOptions struct {
	MaxConcurrency  uint64
	Strip           string
	SkipPermissions bool
	ForceCompletion bool
}

type restoreContext struct {
	hardlinks      map[string]string
	hardlinksMutex sync.Mutex
	pathname       string
	vfs            *vfs.Filesystem
	directories    []dirRec

	errors atomic.Uint64
	force  bool
}

type dirRec struct {
	path string
	info *objects.FileInfo
}

func (ctx *restoreContext) reportFailure(snap *Snapshot, err error, evt events.Event) error {
	if evt != nil {
		snap.Event(evt)
	}
	ctx.errors.Add(1)

	if ctx.force {
		return nil
	}
	return err
}

func snapshotRestorePath(snap *Snapshot, exp exporter.Exporter, target string, opts *RestoreOptions, restoreContext *restoreContext, wg *errgroup.Group) func(entrypath string, e *vfs.Entry, err error) error {
	return func(entrypath string, e *vfs.Entry, err error) error {
		if err != nil {
			evt := events.PathErrorEvent(snap.Header.Identifier, entrypath, err.Error())
			return restoreContext.reportFailure(snap, err, evt)
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
				if err := exp.CreateDirectory(snap.AppContext(), dest); err != nil {
					err := fmt.Errorf("failed to create directory %q: %w", dest, err)
					evt := events.DirectoryErrorEvent(snap.Header.Identifier, entrypath, err.Error())
					return restoreContext.reportFailure(snap, err, evt)
				}
			}

			// WalkDir handles recursion so we don’t need to iterate children manually.
			if entrypath != restoreContext.pathname {
				restoreContext.directories = append(restoreContext.directories, dirRec{path: dest, info: e.Stat()})
			}

			snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, entrypath))
			return nil
		}

		// For non-directory entries, only process regular files.
		if !e.Stat().Mode().IsRegular() {
			if e.Stat().Mode().Type()&fs.ModeSymlink != 0 {
				if err := exp.CreateLink(snap.AppContext(), e.SymlinkTarget, dest, exporter.SYMLINK); err != nil {
					evt := events.FileErrorEvent(snap.Header.Identifier, entrypath,
						fmt.Sprintf("failed to restore symlink: %s\n", err.Error()))
					return restoreContext.reportFailure(snap, err, evt)
				}
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						err := fmt.Errorf("failed to set permissions on symlink %q: %w", entrypath, err)
						evt := events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error())
						return restoreContext.reportFailure(snap, err, evt)
					}
				}
			}
			return nil
		}

		snap.Event(events.FileEvent(snap.Header.Identifier, entrypath))
		wg.Go(func() error {
			if e.Stat().Nlink() > 1 {
				restoreContext.hardlinksMutex.Lock()
				key := fmt.Sprintf("%d:%d", e.Stat().Dev(), e.Stat().Ino())
				v, ok := restoreContext.hardlinks[key]
				if ok {
					if err := exp.CreateLink(snap.AppContext(), v, dest, exporter.HARDLINK); err != nil {
						evt := events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error())
						restoreContext.reportFailure(snap, err, evt)
					}
					restoreContext.hardlinksMutex.Unlock()
					return nil
				}
			}

			rd, err := e.Open(restoreContext.vfs)
			if err != nil {
				restoreContext.reportFailure(snap, err,
					events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
				return nil
			}
			defer rd.Close()

			// Ensure the parent directory exists.
			if err := exp.CreateDirectory(snap.AppContext(), path.Dir(dest)); err != nil {
				err := fmt.Errorf("failed to create directory %q: %w", dest, err)
				restoreContext.reportFailure(snap, err,
					events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error()))
			}

			// Restore the file content.
			if e.Stat().Nlink() > 1 {
				key := fmt.Sprintf("%d:%d", e.Stat().Dev(), e.Stat().Ino())
				restoreContext.hardlinks[key] = dest
				defer restoreContext.hardlinksMutex.Unlock()
			}

			if err := exp.StoreFile(snap.AppContext(), dest, rd, e.Size()); err != nil {
				err := fmt.Errorf("failed to write file %q at %q: %w", entrypath, dest, err)
				evt := events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error())
				restoreContext.reportFailure(snap, err, evt)
			} else {
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						err := fmt.Errorf("failed to set permissions on file %q: %w", entrypath, err)
						evt := events.FileErrorEvent(snap.Header.Identifier, entrypath, err.Error())
						restoreContext.reportFailure(snap, err, evt)
						return nil
					}
				}
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
		vfs:            fs,
		pathname:       pathname,
		directories:    make([]dirRec, 0, 256),
		force:          opts.ForceCompletion,
	}

	base = path.Clean(base)
	if base != "/" && !strings.HasSuffix(base, "/") {
		base = base + "/"
	}

	wg := errgroup.Group{}
	wg.SetLimit(int(maxConcurrency))

	if err := fs.WalkDir(pathname, snapshotRestorePath(snap, exp, base, opts, restoreContext, &wg)); err != nil {
		wg.Wait()
		return err
	}

	wg.Wait()
	if !opts.SkipPermissions {
		sort.Slice(restoreContext.directories, func(i, j int) bool {
			di := strings.Count(restoreContext.directories[i].path, "/")
			dj := strings.Count(restoreContext.directories[j].path, "/")
			return di > dj
		})

		for _, d := range restoreContext.directories {
			if err := exp.SetPermissions(snap.AppContext(), d.path, d.info); err != nil {
				err := fmt.Errorf("failed to set permissions on directory %q: %w", d.path, err)
				snap.Event(events.DirectoryErrorEvent(snap.Header.Identifier, d.path, err.Error()))
			}
		}
	}

	if n := restoreContext.errors.Load(); n > 0 {
		errors := "errors"
		if n == 1 {
			errors = "error"
		}
		return fmt.Errorf("restoration completed with %v %s", n, errors)
	}
	return nil
}
