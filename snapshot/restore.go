package snapshot

import (
	"errors"
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

	emitter *events.Emitter
}

type dirRec struct {
	path string
	info *objects.FileInfo
}

func (ctx *restoreContext) reportFailure(snap *Snapshot, err error) error {
	ctx.errors.Add(1)

	if ctx.force {
		return nil
	}
	return err
}

func snapshotRestorePath(snap *Snapshot, exp exporter.Exporter, target string, opts *RestoreOptions, restoreContext *restoreContext, wg *errgroup.Group) func(entrypath string, e *vfs.Entry, err error) error {
	emitter := restoreContext.emitter
	return func(entrypath string, e *vfs.Entry, err error) error {
		if err != nil {
			emitter.PathError(entrypath, err)
			return restoreContext.reportFailure(snap, err)
		}

		if err := snap.AppContext().Err(); err != nil {
			return err
		}
		emitter.Path(entrypath)

		// Determine destination path by stripping the prefix.
		rel := strings.TrimPrefix(entrypath, opts.Strip)
		rel = strings.TrimPrefix(rel, "/")
		dest := path.Join(target, rel)

		// Directory processing.
		if e.IsDir() {
			emitter.Directory(entrypath)

			// Create directory if not root.
			if entrypath != "/" {
				if err := exp.CreateDirectory(snap.AppContext(), dest); err != nil {
					err := fmt.Errorf("failed to create directory %q: %w", dest, err)
					emitter.DirectoryError(entrypath, err)
					return restoreContext.reportFailure(snap, err)
				}
			}

			// WalkDir handles recursion so we don’t need to iterate children manually.
			if entrypath != restoreContext.pathname {
				restoreContext.directories = append(restoreContext.directories, dirRec{path: dest, info: e.Stat()})
			}
			emitter.DirectoryOk(entrypath)

			return nil
		}

		// For non-directory entries, only process regular files.
		if !e.Stat().Mode().IsRegular() {
			if e.Stat().Mode().Type()&fs.ModeSymlink != 0 {
				emitter.Symlink(entrypath)
				if err := exp.CreateLink(snap.AppContext(), e.SymlinkTarget, dest, exporter.SYMLINK); err != nil {
					emitter.SymlinkError(entrypath, err)
					restoreContext.reportFailure(snap, err)
					return nil
				}
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						emitter.SymlinkError(entrypath, err)
						restoreContext.reportFailure(snap, err)
						return nil
					}
				}
				emitter.SymlinkOk(entrypath)
			}
			return nil
		}

		emitter.File(entrypath)
		wg.Go(func() error {
			if e.Stat().Nlink() > 1 {
				restoreContext.hardlinksMutex.Lock()
				key := fmt.Sprintf("%d:%d", e.Stat().Dev(), e.Stat().Ino())
				v, ok := restoreContext.hardlinks[key]
				if ok {
					if err := exp.CreateLink(snap.AppContext(), v, dest, exporter.HARDLINK); err != nil {
						emitter.FileError(entrypath, err)
						restoreContext.reportFailure(snap, err)
					}
					restoreContext.hardlinksMutex.Unlock()
					return nil
				}
			}

			rd, err := e.Open(restoreContext.vfs)
			if err != nil {
				emitter.FileError(entrypath, err)
				restoreContext.reportFailure(snap, err)
				return nil
			}
			defer rd.Close()

			// Restore the file content.
			if e.Stat().Nlink() > 1 {
				key := fmt.Sprintf("%d:%d", e.Stat().Dev(), e.Stat().Ino())
				restoreContext.hardlinks[key] = dest
				defer restoreContext.hardlinksMutex.Unlock()
			}

			if err := exp.StoreFile(snap.AppContext(), dest, rd, e.Size()); err != nil {
				err := fmt.Errorf("failed to write file %q at %q: %w", entrypath, dest, err)
				emitter.FileError(entrypath, err)
				restoreContext.reportFailure(snap, err)
			} else {
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						err := fmt.Errorf("failed to set permissions on file %q: %w", entrypath, err)
						emitter.FileError(entrypath, err)
						restoreContext.reportFailure(snap, err)
						return nil
					}
				}
				emitter.FileOk(entrypath)
			}
			return nil
		})
		return nil
	}
}

func (snap *Snapshot) Restore(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
	emitter := snap.Emitter("restore")
	defer emitter.Close()

	pvfs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	restoreContext := &restoreContext{
		hardlinks:      make(map[string]string),
		hardlinksMutex: sync.Mutex{},
		vfs:            pvfs,
		pathname:       pathname,
		directories:    make([]dirRec, 0, 256),
		force:          opts.ForceCompletion,
		emitter:        emitter,
	}

	base = path.Clean(base)
	if base != "/" && !strings.HasSuffix(base, "/") {
		base = base + "/"
	}

	if err := exp.CreateDirectory(snap.AppContext(), base); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("failed to create base directory %q: %w", base, err)
		}
	}

	wg := errgroup.Group{}
	wg.SetLimit(int(snap.AppContext().MaxConcurrency))

	if err := pvfs.WalkDir(pathname, snapshotRestorePath(snap, exp, base, opts, restoreContext, &wg)); err != nil {
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
				emitter.Warn("snapshot.restore.directory.error", map[string]any{
					"snapshot": snap.Header.Identifier,
					"path":     d.path,
					"error":    err.Error(),
				})
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
