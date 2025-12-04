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

type hardlinkRecord struct {
	dest string
	done chan struct{}
	err  error
}

type RestoreOptions struct {
	MaxConcurrency  uint64
	Strip           string
	SkipPermissions bool
	ForceCompletion bool
}

type restoreContext struct {
	hardlinks      map[string]*hardlinkRecord
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
			emitter.Emit("snapshot.restore.path.error", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
				"error":       err.Error(),
			})
			return restoreContext.reportFailure(snap, err)
		}

		if err := snap.AppContext().Err(); err != nil {
			return err
		}

		emitter.Emit("snapshot.restore.path", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"path":        entrypath,
		})

		// Determine destination path by stripping the prefix.
		rel := strings.TrimPrefix(entrypath, opts.Strip)
		rel = strings.TrimPrefix(rel, "/")
		dest := path.Join(target, rel)

		// Directory processing.
		if e.IsDir() {
			emitter.Emit("snapshot.restore.directory", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
			})
			// Create directory if not root.
			if entrypath != "/" {
				if err := exp.CreateDirectory(snap.AppContext(), dest); err != nil {
					err := fmt.Errorf("failed to create directory %q: %w", dest, err)

					emitter.Emit("snapshot.restore.directory.error", map[string]any{
						"snapshot_id": snap.Header.Identifier,
						"path":        entrypath,
						"error":       err.Error(),
					})
					return restoreContext.reportFailure(snap, err)
				}
			}

			// WalkDir handles recursion so we don’t need to iterate children manually.
			if entrypath != restoreContext.pathname {
				restoreContext.directories = append(restoreContext.directories, dirRec{path: dest, info: e.Stat()})
			}

			emitter.Emit("snapshot.restore.directory.ok", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
			})
			return nil
		}

		// For non-directory entries, only process regular files.
		if !e.Stat().Mode().IsRegular() {
			emitter.Emit("snapshot.restore.symlink", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
			})

			if e.Stat().Mode().Type()&fs.ModeSymlink != 0 {
				if err := exp.CreateLink(snap.AppContext(), e.SymlinkTarget, dest, exporter.SYMLINK); err != nil {
					emitter.Emit("snapshot.restore.symlink.error", map[string]any{
						"snapshot_id": snap.Header.Identifier,
						"path":        entrypath,
						"error":       err.Error(),
					})
					restoreContext.reportFailure(snap, err)
					return nil
				}
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						emitter.Emit("snapshot.restore.symlink.error", map[string]any{
							"snapshot_id": snap.Header.Identifier,
							"path":        entrypath,
							"error":       err.Error(),
						})
						restoreContext.reportFailure(snap, err)
						return nil
					}
				}
				emitter.Emit("snapshot.restore.symlink.ok", map[string]any{
					"snapshot_id": snap.Header.Identifier,
					"path":        entrypath,
				})
			}
			return nil
		}

		emitter.Emit("snapshot.restore.file", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"path":        entrypath,
		})

		wg.Go(func() error {

			stat := e.Stat()
			nlink := stat.Nlink()

			var hardlinkKey string
			var rec *hardlinkRecord
			var isLeader bool
			var leaderErr error
			var leaderDest string

			if nlink > 1 {
				hardlinkKey = fmt.Sprintf("%d:%d", stat.Dev(), stat.Ino())

				restoreContext.hardlinksMutex.Lock()
				rec, ok := restoreContext.hardlinks[hardlinkKey]
				if !ok {
					rec = &hardlinkRecord{done: make(chan struct{})}
					restoreContext.hardlinks[hardlinkKey] = rec
					isLeader = true
				}
				restoreContext.hardlinksMutex.Unlock()

				if !isLeader {
					<-rec.done

					if rec.err != nil {
						if err2 := restoreContext.reportFailure(snap, rec.err); err2 != nil {
							return err2
						}
						return nil
					}

					if err := exp.CreateLink(snap.AppContext(), rec.dest, dest, exporter.HARDLINK); err != nil {
						emitter.Emit("snapshot.restore.file.error", map[string]any{
							"snapshot_id": snap.Header.Identifier,
							"path":        entrypath,
							"error":       err.Error(),
						})
						if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
							return err2
						}
					} else {
						emitter.Emit("snapshot.restore.file.ok", map[string]any{
							"snapshot_id": snap.Header.Identifier,
							"path":        entrypath,
							"size":        e.Size(),
						})
					}
					return nil
				}
			}

			if isLeader {
				defer func() {
					restoreContext.hardlinksMutex.Lock()
					defer restoreContext.hardlinksMutex.Unlock()

					rec.dest = leaderDest
					rec.err = leaderErr
					close(rec.done)
				}()
			}

			rd, err := e.Open(restoreContext.vfs)
			if err != nil {
				emitter.Emit("snapshot.restore.file.error", map[string]any{
					"snapshot_id": snap.Header.Identifier,
					"path":        entrypath,
					"error":       err.Error(),
				})
				if isLeader {
					leaderErr = err
				}
				if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
					return err2
				}
				return nil
			}
			defer rd.Close()

			// Ensure the parent directory exists.
			if err := exp.CreateDirectory(snap.AppContext(), path.Dir(dest)); err != nil {
				err := fmt.Errorf("failed to create directory %q: %w", dest, err)
				emitter.Emit("snapshot.restore.file.error", map[string]any{
					"snapshot_id": snap.Header.Identifier,
					"path":        entrypath,
					"error":       err.Error(),
				})
				if isLeader {
					leaderErr = err
				}
				if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
					return err2
				}
				return nil
			}

			if err := exp.StoreFile(snap.AppContext(), dest, rd, e.Size()); err != nil {
				err := fmt.Errorf("failed to write file %q at %q: %w", entrypath, dest, err)
				emitter.Emit("snapshot.restore.file.error", map[string]any{
					"snapshot_id": snap.Header.Identifier,
					"path":        entrypath,
					"error":       err.Error(),
				})
				if isLeader {
					leaderErr = err
				}
				if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
					return err2
				}
				return nil
			}

			if !opts.SkipPermissions {
				if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
					err := fmt.Errorf("failed to set permissions on file %q: %w", entrypath, err)
					emitter.Emit("snapshot.restore.file.error", map[string]any{
						"snapshot_id": snap.Header.Identifier,
						"path":        entrypath,
						"error":       err.Error(),
					})
					if isLeader {
						leaderErr = err
					}
					if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
						return err2
					}
					return nil
				}
			}

			if isLeader {
				leaderDest = dest
				// leaderErr stays nil
			}

			emitter.Emit("snapshot.restore.file.ok", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
				"size":        e.Size(),
			})
			return nil
		})
		return nil
	}
}

func (snap *Snapshot) Restore(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
	emitter := snap.AppContext().Events().Emitter()
	emitter.Emit("snapshot.restore.start", map[string]any{
		"snapshot": snap.Header.Identifier,
	})
	defer emitter.Emit("snapshot.restore.done", map[string]any{
		"snapshot": snap.Header.Identifier,
	})

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.AppContext().MaxConcurrency)
	}

	restoreContext := &restoreContext{
		hardlinks:      make(map[string]*hardlinkRecord),
		hardlinksMutex: sync.Mutex{},
		vfs:            fs,
		pathname:       pathname,
		directories:    make([]dirRec, 0, 256),
		force:          opts.ForceCompletion,
		emitter:        emitter,
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
				emitter.Emit("snapshot.restore.directory.error", map[string]any{
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
