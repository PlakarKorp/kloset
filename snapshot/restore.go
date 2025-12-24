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
	"time"

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

type hardlinkRecord struct {
	dest string
	done chan struct{}
	err  error
}

type restoreContext struct {
	hardlinks      map[string]*hardlinkRecord
	hardlinksMutex sync.Mutex
	pathname       string
	vfs            *vfs.Filesystem
	directories    []dirRec

	size   atomic.Uint64
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
			stat := e.Stat()
			nlink := stat.Nlink()

			var hardlinkKey string
			var rec *hardlinkRecord
			var ok bool
			var isLeader bool
			var leaderErr error
			var leaderDest string

			// Hardlink coordination: exactly one leader writes the file, others wait and link.
			if nlink > 1 {
				hardlinkKey = fmt.Sprintf("%d:%d", stat.Dev(), stat.Ino())

				restoreContext.hardlinksMutex.Lock()
				rec, ok = restoreContext.hardlinks[hardlinkKey]
				if !ok {
					rec = &hardlinkRecord{done: make(chan struct{})}
					restoreContext.hardlinks[hardlinkKey] = rec
					isLeader = true
				}
				restoreContext.hardlinksMutex.Unlock()

				// Follower: wait for leader to finish, then create hardlink or propagate error.
				if !isLeader {
					<-rec.done

					if rec.err != nil {
						restoreContext.reportFailure(snap, rec.err)
						return nil
					}

					if err := exp.CreateLink(snap.AppContext(), rec.dest, dest, exporter.HARDLINK); err != nil {
						emitter.FileError(entrypath, err)
						restoreContext.reportFailure(snap, err)
					} else {
						emitter.FileOk(entrypath)
					}
					return nil
				}
			}

			// Leader: publish result to followers when done.
			if isLeader {
				defer func() {
					rec.dest = leaderDest
					rec.err = leaderErr
					close(rec.done)
				}()
			}

			rd, err := e.Open(restoreContext.vfs)
			if err != nil {
				emitter.FileError(entrypath, err)
				if isLeader {
					leaderErr = err
				}
				restoreContext.reportFailure(snap, err)
				return nil
			}
			defer rd.Close()

			if err := exp.StoreFile(snap.AppContext(), dest, rd, e.Size()); err != nil {
				err := fmt.Errorf("failed to write file %q at %q: %w", entrypath, dest, err)
				emitter.FileError(entrypath, err)
				if isLeader {
					leaderErr = err
				}
				restoreContext.reportFailure(snap, err)
			} else {
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						err := fmt.Errorf("failed to set permissions on file %q: %w", entrypath, err)
						emitter.FileError(entrypath, err)
						if isLeader {
							leaderErr = err
						}
						restoreContext.reportFailure(snap, err)
						return nil
					}
				}
				if isLeader {
					leaderDest = dest
				}
				emitter.FileOk(entrypath)
				restoreContext.size.Add(uint64(e.Size()))
			}
			return nil
		})
		return nil
	}
}

func (snap *Snapshot) Restore(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
	emitter := snap.Emitter("restore")
	defer emitter.Close()

	fileCount := snap.Header.GetSource(0).Summary.Directory.Files + snap.Header.GetSource(0).Summary.Below.Files
	dirCount := snap.Header.GetSource(0).Summary.Directory.Directories + snap.Header.GetSource(0).Summary.Below.Directories - uint64(len(strings.Split(pathname, "/"))-2)
	symlinkCount := snap.Header.GetSource(0).Summary.Directory.Symlinks + snap.Header.GetSource(0).Summary.Below.Symlinks
	totalSize := snap.Header.GetSource(0).Summary.Directory.Size + snap.Header.GetSource(0).Summary.Below.Size

	emitter.FilesystemSummary(fileCount, dirCount, symlinkCount, 0, totalSize)

	pvfs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	restoreContext := &restoreContext{
		hardlinks:      make(map[string]*hardlinkRecord),
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
	wg.SetLimit(int(snap.AppContext().MaxConcurrency) * 2)

	t0 := time.Now()

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
				emitter.DirectoryError(d.path, err)
			}
		}
	}

	err = nil
	if n := restoreContext.errors.Load(); n > 0 {
		errors := "errors"
		if n == 1 {
			errors = "error"
		}
		err = fmt.Errorf("restoration completed with %v %s", n, errors)
	}

	rBytes := snap.repository.RBytes()
	wBytes := snap.repository.WBytes()

	target, err := exp.Root(snap.AppContext())
	if err != nil {
		return err
	}

	emitter.Result(target, restoreContext.size.Load(), restoreContext.errors.Load(), time.Since(t0), rBytes, wBytes)

	return err
}
