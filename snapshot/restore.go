package snapshot

import (
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

	activeWorkers atomic.Uint64
	openCount     atomic.Uint64
	openTimeNS    atomic.Uint64

	storeCount  atomic.Uint64
	storeTimeNS atomic.Uint64
	storeBytes  atomic.Uint64
}

func (ctx *restoreContext) workerStart() {
	ctx.activeWorkers.Add(1)
}

func (ctx *restoreContext) workerDone() {
	// subtract 1 using 2's complement
	ctx.activeWorkers.Add(^uint64(0))
}

func (ctx *restoreContext) addOpenSample(d time.Duration) {
	ctx.openCount.Add(1)
	ctx.openTimeNS.Add(uint64(d))
}

func (ctx *restoreContext) addStoreSample(d time.Duration, nbytes uint64) {
	ctx.storeCount.Add(1)
	ctx.storeTimeNS.Add(uint64(d))
	ctx.storeBytes.Add(nbytes)
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
					if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
						return err2
					}
					return nil
				}
				if !opts.SkipPermissions {
					if err := exp.SetPermissions(snap.AppContext(), dest, e.Stat()); err != nil {
						emitter.Emit("snapshot.restore.symlink.error", map[string]any{
							"snapshot_id": snap.Header.Identifier,
							"path":        entrypath,
							"error":       err.Error(),
						})
						if err2 := restoreContext.reportFailure(snap, err); err2 != nil {
							return err2
						}
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
			restoreContext.workerStart()
			defer restoreContext.workerDone()

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

			openStart := time.Now()
			rd, err := e.Open(restoreContext.vfs)
			openDur := time.Since(openStart)
			restoreContext.addOpenSample(openDur)

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

			storeStart := time.Now()
			if err := exp.StoreFile(snap.AppContext(), dest, rd, e.Size()); err != nil {
				storeDur := time.Since(storeStart)
				restoreContext.addStoreSample(storeDur, 0)
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
			storeDur := time.Since(storeStart)
			restoreContext.addStoreSample(storeDur, uint64(e.Size()))

			opts.SkipPermissions = true
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

	start := time.Now()

	tickerDone := make(chan struct{})
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()

		for {
			select {
			case <-t.C:
				active := restoreContext.activeWorkers.Load()
				openCount := restoreContext.openCount.Load()
				openTime := restoreContext.openTimeNS.Load()
				storeCount := restoreContext.storeCount.Load()
				storeTime := restoreContext.storeTimeNS.Load()
				storeBytes := restoreContext.storeBytes.Load()
				elapsed := time.Since(start).Seconds()

				var avgOpenMS, avgStoreMS float64
				if openCount > 0 {
					avgOpenMS = float64(openTime) / float64(openCount) / 1e6
				}
				if storeCount > 0 {
					avgStoreMS = float64(storeTime) / float64(storeCount) / 1e6
				}

				var mbPerSec float64
				if elapsed > 0 && storeBytes > 0 {
					mbPerSec = float64(storeBytes) / (1024 * 1024) / elapsed
				}

				fmt.Println("----- Restore Stats -----")
				fmt.Printf("Active: %d/%d  Open_avg_ms: %.2f  Store_avg_ms: %.2f  MB/s: %.2f\n",
					active, maxConcurrency, avgOpenMS, avgStoreMS, mbPerSec)
				fmt.Println("-------------------------")

			case <-tickerDone:
				return
			}
		}
	}()
	defer close(tickerDone)

	base = path.Clean(base)
	if base != "/" && !strings.HasSuffix(base, "/") {
		base = base + "/"
	}

	wg := errgroup.Group{}
	wg.SetLimit(int(maxConcurrency))

	if err := exp.CreateDirectory(snap.AppContext(), base); err != nil {
		return fmt.Errorf("failed to create base directory %q: %w", base, err)
	}

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
