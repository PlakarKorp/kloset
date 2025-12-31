package snapshot

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

type Builder struct {
	appContext *kcontext.KContext

	repository *repository.RepositoryWriter

	//This is protecting the above two pointers, not their underlying objects
	scanCache  *caching.ScanCache
	deltaCache *caching.ScanCache
	vfsCache   *vfs.Filesystem

	builderOptions *BuilderOptions

	stateId          objects.MAC
	flushTick        *time.Ticker
	flushEnd         chan bool
	flushEnded       chan error
	flushTerminating atomic.Bool

	Header  *header.Header
	emitter *events.Emitter

	lockReleaser chan bool
}

func (snap *Builder) Emitter(workflow string) *events.Emitter {
	return snap.appContext.Events().NewSnapshotEmitter(snap.repository.Configuration().RepositoryID, snap.Header.Identifier, workflow)
}

func newBuilder(appContext *kcontext.KContext, identifier objects.MAC, builderOptions *BuilderOptions) (*Builder, error) {
	scanCache, err := appContext.GetCache().Scan(identifier)
	if err != nil {
		return nil, err
	}

	snap := &Builder{
		appContext: appContext,

		scanCache:  scanCache,
		deltaCache: scanCache,

		builderOptions: builderOptions,
		Header:         header.NewHeader("default", identifier),
		stateId:        identifier,
		flushEnd:       make(chan bool),
		flushEnded:     make(chan error),
	}

	snap.Header.SetContext("Hostname", appContext.Hostname)
	snap.Header.SetContext("Username", appContext.Username)
	snap.Header.SetContext("OperatingSystem", appContext.OperatingSystem)
	snap.Header.SetContext("MachineID", appContext.MachineID)
	snap.Header.SetContext("CommandLine", appContext.CommandLine)
	snap.Header.SetContext("ProcessID", fmt.Sprintf("%d", appContext.ProcessID))
	snap.Header.SetContext("Architecture", appContext.Architecture)
	snap.Header.SetContext("NumCPU", fmt.Sprintf("%d", runtime.NumCPU()))
	snap.Header.SetContext("MaxProcs", fmt.Sprintf("%d", runtime.GOMAXPROCS(0)))
	snap.Header.SetContext("Client", appContext.Client)

	if !builderOptions.NoCheckpoint {
		snap.flushTick = time.NewTicker(1 * time.Hour)
		go snap.flushDeltaState()
	}

	if !builderOptions.ForcedTimestamp.IsZero() {
		if builderOptions.ForcedTimestamp.Before(time.Now()) {
			snap.Header.Timestamp = builderOptions.ForcedTimestamp.UTC()
		}
	}

	snap.Header.Tags = append(snap.Header.Tags, builderOptions.Tags...)
	snap.Header.Name = builderOptions.Name

	return snap, nil
}

func Create(repo *repository.Repository, packingStrategy repository.RepositoryType, packfileTmpDir string, snapId objects.MAC, builderOptions *BuilderOptions) (*Builder, error) {
	identifier := snapId
	if identifier == objects.NilMac {
		identifier = objects.RandomMAC()
	}

	snap, err := newBuilder(repo.AppContext(), identifier, builderOptions)
	if err != nil {
		return nil, err
	}
	snap.repository = repo.NewRepositoryWriter(snap.scanCache, snap.Header.Identifier, packingStrategy, packfileTmpDir)
	snap.emitter = snap.Emitter("backup")

	done, err := snap.Lock()
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil, err
	}
	snap.lockReleaser = done

	repo.Logger().Trace("snapshot", "%x: Create()", snap.Header.GetIndexShortID())

	return snap, nil
}

func (snap *Builder) WithVFSCache(vfsCache *vfs.Filesystem) {
	snap.vfsCache = vfsCache
}

func CreateWithRepositoryWriter(repo *repository.RepositoryWriter, builderOptions *BuilderOptions) (*Builder, error) {
	identifier := objects.RandomMAC()

	snap, err := newBuilder(repo.AppContext(), identifier, builderOptions)
	if err != nil {
		return nil, err
	}
	snap.repository = repo
	snap.emitter = snap.Emitter("backup")

	done, err := snap.Lock()
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil, err
	}
	snap.lockReleaser = done

	repo.Logger().Trace("snapshot", "%x: Create()", snap.Header.GetIndexShortID())

	return snap, nil
}

func (src *Snapshot) Fork(builderOptions *BuilderOptions) (*Builder, error) {

	identifier := objects.RandomMAC()

	location, err := src.repository.Location()
	if err != nil {
		return nil, err
	}

	var packingStrategy repository.RepositoryType
	if strings.HasPrefix(location, "ptar:") {
		packingStrategy = repository.PtarType
	} else {
		packingStrategy = repository.DefaultType
	}

	snap, err := newBuilder(src.AppContext(), identifier, builderOptions)
	if err != nil {
		return nil, err
	}

	snap.Header.Timestamp = time.Now()
	snap.Header.Name = src.Header.Name
	snap.Header.Category = src.Header.Category
	snap.Header.Environment = src.Header.Environment
	snap.Header.Perimeter = src.Header.Perimeter
	snap.Header.Job = src.Header.Job
	snap.Header.Replicas = src.Header.Replicas

	snap.Header.Classifications = make([]header.Classification, len(src.Header.Classifications))
	copy(snap.Header.Classifications, src.Header.Classifications)

	snap.Header.Tags = make([]string, len(src.Header.Tags))
	copy(snap.Header.Tags, src.Header.Tags)

	snap.Header.Context = make([]header.KeyValue, len(src.Header.Context))

	snap.Header.Sources = make([]header.Source, len(src.Header.Sources))
	copy(snap.Header.Sources, src.Header.Sources)

	snap.repository = src.repository.NewRepositoryWriter(snap.scanCache, snap.Header.Identifier, packingStrategy, "")
	snap.emitter = snap.Emitter("backup")

	done, err := snap.Lock()
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil, err
	}
	snap.lockReleaser = done

	src.repository.Logger().Trace("snapshot", "%x: Fork()", snap.Header.GetIndexShortID())

	return snap, nil
}

func (snap *Builder) Repository() *repository.Repository {
	return snap.repository.Repository
}

func (snap *Builder) Close() error {
	snap.Logger().Trace("snapshotBuilder", "%x: Close(): %x", snap.Header.Identifier, snap.Header.GetIndexShortID())
	defer snap.emitter.Close()

	snap.Unlock()

	if snap.scanCache != nil {
		return snap.scanCache.Close()
	}

	return nil
}

func (snap *Builder) Logger() *logging.Logger {
	return snap.appContext.GetLogger()
}

func (snap *Builder) AppContext() *kcontext.KContext {
	return snap.repository.AppContext()
}

func (snap *Builder) Lock() (chan bool, error) {
	lockless, _ := strconv.ParseBool(os.Getenv("PLAKAR_LOCKLESS"))
	lockDone := make(chan bool)
	if lockless {
		return lockDone, nil
	}

	lock := repository.NewSharedLock(snap.appContext.Hostname)

	buffer := &bytes.Buffer{}
	err := lock.SerializeToStream(buffer)
	if err != nil {
		return nil, err
	}

	_, err = snap.repository.PutLock(snap.Header.Identifier, buffer)
	if err != nil {
		return nil, err
	}

	// We installed the lock, now let's see if there is a conflicting exclusive lock or not.
	locksID, err := snap.repository.GetLocks()
	if err != nil {
		// We still need to delete it, and we need to do so manually.
		snap.repository.DeleteLock(snap.Header.Identifier)
		return nil, err
	}

	for _, lockID := range locksID {
		rd, err := snap.repository.GetLock(lockID)
		if err != nil {
			snap.repository.DeleteLock(snap.Header.Identifier)
			return nil, err
		}

		lock, err := repository.NewLockFromStream(rd)
		rd.Close()
		if err != nil {
			snap.repository.DeleteLock(snap.Header.Identifier)
			return nil, err
		}

		/* Kick out stale locks */
		if lock.IsStale() {
			err := snap.repository.DeleteLock(lockID)
			if err != nil {
				snap.repository.DeleteLock(snap.Header.Identifier)
				return nil, err
			}
		}

		// There is an exclusive lock in place, we need to abort.
		if lock.Exclusive {
			err := snap.repository.DeleteLock(snap.Header.Identifier)
			if err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("Can't take repository lock, it's already locked by maintenance.")
		}
	}

	// The following bit is a "ping" mechanism, Lock() is a bit badly named at this point,
	// we are just refreshing the existing lock so that the watchdog doesn't removes us.
	go func() {
		for {
			select {
			case <-lockDone:
				snap.repository.DeleteLock(snap.Header.Identifier)
				return
			case <-time.After(repository.LOCK_REFRESH_RATE):
				lock := repository.NewSharedLock(snap.appContext.Hostname)

				buffer := &bytes.Buffer{}

				// We ignore errors here on purpose, it's tough to handle them
				// correctly, and if they happen we will be ripped by the
				// watchdog anyway.
				lock.SerializeToStream(buffer)
				snap.repository.PutLock(snap.Header.Identifier, buffer)
			}
		}
	}()

	return lockDone, nil
}

func (snap *Builder) Unlock() {
	close(snap.lockReleaser)
}

func (snap *Builder) flushDeltaState() {
	for {
		select {
		case <-snap.appContext.Done():
			return
		case <-snap.flushEnd:
			// End of backup we push the last and final State.
			err := snap.repository.CommitTransaction(snap.stateId)
			if err != nil {
				snap.Logger().Warn("Failed to push the final state to the repository %s", err)
			}

			// See below
			if snap.deltaCache != snap.scanCache {
				snap.deltaCache.Close()
			}

			snap.flushEnded <- err
			close(snap.flushEnded)
			return
		case <-snap.flushTick.C:
			// In case the previous Flushing operation was too long and the
			// ticker got a chance to queue another tick we might end up here
			// rather than in flushEnd so just skip this and go to the final
			// state push.
			if snap.flushTerminating.Load() {
				continue
			}

			// New Delta
			oldCache := snap.deltaCache
			oldStateId := snap.stateId

			identifier := objects.RandomMAC()
			newDeltaCache, err := snap.repository.AppContext().GetCache().Scan(identifier)
			if err != nil {
				snap.appContext.Cancel(fmt.Errorf("state flusher: failed to open a new cache %w", err))
				return
			}

			snap.stateId = identifier
			snap.deltaCache = newDeltaCache

			// Now that the backup is free to progress we can serialize and push
			// the resulting statefile to the repo.
			err = snap.repository.RotateTransaction(snap.deltaCache, oldStateId, snap.stateId)
			if err != nil {
				snap.appContext.Cancel(fmt.Errorf("state flusher: failed to rotate state's transaction %w", err))
				return
			}

			// XXX: Pass down the path to the delta state db.
			if snap.builderOptions.StateRefresher != nil {
				if err := snap.builderOptions.StateRefresher(); err != nil {
					snap.appContext.Cancel(fmt.Errorf("state flusher: failed to merge the previous delta state inside the local state %w", err))
					return
				}
			} else {
				if err := snap.repository.MergeLocalStateWith(oldStateId, oldCache); err != nil {
					snap.appContext.Cancel(fmt.Errorf("state flusher: failed to merge the previous delta state inside the local state %w", err))
					return
				}
			}

			snap.repository.RemoveTransaction(oldStateId)

			// The first cache is always the scanCache, only in this function we
			// allocate a new and different one, so when we first hit this function
			// do not close the deltaCache, as it'll be closed at the end of the
			// backup because it's used by other parts of the code.
			if oldCache != snap.scanCache {
				oldCache.Close()
			}
		}
	}
}

func (snap *Builder) PutSnapshot() ([]byte, error) {
	// First thing is to stop the ticker, as we don't want any concurrent flushes to run.
	// Maybe this could be stopped earlier.
	if snap.flushTick != nil {
		snap.flushTerminating.Store(true)
		snap.flushTick.Stop()
	}

	serializedHdr, err := snap.Header.Serialize()
	if err != nil {
		return nil, err
	}

	if kp := snap.appContext.Keypair; kp != nil {
		serializedHdrMAC := snap.repository.ComputeMAC(serializedHdr)
		signature := kp.Sign(serializedHdrMAC[:])
		if err := snap.repository.PutBlob(resources.RT_SIGNATURE, snap.Header.Identifier, signature); err != nil {
			return nil, err
		}
	}

	if err := snap.repository.PutBlob(resources.RT_SNAPSHOT, snap.Header.Identifier, serializedHdr); err != nil {
		return nil, err
	}

	return serializedHdr, nil
}

func (snap *Builder) Commit() error {
	snap.emitter.Info("snapshot.commit.start", map[string]any{})
	defer snap.emitter.Info("snapshot.commit.end", map[string]any{})

	var serializedHdr []byte
	var err error
	if serializedHdr, err = snap.PutSnapshot(); err != nil {
		return err
	}

	snap.repository.PackerManager.Wait()

	// We are done with packfiles we can flush the last state, either through
	// the flusher, or manually here.
	if snap.flushTick != nil {
		snap.flushEnd <- true
		close(snap.flushEnd)
		err = <-snap.flushEnded
	} else {
		err = snap.repository.CommitTransaction(snap.Header.Identifier)
	}
	if err != nil {
		snap.Logger().Warn("Failed to push the state to the repository %s", err)
		return err
	}

	cache, err := snap.appContext.GetCache().Repository(snap.repository.Configuration().RepositoryID)
	if err == nil {
		_ = cache.PutSnapshot(snap.Header.Identifier, serializedHdr)
	}

	totalSize := uint64(0)
	totalErrors := uint64(0)
	for _, source := range snap.Header.Sources {
		totalSize += source.Summary.Directory.Size + source.Summary.Below.Size
		totalErrors += source.Summary.Directory.Errors + source.Summary.Below.Errors
	}

	rBytes := snap.repository.RBytes()
	wBytes := snap.repository.WBytes()

	target, err := snap.repository.Location()
	if err != nil {
		return err
	}

	snap.emitter.Result(target, totalSize, totalErrors, snap.Header.Duration, rBytes, wBytes)

	snap.Logger().Trace("snapshot", "%x: Commit()", snap.Header.GetIndexShortID())
	return nil
}
