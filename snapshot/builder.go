package snapshot

import (
	"fmt"
	"runtime"
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
	"github.com/PlakarKorp/kloset/throttle"
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

	lock *repository.LockedRepository

	beginTime time.Time

	noSkipSelf bool

	throttler *throttle.Throttler
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

		beginTime: time.Now(),
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
	snap.Header.Category = builderOptions.Category
	snap.Header.Environment = builderOptions.Environment
	snap.Header.Perimeter = builderOptions.Perimeter
	snap.Header.Job = builderOptions.Job
	snap.Header.Dataset = builderOptions.Dataset
	snap.Header.DataClasses = append(snap.Header.DataClasses, builderOptions.DataClasses...)

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

	lock, err := snap.repository.Lock(snap.Header.Identifier)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil, err
	}
	snap.lock = lock

	repo.Logger().Trace("snapshot", "%x: Create()", snap.Header.GetIndexShortID())

	return snap, nil
}

func (snap *Builder) WithVFSCache(vfsCache *vfs.Filesystem) {
	snap.vfsCache = vfsCache
}

func CreateWithRepositoryWriter(repo *repository.RepositoryWriter, builderOptions *BuilderOptions, snapId objects.MAC) (*Builder, error) {
	identifier := snapId
	if identifier == objects.NilMac {
		identifier = objects.RandomMAC()
	}

	snap, err := newBuilder(repo.AppContext(), identifier, builderOptions)
	if err != nil {
		return nil, err
	}
	snap.repository = repo
	snap.emitter = snap.Emitter("backup")

	lock, err := snap.repository.Lock(snap.Header.Identifier)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil, err
	}
	snap.lock = lock

	repo.Logger().Trace("snapshot", "%x: Create()", snap.Header.GetIndexShortID())

	return snap, nil
}

func (src *Snapshot) Fork(builderOptions *BuilderOptions) (*Builder, error) {
	identifier := objects.RandomMAC()

	var packingStrategy repository.RepositoryType
	if src.repository.Type() == "ptar" {
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

	lock, err := snap.repository.Lock(snap.Header.Identifier)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil, err
	}

	snap.lock = lock

	src.repository.Logger().Trace("snapshot", "%x: Fork()", snap.Header.GetIndexShortID())

	return snap, nil
}

func (snap *Builder) Repository() *repository.Repository {
	return snap.repository.Repository
}

func (snap *Builder) Close() error {
	snap.Logger().Trace("snapshotBuilder", "%x: Close(): %x", snap.Header.Identifier, snap.Header.GetIndexShortID())
	defer snap.emitter.Close()

	if err := snap.lock.Unlock(); err != nil {
		snap.Logger().Warn("failed to release lock %x (%s), you might need to remove it manually", snap.Header.Identifier, err)
	}

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

// commitState pushes the final state, which is what makes the snapshot
// visible. Refuse to if our lock is gone or stale: maintenance may have run
// under us and removed packfiles this snapshot references.
func (snap *Builder) commitState(stateID objects.MAC) error {
	if !snap.lock.Lockless {
		lock, err := snap.lock.GetLock(snap.AppContext())
		if err != nil {
			return fmt.Errorf("snapshot not committed, repository lock lost: %w", err)
		}

		if lock.IsStale() {
			return fmt.Errorf("snapshot not committed, repository lock went stale")
		}
	}

	return snap.repository.CommitTransaction(stateID)
}

func (snap *Builder) flushDeltaState() {
	for {
		select {
		case <-snap.flushEnd:
			// End of backup we push the last and final State.
			err := snap.commitState(snap.stateId)
			if err != nil {
				snap.Logger().Warn("Failed to push the final state to the repository %s", err)
			}

			if snap.builderOptions.StateRefresher != nil {
				if err := snap.builderOptions.StateRefresher(snap.stateId, true); err != nil {
					snap.Logger().Warn("Failed to reload the final state to the repository %s", err)
				}
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

			if snap.builderOptions.StateRefresher != nil {
				if err := snap.builderOptions.StateRefresher(oldStateId, false); err != nil {
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
		if err := snap.repository.PutBlob(resources.RT_SIGNATURE, snap.Header.Identifier, signature, true); err != nil {
			return nil, err
		}
	}

	if err := snap.repository.PutBlob(resources.RT_SNAPSHOT, snap.Header.Identifier, serializedHdr, true); err != nil {
		return nil, err
	}

	return serializedHdr, nil
}

func (snap *Builder) Commit() error {
	snap.emitter.Info("snapshot.commit.start", map[string]any{})
	defer snap.emitter.Info("snapshot.commit.end", map[string]any{})

	snap.Header.Duration = time.Since(snap.beginTime)

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
		err = snap.commitState(snap.Header.Identifier)
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
	target := snap.repository.Origin()

	snap.emitter.Result(target, totalSize, totalErrors, snap.Header.Duration, rBytes, wBytes)

	snap.Logger().Trace("snapshot", "%x: Commit()", snap.Header.GetIndexShortID())
	return nil
}
