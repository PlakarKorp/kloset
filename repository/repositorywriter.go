package repository

import (
	"io"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
)

type RepositoryWriter struct {
	*Repository

	transactionMtx sync.RWMutex
	deltaState     map[objects.MAC]*state.LocalState

	PackerManager  packer.PackerManagerInt
	currentStateID objects.MAC
}

type RepositoryType int

const (
	DefaultType RepositoryType = iota
	PtarType                   = iota
)

// If packfileTmpDir is empty, we default to memory based packfiles. It's an
// upper layer responsibility to provide a sane default for this.
func (r *Repository) newRepositoryWriter(cache *caching.ScanCache, id objects.MAC, typ RepositoryType, packfileTmpDir string) *RepositoryWriter {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "NewRepositoryWriter(): %s", time.Since(t0))
	}()

	rw := RepositoryWriter{
		Repository: r,

		deltaState:     make(map[objects.MAC]*state.LocalState),
		currentStateID: id,
	}
	rw.deltaState[rw.currentStateID] = r.state.Derive(cache)

	switch typ {
	case PtarType:
		rw.PackerManager, _ = packer.NewPlatarPackerManager(rw.AppContext(), &rw.configuration, rw.encode, rw.GetMACHasher, rw.PutPtarPackfile)
	default:
		if packfileTmpDir == "" {
			rw.PackerManager = packer.NewSeqPackerManager(rw.AppContext(), &rw.configuration, rw.encode, packfile.NewPackfileInMemory, rw.GetMACHasher, rw.PutPackfile)
		} else {
			ondiskPackfileCtor := func(hf packfile.HashFactory) (packfile.Packfile, error) {
				return packfile.NewPackfileOnDisk(packfileTmpDir, hf)
			}

			rw.PackerManager = packer.NewSeqPackerManager(rw.AppContext(), &rw.configuration, rw.encode, ondiskPackfileCtor, rw.GetMACHasher, rw.PutPackfile)
		}
	}

	// XXX: Better placement for this
	go rw.PackerManager.Run()

	return &rw
}

func (r *RepositoryWriter) RotateTransaction(newCache *caching.ScanCache, oldStateID, newStateID objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repositorywriter", "FlushTransaction(): %s", time.Since(t0))
	}()

	r.transactionMtx.Lock()
	oldState := r.deltaState[oldStateID]
	r.deltaState[newStateID] = r.state.Derive(newCache)
	r.currentStateID = newStateID
	r.transactionMtx.Unlock()

	return r.internalCommit(oldState, oldStateID)
}

func (r *RepositoryWriter) RemoveTransaction(stateID objects.MAC) {
	r.transactionMtx.Lock()
	delete(r.deltaState, stateID)
	r.transactionMtx.Unlock()
}

func (r *RepositoryWriter) CommitTransaction(oldStateID objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "CommitTransaction(): %s", time.Since(t0))
	}()

	err := r.internalCommit(r.deltaState[oldStateID], oldStateID)
	r.transactionMtx.Lock()
	r.deltaState = nil
	r.currentStateID = objects.NilMac
	r.transactionMtx.Unlock()

	return err
}

func (r *RepositoryWriter) MergeLocalStateWith(stateID objects.MAC, oldCache *caching.ScanCache) error {
	return r.state.MergeStateFromCache(stateID, oldCache)
}

func (r *RepositoryWriter) internalCommit(state *state.LocalState, id objects.MAC) error {
	pr, pw := io.Pipe()

	/* By using a pipe and a goroutine we bound the max size in memory. */
	go func() {
		defer pw.Close()

		if err := state.SerializeToStream(pw); err != nil {
			pw.CloseWithError(err)
		}
	}()

	return r.PutState(id, pr)
}

// MUST be called with `r.transactionMtx` at least read locked.
func (r *RepositoryWriter) currentDeltaState() *state.LocalState {
	// XXX: Do we want debug assertions here?
	return r.deltaState[r.currentStateID]
}

func (r *RepositoryWriter) BlobExists(Type resources.Type, mac objects.MAC) bool {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repositorywriter", "BlobExists(%s, %x): %s", Type, mac, time.Since(t0))
	}()

	ok, _ := r.PackerManager.Exists(Type, mac)
	if ok {
		return true
	}

	r.transactionMtx.RLock()
	for _, ds := range r.deltaState {
		if ds.BlobExists(Type, mac) {
			r.transactionMtx.RUnlock()
			return true
		}
	}
	r.transactionMtx.RUnlock()

	return r.state.BlobExists(Type, mac)
}

func (r *RepositoryWriter) PutBlobIfNotExistsWithHint(hint int, Type resources.Type, mac objects.MAC, data []byte) error {
	if r.BlobExists(Type, mac) {
		return nil
	}
	return r.PutBlobWithHint(hint, Type, mac, data)
}

func (r *RepositoryWriter) PutBlobWithHint(hint int, Type resources.Type, mac objects.MAC, data []byte) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repositorywriter", "PutBlobWithHint(%d, %s, %x): %s", hint, Type, mac, time.Since(t0))
	}()

	if ok, err := r.PackerManager.InsertIfNotPresent(Type, mac); err != nil {
		return err
	} else if ok {
		return nil
	}

	return r.PackerManager.Put(hint, Type, mac, data)
}

func (r *RepositoryWriter) PutBlobIfNotExists(Type resources.Type, mac objects.MAC, data []byte) error {
	if r.BlobExists(Type, mac) {
		return nil
	}
	return r.PutBlob(Type, mac, data)
}

func (r *RepositoryWriter) PutBlob(Type resources.Type, mac objects.MAC, data []byte) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repositorywriter", "PutBlob(%s, %x): %s", Type, mac, time.Since(t0))
	}()

	if ok, err := r.PackerManager.InsertIfNotPresent(Type, mac); err != nil {
		return err
	} else if ok {
		return nil
	}

	return r.PackerManager.Put(-1, Type, mac, data)
}

func (r *RepositoryWriter) DeleteStateResource(Type resources.Type, mac objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "DeleteStateResource(%s, %x): %s", Type.String(), mac, time.Since(t0))
	}()

	r.transactionMtx.RLock()
	defer r.transactionMtx.RUnlock()
	return r.currentDeltaState().DeleteResource(Type, mac)
}

func (r *RepositoryWriter) PutPackfile(pfile packfile.Packfile) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutPackfile(%x): %s", r.currentStateID, time.Since(t0))
	}()

	serializedPackfile, mac, err := pfile.Serialize(r.encode)
	if err != nil {
		return err
	}

	rd, err := storage.Serialize(r.GetMACHasher(), resources.RT_PACKFILE, versioning.GetCurrentVersion(resources.RT_PACKFILE), serializedPackfile)
	if err != nil {
		return err
	}

	span := r.ioStats.GetWriteSpan()
	nbytes, err := r.store.Put(r.appContext, storage.StorageResourcePackfile, mac, rd)

	span.Add(nbytes)
	if err != nil {
		return err
	}

	r.transactionMtx.RLock()
	defer r.transactionMtx.RUnlock()

	db := r.currentDeltaState().NewBatch()

	for _, blob := range pfile.Entries() {
		delta := &state.DeltaEntry{
			Type:    blob.Type,
			Version: blob.Version,
			Blob:    blob.MAC,
			Location: state.Location{
				Packfile: mac,
				Offset:   blob.Offset,
				Length:   blob.Length,
			},
		}

		serialized := delta.ToBytes()
		if err := db.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, serialized); err != nil {
			return err
		}

	}

	db.Commit()

	return r.currentDeltaState().PutPackfile(r.currentStateID, mac)
}

func (r *RepositoryWriter) PutPtarPackfile(packfile *packer.PackWriter) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutPtarPackfile(%x): %s", r.currentStateID, time.Since(t0))
	}()

	mac := objects.RandomMAC()

	// This is impossible with this format, the mac of the packfile has to be random. Shouldn't be a problem.
	//	mac := r.ComputeMAC(serializedPackfile)

	rd, err := storage.Serialize(r.GetMACHasher(), resources.RT_PACKFILE, versioning.GetCurrentVersion(resources.RT_PACKFILE), packfile.Reader)
	if err != nil {
		return err
	}

	span := r.ioStats.GetWriteSpan()
	nbytes, err := r.store.Put(r.appContext, storage.StorageResourcePackfile, mac, rd)
	span.Add(nbytes)
	if err != nil {
		return err
	}

	r.transactionMtx.RLock()
	defer r.transactionMtx.RUnlock()
	for blobData := range packfile.Index.GetIndexesBlob() {
		blob, err := packer.NewBlobFromBytes(blobData)
		if err != nil {
			return err
		}

		delta := &state.DeltaEntry{
			Type:    blob.Type,
			Version: blob.Version,
			Blob:    blob.MAC,
			Location: state.Location{
				Packfile: mac,
				Offset:   blob.Offset,
				Length:   blob.Length,
			},
		}

		if err := r.currentDeltaState().PutDelta(delta); err != nil {
			return err
		}
	}

	return r.currentDeltaState().PutPackfile(r.currentStateID, mac)
}
