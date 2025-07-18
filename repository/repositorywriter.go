package repository

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/storage"
	"github.com/PlakarKorp/kloset/versioning"
)

type RepositoryWriter struct {
	*Repository

	transactionMtx sync.RWMutex
	deltaState     *state.LocalState

	PackerManager  packer.PackerManagerInt
	currentStateID objects.MAC
}

type RepositoryType int

const (
	DefaultType RepositoryType = iota
	PtarType                   = iota
)

func (r *Repository) newRepositoryWriter(cache *caching.ScanCache, id objects.MAC, typ RepositoryType) *RepositoryWriter {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "NewRepositoryWriter(): %s", time.Since(t0))
	}()

	rw := RepositoryWriter{
		Repository:     r,
		deltaState:     r.state.Derive(cache),
		currentStateID: id,
	}

	switch typ {
	case PtarType:
		rw.PackerManager, _ = packer.NewPlatarPackerManager(rw.AppContext(), &rw.configuration, rw.encode, rw.GetMACHasher, rw.PutPtarPackfile)
	default:
		rw.PackerManager = packer.NewSeqPackerManager(rw.AppContext(), r.AppContext().MaxConcurrency, &rw.configuration, rw.encode, rw.GetMACHasher, rw.PutPackfile)
	}

	// XXX: Better placement for this
	go rw.PackerManager.Run()

	return &rw
}

func (r *RepositoryWriter) FlushTransaction(newCache *caching.ScanCache, id objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repositorywriter", "FlushTransaction(): %s", time.Since(t0))
	}()

	r.transactionMtx.Lock()
	oldState := r.deltaState
	r.deltaState = r.state.Derive(newCache)
	r.transactionMtx.Unlock()

	return r.internalCommit(oldState, id)
}

func (r *RepositoryWriter) CommitTransaction(id objects.MAC) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "CommitTransaction(): %s", time.Since(t0))
	}()

	err := r.internalCommit(r.deltaState, id)
	r.transactionMtx.Lock()
	r.deltaState = nil
	r.transactionMtx.Unlock()

	return err
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

	err := r.PutState(id, pr)
	if err != nil {
		return err
	}

	/* We are commiting the transaction, publish the new state to our local aggregated state. */
	return r.state.PutState(id)
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
		r.Logger().Trace("repositorywriter", "PutBlob(%s, %x): %s", Type, mac, time.Since(t0))
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
	if err := r.deltaState.DeleteResource(Type, mac); err != nil {
		return err
	}

	return r.state.DeleteResource(Type, mac)
}

func (r *RepositoryWriter) PutPackfile(pfile *packfile.PackFile) error {
	t0 := time.Now()
	defer func() {
		r.Logger().Trace("repository", "PutPackfile(%x): %s", r.currentStateID, time.Since(t0))
	}()

	serializedData, err := pfile.SerializeData()
	if err != nil {
		return fmt.Errorf("could not serialize pack file data %s", err.Error())
	}
	serializedIndex, err := pfile.SerializeIndex()
	if err != nil {
		return fmt.Errorf("could not serialize pack file index %s", err.Error())
	}
	serializedFooter, err := pfile.SerializeFooter()
	if err != nil {
		return fmt.Errorf("could not serialize pack file footer %s", err.Error())
	}

	encryptedIndex, err := r.encodeBuffer(serializedIndex)
	if err != nil {
		return err
	}

	encryptedFooter, err := r.encodeBuffer(serializedFooter)
	if err != nil {
		return err
	}

	serializedPackfile := append(serializedData, encryptedIndex...)
	serializedPackfile = append(serializedPackfile, encryptedFooter...)

	/* it is necessary to track the footer _encrypted_ length */
	encryptedFooterLength := make([]byte, 4)
	binary.LittleEndian.PutUint32(encryptedFooterLength, uint32(len(encryptedFooter)))
	serializedPackfile = append(serializedPackfile, encryptedFooterLength...)

	mac := r.ComputeMAC(serializedPackfile)

	rd, err := storage.Serialize(r.GetMACHasher(), resources.RT_PACKFILE, versioning.GetCurrentVersion(resources.RT_PACKFILE), bytes.NewBuffer(serializedPackfile))
	if err != nil {
		return err
	}

	nbytes, err := r.store.PutPackfile(mac, rd)
	r.wBytes.Add(nbytes)
	if err != nil {
		return err
	}

	r.transactionMtx.RLock()
	defer r.transactionMtx.RUnlock()
	for idx, blob := range pfile.Index {
		delta := &state.DeltaEntry{
			Type:    blob.Type,
			Version: pfile.Index[idx].Version,
			Blob:    blob.MAC,
			Location: state.Location{
				Packfile: mac,
				Offset:   pfile.Index[idx].Offset,
				Length:   pfile.Index[idx].Length,
			},
		}

		if err := r.deltaState.PutDelta(delta); err != nil {
			return err
		}

		if err := r.state.PutDelta(delta); err != nil {
			return err
		}
	}

	if err := r.deltaState.PutPackfile(r.currentStateID, mac); err != nil {
		return err
	}

	return r.state.PutPackfile(r.currentStateID, mac)
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

	nbytes, err := r.store.PutPackfile(mac, rd)
	r.wBytes.Add(nbytes)
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

		if err := r.deltaState.PutDelta(delta); err != nil {
			return err
		}

		if err := r.state.PutDelta(delta); err != nil {
			return err
		}
	}

	if err := r.deltaState.PutPackfile(r.currentStateID, mac); err != nil {
		return err
	}

	return r.state.PutPackfile(r.currentStateID, mac)
}
