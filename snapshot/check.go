package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"hash"

	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"golang.org/x/sync/errgroup"
)

type CheckOptions struct {
	MaxConcurrency uint64
	FastCheck      bool
}

type checkContext struct {
	emitter *events.Emitter
}

var (
	ErrRootCorrupted   = errors.New("root corrupted")
	ErrObjectMissing   = errors.New("object is missing")
	ErrObjectCorrupted = errors.New("object corrupted")
	ErrChunkMissing    = errors.New("chunk is missing")
	ErrChunkCorrupted  = errors.New("chunk corrupted")
)

func checkChunk(snap *Snapshot, chunk *objects.Chunk, hasher hash.Hash, fast bool, checkCtx *checkContext) error {
	chunkStatus, err := snap.checkCache.GetChunkStatus(chunk.ContentMAC)
	if err != nil {
		return err
	}

	// if chunkStatus is nil, we've never seen this chunk and we
	// have to process it.  It is zero if it's fine, or an error
	// otherwise.
	var seen bool
	if chunkStatus != nil {
		if len(chunkStatus) != 0 {
			return fmt.Errorf("%s", string(chunkStatus))
		}
		if fast {
			return nil
		}
		seen = true
	}

	emitter := checkCtx.emitter
	emitter.Emit("snapshot.check.chunk", map[string]any{
		"snapshot_id": snap.Header.Identifier,
		"content_mac": chunk.ContentMAC,
	})

	if fast {
		if !snap.repository.BlobExists(resources.RT_CHUNK, chunk.ContentMAC) {
			emitter.Emit("snapshot.check.chunk.missing", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"content_mac": chunk.ContentMAC,
				"error":       ErrChunkMissing.Error(),
			})
			snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(ErrChunkMissing.Error()))
			return ErrChunkMissing
		}

		emitter.Emit("snapshot.check.chunk.ok", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"content_mac": chunk.ContentMAC,
		})
		snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(""))
		return nil
	}

	data, err := snap.repository.GetBlobBytes(resources.RT_CHUNK, chunk.ContentMAC)
	if err != nil {
		emitter.Emit("snapshot.check.chunk.missing", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"content_mac": chunk.ContentMAC,
			"error":       ErrChunkMissing.Error(),
		})
		snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(ErrChunkMissing.Error()))
		return ErrChunkMissing
	}

	hasher.Write(data)
	if seen {
		return nil
	}

	mac := snap.repository.ComputeMAC(data)
	if !bytes.Equal(mac[:], chunk.ContentMAC[:]) {
		emitter.Emit("snapshot.check.chunk.corrupted", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"content_mac": chunk.ContentMAC,
			"error":       ErrChunkCorrupted.Error(),
		})
		snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(ErrChunkCorrupted.Error()))
		return ErrChunkCorrupted
	}

	emitter.Emit("snapshot.check.chunk.ok", map[string]any{
		"snapshot_id": snap.Header.Identifier,
		"content_mac": chunk.ContentMAC,
	})
	snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(""))
	return nil
}

func checkObject(snap *Snapshot, fileEntry *vfs.Entry, fast bool, checkCtx *checkContext) error {
	objectStatus, err := snap.checkCache.GetObjectStatus(fileEntry.Object)
	if err != nil {
		return err
	}

	// if objectStatus is nil, we've never seen this object and we
	// have to process it.  It is zero if it's fine, or an error
	// otherwise.
	if objectStatus != nil {
		if len(objectStatus) != 0 {
			return fmt.Errorf("%s", string(objectStatus))
		}
		return nil
	}

	emmiter := checkCtx.emitter

	object, err := snap.LookupObject(fileEntry.Object)
	if err != nil {
		emmiter.Emit("snapshot.check.object.missing", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"object_mac":  fileEntry.Object,
			"error":       err.Error(),
		})
		snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(ErrObjectMissing.Error()))
		return ErrObjectMissing
	}

	hasher := snap.repository.GetMACHasher()
	emmiter.Emit("snapshot.check.object", map[string]any{
		"snapshot_id": snap.Header.Identifier,
		"content_mac": object.ContentMAC,
	})

	var failed bool
	for i := range object.Chunks {
		if err := checkChunk(snap, &object.Chunks[i], hasher, fast, checkCtx); err != nil {
			failed = true
		}
	}

	if failed {
		emmiter.Emit("snapshot.check.object.corrupted", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"content_mac": object.ContentMAC,
			"error":       ErrObjectCorrupted.Error(),
		})
		snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(ErrObjectCorrupted.Error()))
		return ErrObjectCorrupted
	}

	if !fast {
		if !bytes.Equal(hasher.Sum(nil), object.ContentMAC[:]) {
			emmiter.Emit("snapshot.check.object.corrupted", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"content_mac": object.ContentMAC,
				"error":       ErrObjectCorrupted.Error(),
			})
			snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(ErrObjectCorrupted.Error()))
			return ErrObjectCorrupted
		}
	}

	emmiter.Emit("snapshot.check.object.ok", map[string]any{
		"snapshot_id": snap.Header.Identifier,
		"content_mac": object.ContentMAC,
	})
	snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(""))
	return nil
}

func checkEntry(snap *Snapshot, opts *CheckOptions, entrypath string, e *vfs.Entry, wg *errgroup.Group, checkCtx *checkContext) error {
	entryMAC := e.MAC
	entryStatus, err := snap.checkCache.GetVFSEntryStatus(entryMAC)
	if err != nil {
		return err
	}
	if entryStatus != nil {
		if len(entryStatus) != 0 {
			return fmt.Errorf("%s", string(entryStatus))
		}
		return nil
	}

	emitter := checkCtx.emitter
	emitter.Emit("snapshot.check.path", map[string]any{
		"snapshot_id": snap.Header.Identifier,
		"path":        entrypath,
	})

	if e.Stat().Mode().IsDir() {
		emitter.Emit("snapshot.check.directory", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"path":        entrypath,
		})
		emitter.Emit("snapshot.check.directory.ok", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"path":        entrypath,
		})
		snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(""))
		return nil
	}

	if !e.Stat().Mode().IsRegular() {
		snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(""))
		return nil
	}

	emitter.Emit("snapshot.check.file", map[string]any{
		"snapshot_id": snap.Header.Identifier,
		"path":        entrypath,
	})

	wg.Go(func() error {
		err := checkObject(snap, e, opts.FastCheck, checkCtx)
		if err != nil {
			emitter.Emit("snapshot.check.file.corrupted", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
				"error":       err.Error(),
			})
			snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(err.Error()))
			return err
		}

		emitter.Emit("snapshot.check.file.ok", map[string]any{
			"snapshot_id": snap.Header.Identifier,
			"path":        entrypath,
		})
		snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(""))
		return nil
	})
	return nil
}

func (snap *Snapshot) Check(pathname string, opts *CheckOptions) error {
	emitter := snap.AppContext().Events().Emitter()
	emitter.Emit("snapshot.check.start", map[string]any{
		"snapshot_id": snap.Header.Identifier,
	})
	defer emitter.Emit("snapshot.check.done", map[string]any{
		"snapshot_id": snap.Header.Identifier,
	})

	checkCtx := &checkContext{
		emitter: emitter,
	}

	vfsStatus, err := snap.checkCache.GetVFSStatus(snap.Header.GetSource(0).VFS.Root)
	if err != nil {
		return err
	}

	// if vfsStatus is nil, we've never seen this vfs and we have
	// to process it.  It is zero if it's fine, or an error
	// otherwise.
	if vfsStatus != nil {
		if len(vfsStatus) != 0 {
			return fmt.Errorf("%s", string(vfsStatus))
		}
		return nil
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.AppContext().MaxConcurrency)
	}

	wg := new(errgroup.Group)
	wg.SetLimit(int(maxConcurrency))

	var failed bool
	err = fs.WalkDir(pathname, func(entrypath string, e *vfs.Entry, err error) error {
		if err != nil {
			emitter.Emit("snapshot.check.path.error", map[string]any{
				"snapshot_id": snap.Header.Identifier,
				"path":        entrypath,
				"error":       err.Error(),
			})
			return err
		}

		if err := snap.AppContext().Err(); err != nil {
			return err
		}

		if err := checkEntry(snap, opts, entrypath, e, wg, checkCtx); err != nil {
			// don't stop at the first error; we need to
			// process all the entries to report all the
			// findings.
			failed = true
		}

		return nil
	})
	if err != nil {
		snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root, []byte(err.Error()))
		wg.Wait()
		return err
	}
	if err := wg.Wait(); err != nil {
		snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root, []byte(err.Error()))
		return err
	}
	if failed {
		snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root,
			[]byte(ErrRootCorrupted.Error()))
		return ErrRootCorrupted
	}

	snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root, []byte(""))
	return nil
}
