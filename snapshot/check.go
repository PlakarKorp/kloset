package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"hash"
	"sync/atomic"
	"time"

	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"golang.org/x/sync/errgroup"
)

type CheckOptions struct {
	FastCheck bool
}

type checkContext struct {
	emitter *events.Emitter
	size    atomic.Uint64
	errors  atomic.Uint64
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
	emitter.Chunk(chunk.ContentMAC)

	if fast {
		if !snap.repository.BlobExists(resources.RT_CHUNK, chunk.ContentMAC) {
			emitter.ChunkError(chunk.ContentMAC, ErrChunkMissing)
			snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(ErrChunkMissing.Error()))
			return ErrChunkMissing
		}
		emitter.ChunkOk(chunk.ContentMAC)
		snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(""))
		return nil
	}

	data, err := snap.repository.GetBlobBytes(resources.RT_CHUNK, chunk.ContentMAC)
	if err != nil {
		emitter.ChunkError(chunk.ContentMAC, ErrChunkMissing)
		snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(ErrChunkMissing.Error()))
		return ErrChunkMissing
	}

	hasher.Write(data)
	if seen {
		return nil
	}

	mac := snap.repository.ComputeMAC(data)
	if !bytes.Equal(mac[:], chunk.ContentMAC[:]) {
		emitter.ChunkError(chunk.ContentMAC, ErrChunkCorrupted)
		snap.checkCache.PutChunkStatus(chunk.ContentMAC, []byte(ErrChunkCorrupted.Error()))
		return ErrChunkCorrupted
	}

	emitter.ChunkOk(chunk.ContentMAC)
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
		emmiter.ObjectError(fileEntry.Object, err)
		snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(ErrObjectMissing.Error()))
		return ErrObjectMissing
	}

	hasher := snap.repository.GetMACHasher()
	emmiter.Object(object.ContentMAC)

	var failed bool
	for i := range object.Chunks {
		if err := checkChunk(snap, &object.Chunks[i], hasher, fast, checkCtx); err != nil {
			failed = true
		}
	}

	if failed {
		emmiter.ObjectError(object.ContentMAC, ErrObjectCorrupted)
		snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(ErrObjectCorrupted.Error()))
		return ErrObjectCorrupted
	}

	if !fast {
		if !bytes.Equal(hasher.Sum(nil), object.ContentMAC[:]) {
			emmiter.ObjectError(object.ContentMAC, ErrObjectCorrupted)
			snap.checkCache.PutObjectStatus(fileEntry.Object, []byte(ErrObjectCorrupted.Error()))
			return ErrObjectCorrupted
		}
	}
	emmiter.ObjectOk(object.ContentMAC)
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
	emitter.Path(entrypath)

	if e.Stat().Mode().IsDir() {
		emitter.Directory(entrypath)
		emitter.DirectoryOk(entrypath)
		snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(""))
		return nil
	}

	if !e.Stat().Mode().IsRegular() {
		snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(""))
		return nil
	}

	emitter.File(entrypath)

	wg.Go(func() error {
		err := checkObject(snap, e, opts.FastCheck, checkCtx)
		if err != nil {
			emitter.FileError(entrypath, err)
			checkCtx.errors.Add(1)
			snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(err.Error()))
			return err
		}
		emitter.FileOk(entrypath)
		checkCtx.size.Add(uint64(e.Stat().Size()))
		snap.checkCache.PutVFSEntryStatus(entryMAC, []byte(""))
		return nil
	})
	return nil
}

func (snap *Snapshot) Check(pathname string, opts *CheckOptions) error {
	emitter := snap.Emitter("check")
	defer emitter.Close()

	t0 := time.Now()

	target, err := snap.repository.Location()
	if err != nil {
		return err
	}

	rBytesSaved, wBytesSaved := snap.repository.RBytes(), snap.repository.WBytes()

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
		emitter.Result(target, checkCtx.size.Load(), checkCtx.errors.Load(), time.Since(t0), rBytesSaved, wBytesSaved)
		return nil
	}

	fs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	wg := new(errgroup.Group)
	wg.SetLimit(int(snap.AppContext().MaxConcurrency))

	var failed bool
	err = fs.WalkDir(pathname, func(entrypath string, e *vfs.Entry, err error) error {
		if err != nil {
			emitter.PathError(entrypath, err)
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

	rBytes := snap.repository.RBytes() - rBytesSaved
	wBytes := snap.repository.WBytes() - wBytesSaved

	if err != nil {
		snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root, []byte(err.Error()))
		wg.Wait()

		emitter.Result(target, checkCtx.size.Load(), checkCtx.errors.Load(), time.Since(t0), rBytes, wBytes)
		return err
	}
	if err := wg.Wait(); err != nil {
		snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root, []byte(err.Error()))
		emitter.Result(target, checkCtx.size.Load(), checkCtx.errors.Load(), time.Since(t0), rBytes, wBytes)
		return err
	}
	if failed {
		snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root,
			[]byte(ErrRootCorrupted.Error()))
		emitter.Result(target, checkCtx.size.Load(), checkCtx.errors.Load(), time.Since(t0), rBytes, wBytes)
		return ErrRootCorrupted
	}

	snap.checkCache.PutVFSStatus(snap.Header.GetSource(0).VFS.Root, []byte(""))
	emitter.Result(target, checkCtx.size.Load(), checkCtx.errors.Load(), time.Since(t0), rBytes, wBytes)

	return nil
}
