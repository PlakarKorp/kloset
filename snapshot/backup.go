package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/scanlog"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/gabriel-vasile/mimetype"
	"golang.org/x/sync/errgroup"
)

type sourceIndexes struct {
	vfsidx     *btree.BTree[string, int, []byte]
	summaryidx *btree.BTree[string, int, []byte]
	erridx     *btree.BTree[string, int, objects.MAC]
	xattridx   *btree.BTree[string, int, objects.MAC]
	ctidx      *btree.BTree[string, int, objects.MAC]
	dirpackidx *btree.BTree[string, int, objects.MAC]
}

type sourceContext struct {
	builder *Builder
	source  *Source

	vfsCache *vfs.Filesystem

	vfsEntBatch *scanlog.ScanBatch
	vfsEntLock  sync.Mutex

	indexes *sourceIndexes

	scanLog *scanlog.ScanLog
}

type scanStats struct {
	nfiles  uint64
	ndirs   uint64
	nlinks  uint64
	nxattrs uint64
	size    uint64
}

type BuilderOptions struct {
	Name            string
	Tags            []string
	NoCheckpoint    bool
	NoCommit        bool
	NoXattr         bool
	ForcedTimestamp time.Time
	StateRefresher  func(objects.MAC, bool) error
}

var (
	ErrOutOfRange = errors.New("out of range")
)

func (sourceCtx *sourceContext) batchRecordEntry(kind scanlog.EntryKind, pathname string, serializedEntry []byte, serializedSummary []byte) error {
	sourceCtx.vfsEntLock.Lock()
	defer sourceCtx.vfsEntLock.Unlock()

	if kind == scanlog.KindDirectory {
		if err := sourceCtx.vfsEntBatch.PutDirectory(pathname, serializedEntry); err != nil {
			return err
		}
	} else {
		if err := sourceCtx.vfsEntBatch.PutFile(pathname, serializedEntry, serializedSummary); err != nil {
			return err
		}
	}

	if sourceCtx.vfsEntBatch.Count() >= 1000 {
		if err := sourceCtx.vfsEntBatch.Commit(); err != nil {
			return err
		}

		sourceCtx.vfsEntBatch = sourceCtx.scanLog.NewBatch()
	}

	return nil
}

func (sourceCtx *sourceContext) recordError(idx int, path string, err error) error {
	entry := vfs.NewErrorItem(path, err.Error())
	serialized, e := entry.ToBytes()
	if e != nil {
		return err
	}

	mac := sourceCtx.builder.repository.ComputeMAC(serialized)
	err = sourceCtx.builder.repository.PutBlobIfNotExistsWithHint(idx, resources.RT_ERROR_ENTRY, mac, serialized)
	if err != nil {
		return nil
	}
	return sourceCtx.scanLog.PutPathMAC(scanlog.KindError, path, mac)
}

func (sourceCtx *sourceContext) recordXattr(idx int, record *connectors.Record, objectMAC objects.MAC, size int64) error {
	xattr := vfs.NewXattr(record, objectMAC, size)
	serialized, err := xattr.ToBytes()
	if err != nil {
		return err
	}

	mac := sourceCtx.builder.repository.ComputeMAC(serialized)
	err = sourceCtx.builder.repository.PutBlobIfNotExistsWithHint(idx, resources.RT_XATTR_ENTRY, mac, serialized)
	if err != nil {
		return nil
	}
	return sourceCtx.scanLog.PutPathMAC(scanlog.KindXattr, record.Pathname, mac)

	//return sourceCtx.indexes.xattridx.Insert(xattr.ToPath(), serialized)
}

func (snapshot *Builder) skipExcludedPathname(sourceCtx *sourceContext, record *connectors.Record) bool {
	var isDir bool
	if record.Err == nil {
		isDir = record.FileInfo.IsDir()
	}

	if record.Pathname == "/" {
		return false
	}

	return sourceCtx.source.excludes.IsExcluded(record.Pathname, isDir)
}

func (snap *Builder) processRecord(idx int, sourceCtx *sourceContext, record *connectors.Record, stats *scanStats, chunker *chunkers.Chunker) error {
	if !snap.noSkipSelf {
		// XXX: Remove this when we introduce the Location object.
		if snap.repository.Type() == "fs" || snap.repository.Type() == "ptar" {
			if record.Pathname == snap.repository.Root() || strings.HasPrefix(record.Pathname, snap.repository.Root()+"/") {
				snap.Logger().Warn("skipping entry from repository: %s", record.Pathname)
				snap.emitter.PathOk(record.Pathname)
				return nil
			}
		}
	}

	if record.IsXattr {
		atomic.AddUint64(&stats.nxattrs, +1)
		snap.emitter.Xattr(record.Pathname)
		if err := snap.processFileRecord(idx, sourceCtx, record, chunker); err != nil {
			snap.emitter.XattrError(record.Pathname, err)
			return err
		} else {
			snap.emitter.XattrOk(record.Pathname, record.FileInfo.Size())
		}
	} else if record.FileInfo.Mode().IsDir() {
		atomic.AddUint64(&stats.ndirs, +1)
		snap.emitter.Directory(record.Pathname)
		if err := snap.processDirectoryRecord(idx, sourceCtx, record, chunker); err != nil {
			snap.emitter.DirectoryError(record.Pathname, err)
			return err
		} else {
			snap.emitter.DirectoryOk(record.Pathname, record.FileInfo)
		}
		return nil
	} else if record.Target != "" {
		atomic.AddUint64(&stats.nlinks, +1)
		snap.emitter.Symlink(record.Pathname)
		if err := snap.processFileRecord(idx, sourceCtx, record, chunker); err != nil {
			snap.emitter.SymlinkError(record.Pathname, err)
			return err
		} else {
			snap.emitter.SymlinkOk(record.Pathname)
		}
	} else {
		atomic.AddUint64(&stats.nfiles, +1)
		snap.emitter.File(record.Pathname)
		if err := snap.processFileRecord(idx, sourceCtx, record, chunker); err != nil {
			snap.emitter.FileError(record.Pathname, err)
			return err
		} else {
			snap.emitter.FileOk(record.Pathname, record.FileInfo)
			if record.FileInfo.Mode().IsRegular() {
				atomic.AddUint64(&stats.size, uint64(record.FileInfo.Size()))
			}
		}
	}
	return nil
}

func (snap *Builder) importSource(imp importer.Importer, sourceCtx *sourceContext, stats *scanStats) error {
	var ckers []*chunkers.Chunker
	for range snap.AppContext().MaxConcurrency {
		cker, err := snap.repository.Chunker(nil)
		if err != nil {
			return err
		}

		ckers = append(ckers, cker)
	}

	var (
		size    = snap.appContext.MaxConcurrency
		records = make(chan *connectors.Record, size)
		errch   = make(chan error, 1)

		wg  = errgroup.Group{}
		ctx = snap.AppContext()
	)

	var results chan *connectors.Result
	if (imp.Flags() & location.FLAG_NEEDACK) != 0 {
		results = make(chan *connectors.Result, size)
	}

	for i, cker := range ckers {
		ck := cker
		idx := i
		wg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case record, ok := <-records:
					if !ok {
						return nil
					}

					if !snap.builderOptions.NoXattr || !record.IsXattr {
						if record.Err != nil {
							snap.emitter.Path(record.Pathname)
							snap.emitter.PathError(record.Pathname, record.Err)
							sourceCtx.recordError(idx, record.Pathname, record.Err)

						} else if !snap.skipExcludedPathname(sourceCtx, record) {
							snap.emitter.Path(record.Pathname)
							if err := snap.processRecord(idx, sourceCtx, record, stats, ck); err != nil {
								sourceCtx.recordError(idx, record.Pathname, err)
								snap.emitter.PathError(record.Pathname, err)
							} else {
								snap.emitter.PathOk(record.Pathname)
							}
						}
					}
					if results != nil {
						results <- record.Ok()
					} else {
						record.Close()
					}
				}
			}
		})
	}

	// wait asynchronously for the workers because we need to
	// close results to signal Import() that we're done.
	go func() {
		if err := wg.Wait(); err != nil {
			errch <- err

			// we also need to drain records if we were canceled
			// otherwise importers may hang if they tried to send
			// before handling cancellation
			for range records {
			}
		}

		close(errch)
		if results != nil {
			close(results)
		}
	}()

	importerErr := imp.Import(ctx, records, results)
	if results != nil {
		for range results {
			// drain the results channel so that we unblock the
			// workers.
		}
	}
	if importerErr != nil {
		return importerErr
	}

	if err := <-errch; err != nil {
		return err
	}

	return nil
}

func (snap *Builder) importerJob(sourceCtx *sourceContext) error {
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "importer job took %s", time.Since(t0))
	}()

	var stats scanStats

	snap.emitter.Info("snapshot.import.start", map[string]any{})
	defer func() {
		snap.emitter.Info("snapshot.import.done", map[string]any{
			"nfiles": stats.nfiles,
			"ndirs":  stats.ndirs,
			"size":   stats.size,
		})
	}()

	for _, imp := range sourceCtx.source.Importers() {
		if err := snap.importSource(imp, sourceCtx, &stats); err != nil {
			return err
		}
	}

	// Flush any left over entries.
	if err := sourceCtx.vfsEntBatch.Commit(); err != nil {
		return err
	}

	sourceCtx.vfsEntBatch = nil
	return nil
}

func (snap *Builder) Import(source *Source) error {
	return snap.Backup(source)
}

func (snap *Builder) Backup(source *Source) error {
	/* phase 0: setup - prepare source context and indexes */
	sourceCtx, err := snap.prepareSourceContext(source)
	if sourceCtx != nil {
		defer sourceCtx.indexes.Close(snap.Logger())
	}
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}
	defer sourceCtx.scanLog.Close()

	/* phase 1: import data/metadata from source, reuse cached or store new, log to scanlog */
	if err := snap.importerJob(sourceCtx); err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* phase 2: build VFS indexes from scanlog */
	rootSummary, err := snap.buildVFS(sourceCtx)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* phase 3: persist indexes to store */
	vfsHeader, indexes, err := snap.persistVFS(sourceCtx)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* everything successful, we can record to snapshot header */
	headerSource := source.GetHeader()
	headerSource.VFS = *vfsHeader
	headerSource.Summary = *rootSummary
	headerSource.Indexes = indexes
	snap.Header.Sources = append(snap.Header.Sources, headerSource)

	return nil
}

func entropy(data []byte) (float64, [256]float64) {
	if len(data) == 0 {
		return 0.0, [256]float64{}
	}

	// Count the frequency of each byte value
	var freq [256]float64
	for _, b := range data {
		freq[b]++
	}

	// Calculate the entropy
	entropy := 0.0
	dataSize := float64(len(data))
	for _, f := range freq {
		if f > 0 {
			p := f / dataSize
			entropy -= p * math.Log2(p)
		}
	}
	return entropy, freq
}

func (snap *Builder) chunkifyDirpack(chk *chunkers.Chunker, chkId int, rd io.Reader) (objects.MAC, error) {
	object := objects.NewObject()

	objectHasher, releaseGlobalHasher := snap.repository.GetPooledMACHasher()
	defer releaseGlobalHasher()

	var cdcOffset uint64
	var object_t32 objects.MAC

	// Helper function to process a chunk
	processChunk := func(idx int, data []byte) error {
		var chunk_t32 objects.MAC

		chunkHasher, releaseChunkHasher := snap.repository.GetPooledMACHasher()

		chunkHasher.Write(data)
		copy(chunk_t32[:], chunkHasher.Sum(nil))
		releaseChunkHasher()

		chunk := objects.NewChunk()
		chunk.ContentMAC = chunk_t32
		chunk.Length = uint32(len(data))

		object.Chunks = append(object.Chunks, *chunk)
		cdcOffset += uint64(len(data))

		// It's safe to pin to 0 here, we are not chunkifying files
		// concurrently so there will be no interleaving, and that means we get
		// the dirpack's chunk and Object entries in a sequence.
		return snap.repository.PutBlobIfNotExistsWithHint(chkId, resources.RT_CHUNK, chunk.ContentMAC, data)
	}

	chk.Reset(rd)
	ctx := snap.AppContext()
	for i := 0; ; i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return objects.MAC{}, err
			}
		}

		cdcChunk, err := chk.Next()
		if err != nil && err != io.EOF {
			return objects.MAC{}, err
		}

		if cdcChunk != nil {
			chunkCopy := make([]byte, len(cdcChunk))
			copy(chunkCopy, cdcChunk)

			objectHasher.Write(chunkCopy)

			if err := processChunk(i, chunkCopy); err != nil {
				return objects.MAC{}, err
			}
		}
		if err == io.EOF {
			break
		}
	}

	object.ContentType = "application/octet-stream"

	copy(object_t32[:], objectHasher.Sum(nil))
	object.ContentMAC = object_t32

	objectSerialized, err := object.Serialize()
	if err != nil {
		return objects.MAC{}, err
	}
	objectMAC := snap.repository.ComputeMAC(objectSerialized)

	// chunkify has only PutBlobWithHint() because the Exists() test is done
	// _beforehand_, in the dirpack case we do not do the caching layer at all
	// so we need to do the deduplication here.
	if err := snap.repository.PutBlobIfNotExistsWithHint(chkId, resources.RT_OBJECT, objectMAC, objectSerialized); err != nil {
		return objects.MAC{}, err
	}

	return objectMAC, nil
}

func (snap *Builder) chunkify(cIdx int, chk *chunkers.Chunker, pathname string, rd io.Reader, isXattr bool) (*objects.Object, objects.MAC, int64, error) {
	object := objects.NewObject()

	objectHasher, releaseGlobalHasher := snap.repository.GetPooledMACHasher()
	defer releaseGlobalHasher()

	var cdcOffset uint64
	var object_t32 objects.MAC

	var totalEntropy float64
	var totalFreq [256]float64
	var totalDataSize int64

	// Helper function to process a chunk
	processChunk := func(idx int, data []byte) error {
		var chunk_t32 objects.MAC

		chunkHasher, releaseChunkHasher := snap.repository.GetPooledMACHasher()
		if idx == 0 {
			if object.ContentType == "" {
				object.ContentType = mimetype.Detect(data).String()
			}
		}

		chunkHasher.Write(data)
		copy(chunk_t32[:], chunkHasher.Sum(nil))
		releaseChunkHasher()

		entropyScore, freq := entropy(data)
		if len(data) > 0 {
			for i := range 256 {
				totalFreq[i] += freq[i]
			}
		}
		chunk := objects.NewChunk()
		chunk.ContentMAC = chunk_t32
		chunk.Length = uint32(len(data))
		chunk.Entropy = entropyScore

		object.Chunks = append(object.Chunks, *chunk)
		cdcOffset += uint64(len(data))

		totalEntropy += chunk.Entropy * float64(len(data))
		totalDataSize += int64(len(data))

		return snap.repository.PutBlobIfNotExistsWithHint(cIdx, resources.RT_CHUNK, chunk.ContentMAC, data)
	}

	chk.Reset(rd)
	ctx := snap.AppContext()
	for i := 0; ; i++ {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, objects.MAC{}, -1, err
			}
		}

		cdcChunk, err := chk.Next()
		if err != nil && err != io.EOF {
			return nil, objects.MAC{}, -1, err
		}

		if cdcChunk != nil {
			chunkCopy := make([]byte, len(cdcChunk))
			copy(chunkCopy, cdcChunk)

			objectHasher.Write(chunkCopy)

			if err := processChunk(i, chunkCopy); err != nil {
				return nil, objects.MAC{}, -1, err
			}
		}
		if err == io.EOF {
			break
		}
	}

	if totalDataSize > 0 {
		object.Entropy = totalEntropy / float64(totalDataSize)
	} else {
		object.Entropy = 0.0
	}

	if object.ContentType == "" {
		object.ContentType = mime.TypeByExtension(path.Ext(pathname))
		if object.ContentType == "" {
			object.ContentType = "application/octet-stream"
		}
	}

	copy(object_t32[:], objectHasher.Sum(nil))
	object.ContentMAC = object_t32

	objectSerialized, err := object.Serialize()
	if err != nil {
		return nil, objects.MAC{}, -1, err
	}
	objectMAC := snap.repository.ComputeMAC(objectSerialized)

	if snap.vfsCache == nil || isXattr {
		if err := snap.repository.PutBlobIfNotExistsWithHint(cIdx, resources.RT_OBJECT, objectMAC, objectSerialized); err != nil {
			return nil, objects.MAC{}, -1, err
		}
	} else {
		if err := snap.repository.PutBlobWithHint(cIdx, resources.RT_OBJECT, objectMAC, objectSerialized); err != nil {
			return nil, objects.MAC{}, -1, err
		}
	}
	return object, objectMAC, totalDataSize, nil
}

func (bi *sourceIndexes) Close(log *logging.Logger) {
	// We need to protect those behind nil checks because we might be cleaning
	// up a half initialized backupIndex.
	if bi.vfsidx != nil {
		if err := bi.vfsidx.Close(); err != nil {
			log.Warn("Failed to close vfs btree: %s", err)
		}
	}

	if bi.summaryidx != nil {
		if err := bi.summaryidx.Close(); err != nil {
			log.Warn("Failed to close summary btree: %s", err)
		}
	}

	if bi.erridx != nil {
		if err := bi.erridx.Close(); err != nil {
			log.Warn("Failed to close error btree: %s", err)
		}
	}

	if bi.xattridx != nil {
		if err := bi.xattridx.Close(); err != nil {
			log.Warn("Failed to close xattr btree: %s", err)
		}
	}

	if bi.ctidx != nil {
		if err := bi.ctidx.Close(); err != nil {
			log.Warn("Failed to close content type btree: %s", err)
		}
	}

	if bi.dirpackidx != nil {
		if err := bi.dirpackidx.Close(); err != nil {
			log.Warn("Failed to close content dirpack btree: %s", err)
		}
	}
}

// XXX: Small layer violation, but this helps us steer away from the caching
// Manager. Trust the process (TM)
func (snap *Builder) tmpCacheDir() string {
	return path.Join(snap.AppContext().CacheDir, caching.CACHE_VERSION, fmt.Sprintf("dbstorer-%x", snap.Header.Identifier))
}

func (snap *Builder) makeBackupIndexes() (*sourceIndexes, error) {
	bi := &sourceIndexes{}

	vfsstore, err := caching.NewSQLiteDBStore[string, []byte](snap.tmpCacheDir(), "vfs")
	if err != nil {
		return nil, err
	}

	summarystore, err := caching.NewSQLiteDBStore[string, []byte](snap.tmpCacheDir(), "summary")
	if err != nil {
		return nil, err
	}

	errstore, err := caching.NewSQLiteDBStore[string, objects.MAC](snap.tmpCacheDir(), "error")
	if err != nil {
		return nil, err
	}

	xattrstore, err := caching.NewSQLiteDBStore[string, objects.MAC](snap.tmpCacheDir(), "xattr")
	if err != nil {
		return nil, err
	}

	ctstore, err := caching.NewSQLiteDBStore[string, objects.MAC](snap.tmpCacheDir(), "contenttype")
	if err != nil {
		return nil, err
	}

	dirpackstore, err := caching.NewSQLiteDBStore[string, objects.MAC](snap.tmpCacheDir(), "dirpack")
	if err != nil {
		return nil, err
	}

	bi.vfsidx, err = btree.New(vfsstore, vfs.PathCmp, 50)
	if err != nil {
		return nil, err
	}

	if bi.summaryidx, err = btree.New(summarystore, strings.Compare, 50); err != nil {
		return nil, err
	}

	if bi.erridx, err = btree.New(errstore, strings.Compare, 50); err != nil {
		return nil, err
	}

	if bi.xattridx, err = btree.New(xattrstore, vfs.PathCmp, 50); err != nil {
		return nil, err
	}

	if bi.ctidx, err = btree.New(ctstore, strings.Compare, 50); err != nil {
		return nil, err
	}

	if bi.dirpackidx, err = btree.New(dirpackstore, strings.Compare, 50); err != nil {
		return nil, err
	}

	return bi, nil
}

func (snap *Builder) prepareSourceContext(source *Source) (*sourceContext, error) {
	scanLog, err := scanlog.New(snap.tmpCacheDir())
	if err != nil {
		return nil, err
	}

	sourceCtx := &sourceContext{
		builder:     snap,
		source:      source,
		vfsEntBatch: scanLog.NewBatch(),
		vfsCache:    snap.vfsCache,
		scanLog:     scanLog,
	}

	if bi, err := snap.makeBackupIndexes(); err != nil {
		return nil, err
	} else {
		sourceCtx.indexes = bi
	}

	return sourceCtx, nil
}

func (snap *Builder) checkVFSCache(sourceCtx *sourceContext, record *connectors.Record) (*objects.CachedPath, error) {
	if sourceCtx.vfsCache == nil {
		return nil, nil
	}

	if record.IsXattr {
		return nil, nil
	}

	entry, err := sourceCtx.vfsCache.GetEntryForBackup(record.Pathname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	_ = snap.repository.BlobExists(resources.RT_VFS_ENTRY, entry.MAC)

	same := entry.Stat().Equal(&record.FileInfo)
	if record.FileInfo.Size() == -1 {
		same = entry.Stat().EqualIgnoreSize(&record.FileInfo)
	}
	if !same {
		return nil, nil
	}
	record.FileInfo = entry.FileInfo

	snap.emitter.PathCached(record.Pathname)

	if record.FileInfo.Mode()&os.ModeSymlink != 0 {
		snap.emitter.SymlinkCached(record.Pathname)
		return &objects.CachedPath{
			MAC:         entry.MAC,
			FileInfo:    entry.FileInfo,
			ContentType: "application/x-not-regular-file",
		}, nil
	}

	if entry.Object != (objects.MAC{}) {
		_ = snap.repository.BlobExists(resources.RT_OBJECT, entry.Object)
		b, err := snap.repository.GetBlobBytes(resources.RT_OBJECT, entry.Object)
		if err != nil {
			return nil, err
		}

		obj, err := objects.NewObjectFromBytes(b)
		if err != nil {
			return nil, err
		}
		for _, chunk := range obj.Chunks {
			_ = snap.repository.BlobExists(resources.RT_CHUNK, chunk.ContentMAC)
		}
	}

	snap.emitter.FileCached(record.Pathname, entry.FileInfo)
	return &objects.CachedPath{
		MAC:         entry.MAC,
		ObjectMAC:   entry.Object,
		FileInfo:    entry.FileInfo,
		Chunks:      entry.GetChunks(),
		Entropy:     entry.GetEntropy(),
		ContentType: entry.GetContentType(),
	}, nil
}

type contentMeta struct {
	ObjectMAC   objects.MAC
	Size        int64
	Chunks      uint64
	Entropy     float64
	ContentType string
}

func (snap *Builder) computeContent(idx int, chunker *chunkers.Chunker, cachedPath *objects.CachedPath, record *connectors.Record) (*contentMeta, error) {
	if !record.FileInfo.Mode().IsRegular() {
		return &contentMeta{
			ContentType: "application/x-not-regular-file",
		}, nil
	}

	if record.Reader == nil {
		return nil, fmt.Errorf("got a regular file without an associated reader: %s", record.Pathname)
	}

	if cachedPath != nil && cachedPath.ObjectMAC != (objects.MAC{}) {
		if snap.repository.BlobExists(resources.RT_OBJECT, cachedPath.ObjectMAC) {
			return &contentMeta{
				ObjectMAC:   cachedPath.ObjectMAC,
				Size:        cachedPath.FileInfo.Size(),
				Chunks:      cachedPath.Chunks,
				Entropy:     cachedPath.Entropy,
				ContentType: cachedPath.ContentType,
			}, nil
		}
	}

	obj, objMAC, dataSize, err := snap.chunkify(idx, chunker, record.Pathname, record.Reader, record.IsXattr)
	if err != nil {
		return nil, err
	}

	// file size may have changed between scan and chunkify
	record.FileInfo.Lsize = dataSize

	return &contentMeta{
		ObjectMAC:   objMAC,
		Size:        dataSize,
		Chunks:      uint64(len(obj.Chunks)),
		Entropy:     obj.Entropy,
		ContentType: obj.ContentType,
	}, nil
}

func (snap *Builder) writeDirectoryEntry(idx int, sourceCtx *sourceContext, cachedPath *objects.CachedPath, record *connectors.Record) error {
	dirEntry := vfs.NewEntry(path.Dir(record.Pathname), record)
	var dirEntryMAC objects.MAC

	if cachedPath != nil && snap.repository.BlobExists(resources.RT_VFS_ENTRY, cachedPath.MAC) {
		dirEntryMAC = cachedPath.MAC
		serialized, err := dirEntry.ToBytes()
		if err != nil {
			return err
		}
		return sourceCtx.batchRecordEntry(scanlog.KindDirectory, dirEntry.Path(), serialized, nil)
	} else {
		serialized, err := dirEntry.ToBytes()
		if err != nil {
			return err
		}

		dirEntryMAC = snap.repository.ComputeMAC(serialized)
		if snap.vfsCache == nil || record.IsXattr {
			if err := snap.repository.PutBlobIfNotExistsWithHint(idx, resources.RT_VFS_ENTRY, dirEntryMAC, serialized); err != nil {
				return err
			}
		} else {
			if err := snap.repository.PutBlobWithHint(idx, resources.RT_VFS_ENTRY, dirEntryMAC, serialized); err != nil {
				return err
			}
		}
		return sourceCtx.batchRecordEntry(scanlog.KindDirectory, dirEntry.Path(), serialized, nil)
	}
}

func (snap *Builder) writeFileEntry(idx int, sourceCtx *sourceContext, meta *contentMeta, cachedPath *objects.CachedPath, record *connectors.Record) error {
	if meta.ContentType == "" {
		return fmt.Errorf("content type cannot be empty!")
	}

	fileEntry := vfs.NewEntry(path.Dir(record.Pathname), record)
	if record.FileInfo.Mode().IsRegular() && meta.ObjectMAC != (objects.MAC{}) {
		fileEntry.Object = meta.ObjectMAC
	}

	var fileEntryMAC objects.MAC
	var serializedFileEntry []byte
	var err error

	if cachedPath != nil && snap.repository.BlobExists(resources.RT_VFS_ENTRY, cachedPath.MAC) {
		fileEntryMAC = cachedPath.MAC
		if fileEntry.Object == (objects.MAC{}) && cachedPath.ObjectMAC != (objects.MAC{}) {
			fileEntry.Object = cachedPath.ObjectMAC
		}
		fileEntry.Chunks = cachedPath.Chunks
		fileEntry.ContentType = cachedPath.ContentType
		fileEntry.Entropy = cachedPath.Entropy
		serializedFileEntry, err = fileEntry.ToBytes()
		if err != nil {
			return err
		}
	} else {

		fileEntry.Chunks = meta.Chunks
		fileEntry.ContentType = meta.ContentType
		fileEntry.Entropy = meta.Entropy

		serialized, err := fileEntry.ToBytes()
		if err != nil {
			return err
		}

		fileEntryMAC = snap.repository.ComputeMAC(serialized)

		if snap.vfsCache == nil || record.IsXattr {
			if err := snap.repository.PutBlobIfNotExistsWithHint(idx, resources.RT_VFS_ENTRY, fileEntryMAC, serialized); err != nil {
				return err
			}
		} else {
			if err := snap.repository.PutBlobWithHint(idx, resources.RT_VFS_ENTRY, fileEntryMAC, serialized); err != nil {
				return err
			}
		}
		serializedFileEntry = serialized
	}

	fileSummary := &vfs.FileSummary{
		Size:        uint64(meta.Size),
		Chunks:      meta.Chunks,
		Mode:        record.FileInfo.Mode(),
		ModTime:     record.FileInfo.ModTime().Unix(),
		ContentType: meta.ContentType,
		Entropy:     meta.Entropy,
	}
	if meta.ObjectMAC != (objects.MAC{}) {
		fileSummary.Objects++
	}
	serializedSummary, err := fileSummary.Serialize()
	if err != nil {
		return err
	}

	parts := strings.SplitN(meta.ContentType, ";", 2)
	mime := parts[0]
	k := fmt.Sprintf("/%s%s", mime, record.Pathname)
	if err := sourceCtx.indexes.ctidx.Insert(k, fileEntryMAC); err != nil {
		return err
	}

	return sourceCtx.batchRecordEntry(scanlog.KindFile, fileEntry.Path(), serializedFileEntry, serializedSummary)
}

func (snap *Builder) processDirectoryRecord(idx int, sourceCtx *sourceContext, record *connectors.Record, chunker *chunkers.Chunker) error {
	cachedPath, err := snap.checkVFSCache(sourceCtx, record)
	if err != nil {
		snap.Logger().Warn("VFS CACHE: %v", err)
	}

	return snap.writeDirectoryEntry(idx, sourceCtx, cachedPath, record)
}

func (snap *Builder) processFileRecord(idx int, sourceCtx *sourceContext, record *connectors.Record, chunker *chunkers.Chunker) error {
	cachedPath, err := snap.checkVFSCache(sourceCtx, record)
	if err != nil {
		snap.Logger().Warn("VFS CACHE: %v", err)
	}

	meta, err := snap.computeContent(idx, chunker, cachedPath, record)
	if err != nil {
		return err
	}

	if record.IsXattr {
		return sourceCtx.recordXattr(idx, record, meta.ObjectMAC, meta.Size)
	}

	return snap.writeFileEntry(idx, sourceCtx, meta, cachedPath, record)
}

// FrameHeader is just for documentation; it is hand-de/serialized.
type FrameHeader struct {
	Type   uint8  // Entry type (directory, file, xattr, etc.)
	Length uint32 // Length of the payload following this header
}

type DirPackEntry uint8

const (
	TypeVFSDirectory DirPackEntry = 1
	TypeVFSFile      DirPackEntry = 2
)

type chunkifyResult struct {
	mac objects.MAC
	err error
}

func (sourceCtx *sourceContext) summarizeFile(parentSummary *vfs.Summary, serializedSummary []byte) error {
	fileSummary, err := vfs.FileSummaryFromBytes(serializedSummary)
	if err != nil {
		return err
	}

	parentSummary.Directory.Children++
	parentSummary.UpdateWithFileSummary(fileSummary)

	return nil
}

func (sourceCtx *sourceContext) summarizeDirectory(parentSummary *vfs.Summary, pathname string) error {
	val, found, err := sourceCtx.indexes.summaryidx.Find(pathname)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("path %q not found in the summary index", pathname)
	}

	currentSummary, err := vfs.SummaryFromBytes(val)
	if err != nil {
		return err
	}
	parentSummary.Directory.Children++
	parentSummary.Directory.Directories++
	parentSummary.UpdateBelow(currentSummary)
	return nil
}

func (sourceCtx *sourceContext) buildSummary(builder *Builder, pathname string) (*vfs.Summary, error) {
	currentSummary := &vfs.Summary{}

	for e := range sourceCtx.scanLog.ListDirectPathnames(pathname, false) {
		switch e.Kind {
		case scanlog.KindFile:
			err := sourceCtx.summarizeFile(currentSummary, e.Summary)
			if err != nil {
				return nil, err
			}
		case scanlog.KindDirectory:
			err := sourceCtx.summarizeDirectory(currentSummary, e.Path)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected entry kind %d for path %q", e.Kind, e.Path)
		}
	}

	errorsCount, err := sourceCtx.scanLog.CountDirectPathMACs(scanlog.KindError, pathname)
	if err != nil {
		return nil, err
	}
	currentSummary.Below.Errors += errorsCount

	currentSummary.UpdateAverages()

	serializedSummary, err := currentSummary.ToBytes()
	if err != nil {
		return nil, err
	}

	if err := sourceCtx.indexes.summaryidx.Insert(pathname, serializedSummary); err != nil {
		return nil, err
	}
	summaryMAC := builder.repository.ComputeMAC(serializedSummary)
	if err := builder.repository.PutBlobIfNotExists(resources.RT_VFS_SUMMARY, summaryMAC, serializedSummary); err != nil {
		return nil, err
	}

	return currentSummary, nil
}

func (sourceCtx *sourceContext) buildSummaries(builder *Builder, directories []string) (*vfs.Summary, error) {
	itemsCount := 0
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "building summary index took %s for %d entries", time.Since(t0), itemsCount)
	}()

	var rootSummary *vfs.Summary
	for _, directory := range directories {
		summary, err := sourceCtx.buildSummary(builder, directory)
		if err != nil {
			return nil, err
		}
		if directory == "/" {
			rootSummary = summary
		}
		itemsCount++
	}
	if rootSummary == nil {
		return nil, fmt.Errorf("failed to build root summary !")
	}
	return rootSummary, nil
}

func (sourceCtx *sourceContext) buildDirpack(builder *Builder, chunker *chunkers.Chunker, chkId int, pathname string) (objects.MAC, error) {
	pr, pw := io.Pipe()
	resCh := make(chan chunkifyResult, 1)

	go func() {
		mac, err := builder.chunkifyDirpack(chunker, chkId, pr)
		resCh <- chunkifyResult{mac: mac, err: err}
	}()

	for e := range sourceCtx.scanLog.ListDirectPathnames(pathname, false) {
		if err := builder.AppContext().Err(); err != nil {
			pw.CloseWithError(err)
			return objects.NilMac, err
		}
		switch e.Kind {
		case scanlog.KindFile:
			if err := writeFrame(builder, pw, TypeVFSFile, e.Payload); err != nil {
				pw.CloseWithError(err)
				return objects.NilMac, err
			}
		case scanlog.KindDirectory:
			if err := writeFrame(builder, pw, TypeVFSDirectory, e.Payload); err != nil {
				pw.CloseWithError(err)
				return objects.NilMac, err
			}

		default:
			err := fmt.Errorf("unexpected entry kind %d for path %q", e.Kind, e.Path)
			pw.CloseWithError(err)
			return objects.NilMac, err
		}
	}

	pw.Close()
	res := <-resCh
	if res.err != nil {
		return objects.NilMac, fmt.Errorf("chunkify failed for %s: %w", pathname, res.err)
	}

	return res.mac, nil
}

func (sourceCtx *sourceContext) buildDirpacks(builder *Builder, directories []string) error {
	itemsCount := 0
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "building dirpack index took %s for %d entries", time.Since(t0), itemsCount)
	}()

	type directoryRequest struct {
		directory string
		idx       int
	}
	macs := make([]objects.MAC, len(directories))
	directoriesChan := make(chan directoryRequest, builder.AppContext().MaxConcurrency*2)

	// setup chunkers
	var ckers []*chunkers.Chunker
	for range builder.AppContext().MaxConcurrency {
		cker, err := builder.repository.Chunker(nil)
		if err != nil {
			return err
		}
		ckers = append(ckers, cker)
	}

	g, ctx := errgroup.WithContext(builder.AppContext())

	// emit directory processing requests, provide idx so that the workers can update
	// the macs array with an immediate access regardless of their execution order...
	g.Go(func() error {
		defer close(directoriesChan)
		for idx, directory := range directories {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case directoriesChan <- directoryRequest{directory: directory, idx: idx}:
			}
			itemsCount++
		}
		return nil
	})

	// compute multiple dirpacks concurrently, each one is pegged to a chunker
	for i, cker := range ckers {
		ck := cker
		cIdx := i

		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case req, ok := <-directoriesChan:
					if !ok {
						return nil
					}

					mac, err := sourceCtx.buildDirpack(builder, ck, cIdx, req.directory)
					if err != nil {
						return err
					}

					// lock-safe: each idx written exactly once
					macs[req.idx] = mac
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for idx, directory := range directories {
		if err := sourceCtx.indexes.dirpackidx.Insert(directory, macs[idx]); err != nil {
			return err
		}
	}
	return nil
}

// writeFrame writes a directory entry for dirpack.  Each entry is
// encoded in TLV, where the type is one byte, the length is a 32 bit
// unsigned integer and the data follows, all without padding.
func writeFrame(builder *Builder, w io.Writer, typ DirPackEntry, data []byte) error {
	mac := builder.repository.ComputeMAC(data)
	overflowCheck := uint64(len(data)) + uint64(len(mac))
	if overflowCheck > math.MaxUint32 {
		return ErrOutOfRange
	}

	tot := uint32(overflowCheck)
	endian := binary.LittleEndian
	if err := binary.Write(w, endian, typ); err != nil {
		return err
	}
	if err := binary.Write(w, endian, tot); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	_, err := w.Write(mac[:])
	return err

}

func (snap *Builder) relinkNodesRecursive(sourceCtx *sourceContext, pathname string) error {
	item, err := sourceCtx.scanLog.GetDirectory(pathname)
	if err != nil {
		return err
	}

	parent := path.Dir(pathname)

	if item == nil {
		dirEntry := vfs.NewEntry(parent, &connectors.Record{
			Pathname: pathname,
			FileInfo: objects.FileInfo{
				Lname:    path.Base(pathname),
				Lmode:    os.ModeDir | 0750,
				LmodTime: time.Unix(0, 0).UTC(),
			},
		})

		serialized, err := dirEntry.ToBytes()
		if err != nil {
			return err
		}

		if err := sourceCtx.scanLog.PutDirectory(pathname, serialized); err != nil {
			return err
		}

		mac := snap.repository.ComputeMAC(serialized)
		if err := snap.repository.PutBlobIfNotExists(resources.RT_VFS_ENTRY, mac, serialized); err != nil {
			return err
		}
	}

	if parent == pathname {
		// reached root
		return nil
	}
	return snap.relinkNodesRecursive(sourceCtx, parent)
}

func (snap *Builder) relinkNodes(sourceCtx *sourceContext) error {
	var itemsCount uint64

	t0 := time.Now()
	defer func() {
		snap.appContext.GetLogger().Trace("import", "relinking nodes took %s for %d entries", time.Since(t0), itemsCount)
	}()

	var missingMap = make(map[string]struct{})

	for e := range sourceCtx.scanLog.ListPathnames("/", true) {
		missingMap[path.Dir(e.Path)] = struct{}{}
		delete(missingMap, e.Path)
	}

	for missingDir := range missingMap {
		if err := snap.relinkNodesRecursive(sourceCtx, missingDir); err != nil {
			return err
		}
		itemsCount++
		delete(missingMap, missingDir)
	}

	return snap.relinkNodesRecursive(sourceCtx, "/")
}

func (sourceCtx *sourceContext) buildErrorIndex() error {
	itemsCount := 0
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "building error index took %s for %d entries", time.Since(t0), itemsCount)
	}()

	for e := range sourceCtx.scanLog.ListPathMACsFrom(scanlog.KindError, "/") {
		if err := sourceCtx.indexes.erridx.Insert(e.Path, e.MAC); err != nil && err != btree.ErrExists {
			return err
		}
		itemsCount++
	}
	return nil
}

func (sourceCtx *sourceContext) buildXattrIndex() error {
	itemsCount := 0
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "building xattr index took %s for %d entries", time.Since(t0), itemsCount)
	}()

	for e := range sourceCtx.scanLog.ListPathMACsFrom(scanlog.KindXattr, "/") {
		if err := sourceCtx.indexes.xattridx.Insert(e.Path, e.MAC); err != nil && err != btree.ErrExists {
			return err
		}
		itemsCount++
	}
	return nil
}

func (sourceCtx *sourceContext) buildVFSIndex() ([]string, error) {
	itemsCount := 0
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "building vfs index took %s for %d entries", time.Since(t0), itemsCount)
	}()

	directories := make([]string, 0)
	for e := range sourceCtx.scanLog.ListPathnameEntries("/", true) {
		if err := sourceCtx.indexes.vfsidx.Insert(e.Path, e.Payload); err != nil && err != btree.ErrExists {
			return nil, err
		}
		if e.Kind == scanlog.KindDirectory {
			directories = append(directories, e.Path)
		}
		itemsCount++
	}
	return directories, nil
}

func (snap *Builder) buildVFS(sourceCtx *sourceContext) (*vfs.Summary, error) {
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "build VFS took %s", time.Since(t0))
	}()

	snap.emitter.Info("snapshot.vfs.start", map[string]any{})
	defer snap.emitter.Info("snapshot.vfs.end", map[string]any{})

	/* relinking must happen before anything else so we guarantee there are no orphan pathnames */
	if err := snap.relinkNodes(sourceCtx); err != nil {
		return nil, err
	}

	sourceCtx.scanLog.SetMaxConn(snap.AppContext().MaxConcurrency)

	// stabilize order of vfsidx inserts early so we can unlock concurrency later
	// on a 1.000.000 korpus, this takes roughly 2s
	// /!\ this can't be parallelized with other indexes and must finish first !
	directories, err := sourceCtx.buildVFSIndex()
	if err != nil {
		return nil, err
	}

	var rootSummary *vfs.Summary

	g, _ := errgroup.WithContext(snap.AppContext())
	g.Go(func() error {
		summary, err := sourceCtx.buildSummaries(snap, directories)
		if err != nil {
			return err
		}
		rootSummary = summary
		return nil
	})

	g.Go(func() error {
		return sourceCtx.buildDirpacks(snap, directories)
	})

	g.Go(func() error {
		return sourceCtx.buildErrorIndex()
	})

	g.Go(func() error {
		return sourceCtx.buildXattrIndex()
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	if rootSummary == nil {
		return nil, fmt.Errorf("failed to summarize root !")
	}
	return rootSummary, nil
}

func (snap *Builder) persistVFS(sourceCtx *sourceContext) (*header.VFS, []header.Index, error) {
	t0 := time.Now()
	defer func() {
		sourceCtx.builder.appContext.GetLogger().Trace("import", "persistVFS took %s", time.Since(t0))
	}()

	snap.emitter.Info("snapshot.index.start", map[string]any{})
	defer snap.emitter.Info("snapshot.index.end", map[string]any{})

	var (
		rootmac      objects.MAC
		errmac       objects.MAC
		xattrmac     objects.MAC
		ctmac        objects.MAC
		dirpackmac   objects.MAC
		summariesmac objects.MAC
	)

	g, _ := errgroup.WithContext(snap.AppContext())

	g.Go(func() error {
		t0 := time.Now()
		defer func() {
			sourceCtx.builder.appContext.GetLogger().Trace("import", "persisting vfs index took %s", time.Since(t0))
		}()
		m, err := persistIndex(snap, sourceCtx.indexes.vfsidx, resources.RT_VFS_BTREE,
			resources.RT_VFS_NODE, func(data []byte) (objects.MAC, error) {
				return snap.repository.ComputeMAC(data), nil
			})
		if err != nil {
			return err
		}
		rootmac = m
		return nil
	})

	g.Go(func() error {
		t0 := time.Now()
		defer func() {
			sourceCtx.builder.appContext.GetLogger().Trace("import", "persisting error index took %s", time.Since(t0))
		}()

		m, err := persistIndex(snap, sourceCtx.indexes.erridx,
			resources.RT_ERROR_BTREE, resources.RT_ERROR_NODE, func(mac objects.MAC) (objects.MAC, error) {
				return mac, nil
			})
		if err != nil {
			return err
		}
		errmac = m
		return nil
	})

	g.Go(func() error {
		t0 := time.Now()
		defer func() {
			sourceCtx.builder.appContext.GetLogger().Trace("import", "persisting xattr index took %s", time.Since(t0))
		}()

		m, err := persistIndex(snap, sourceCtx.indexes.xattridx,
			resources.RT_XATTR_BTREE, resources.RT_XATTR_NODE, func(mac objects.MAC) (objects.MAC, error) {
				return mac, nil
			})
		if err != nil {
			return err
		}
		xattrmac = m
		return nil
	})

	g.Go(func() error {
		t0 := time.Now()
		defer func() {
			sourceCtx.builder.appContext.GetLogger().Trace("import", "persisting content-types index took %s", time.Since(t0))
		}()

		m, err := persistIndex(snap, sourceCtx.indexes.ctidx,
			resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
				return mac, nil
			})
		if err != nil {
			return err
		}
		ctmac = m
		return nil
	})

	g.Go(func() error {
		t0 := time.Now()
		defer func() {
			sourceCtx.builder.appContext.GetLogger().Trace("import", "persisting dirpack index took %s", time.Since(t0))
		}()

		m, err := persistIndex(snap, sourceCtx.indexes.dirpackidx,
			resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
				return mac, nil
			})
		if err != nil {
			return err
		}
		dirpackmac = m
		return nil
	})

	g.Go(func() error {
		t0 := time.Now()
		defer func() {
			sourceCtx.builder.appContext.GetLogger().Trace("import", "persisting summary index took %s", time.Since(t0))
		}()

		m, err := persistIndex(snap, sourceCtx.indexes.summaryidx,
			resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(data []byte) (objects.MAC, error) {
				return snap.repository.ComputeMAC(data), nil
			})
		if err != nil {
			return err
		}
		summariesmac = m
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	vfsHeader := &header.VFS{
		Root:   rootmac,
		Xattrs: xattrmac,
		Errors: errmac,
	}

	vfsIndexes := []header.Index{
		{
			Name:  "content-type",
			Type:  "btree",
			Value: ctmac,
		},
		{
			Name:  "dirpack",
			Type:  "btree",
			Value: dirpackmac,
		},
		{
			Name:  "summary",
			Type:  "btree",
			Value: summariesmac,
		},
	}

	return vfsHeader, vfsIndexes, nil
}
