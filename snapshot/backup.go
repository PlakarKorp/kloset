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
	"github.com/PlakarKorp/kloset/exclude"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/PlakarKorp/kloset/snapshot/scanlog"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/gabriel-vasile/mimetype"
	"golang.org/x/sync/errgroup"
)

type sourceIndexes struct {
	erridx     *btree.BTree[string, int, []byte]
	xattridx   *btree.BTree[string, int, []byte]
	ctidx      *btree.BTree[string, int, objects.MAC]
	dirpackidx *btree.BTree[string, int, objects.MAC]
}

type sourceContext struct {
	imp      importer.Importer
	excludes *exclude.RuleSet

	vfsCache *vfs.Filesystem

	vfsEntBatch *scanlog.ScanBatch
	vfsEntLock  sync.Mutex

	fileidx *btree.BTree[string, int, []byte]
	metaidx *btree.BTree[string, int, []byte]

	indexes *sourceIndexes // Aligned with the number of importers.

	scanLog *scanlog.ScanLog
}

type scanStats struct {
	nfiles uint64
	ndirs  uint64
	size   uint64
}

type BuilderOptions struct {
	Name            string
	Tags            []string
	Excludes        []string
	NoCheckpoint    bool
	NoCommit        bool
	NoXattr         bool
	ForcedTimestamp time.Time
	StateRefresher  func(objects.MAC, bool) error
}

var (
	ErrOutOfRange = errors.New("out of range")
)

func (sourceCtx *sourceContext) batchRecordEntry(entry *vfs.Entry) error {
	path := entry.Path()

	bytes, err := entry.ToBytes()
	if err != nil {
		return err
	}

	sourceCtx.vfsEntLock.Lock()
	defer sourceCtx.vfsEntLock.Unlock()

	if entry.FileInfo.IsDir() {
		if err := sourceCtx.vfsEntBatch.PutDirectory(path, bytes); err != nil {
			return err
		}
	} else {
		if err := sourceCtx.vfsEntBatch.PutFile(path, bytes); err != nil {
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

func (sourceCtx *sourceContext) recordError(path string, err error) error {
	entry := vfs.NewErrorItem(path, err.Error())
	serialized, e := entry.ToBytes()
	if e != nil {
		return err
	}

	return sourceCtx.indexes.erridx.Insert(path, serialized)
}

func (sourceCtx *sourceContext) recordXattr(record *importer.ScanRecord, objectMAC objects.MAC, size int64) error {
	xattr := vfs.NewXattr(record, objectMAC, size)
	serialized, err := xattr.ToBytes()
	if err != nil {
		return err
	}

	return sourceCtx.indexes.xattridx.Insert(xattr.ToPath(), serialized)
}

func (snapshot *Builder) skipExcludedPathname(sourceCtx *sourceContext, record *importer.ScanResult) bool {
	var pathname string
	var isDir bool
	switch {
	case record.Record != nil:
		pathname = record.Record.Pathname
		isDir = record.Record.FileInfo.IsDir()
	case record.Error != nil:
		pathname = record.Error.Pathname
		isDir = false
	}

	if pathname == "/" {
		return false
	}

	return sourceCtx.excludes.IsExcluded(pathname, isDir)
}

func (snap *Builder) processRecord(idx int, sourceCtx *sourceContext, record *importer.ScanResult, stats *scanStats, chunker *chunkers.Chunker) {
	switch {
	case record.Error != nil:
		record := record.Error
		sourceCtx.recordError(record.Pathname, record.Err)
		snap.emitter.PathError(record.Pathname, record.Err)

	case record.Record != nil:
		record := record.Record
		defer record.Close()

		if snap.builderOptions.NoXattr && record.IsXattr {
			return
		}

		snap.emitter.Path(record.Pathname)

		// XXX: Remove this when we introduce the Location object.
		repoLocation, err := snap.repository.Location()
		if err != nil {
			sourceCtx.recordError(record.Pathname, err)
			snap.emitter.FileError(record.Pathname, err)
			return
		}

		repoLocation = strings.TrimPrefix(repoLocation, "fs://")
		repoLocation = strings.TrimPrefix(repoLocation, "ptar://")
		if record.Pathname == repoLocation || strings.HasPrefix(record.Pathname, repoLocation+"/") {
			snap.Logger().Warn("skipping entry from repository: %s", record.Pathname)
			return
		}

		if record.FileInfo.Mode().IsDir() {
			atomic.AddUint64(&stats.ndirs, +1)
			entry := vfs.NewEntry(path.Dir(record.Pathname), record)
			if err := sourceCtx.batchRecordEntry(entry); err != nil {
				sourceCtx.recordError(record.Pathname, err)
			}
			return
		}

		snap.emitter.File(record.Pathname)

		if err := snap.processFileRecord(idx, sourceCtx, record, chunker); err != nil {
			snap.emitter.FileError(record.Pathname, err)
			sourceCtx.recordError(record.Pathname, err)
		} else {
			snap.emitter.FileOk(record.Pathname)
		}

		if record.IsXattr {
			return
		}

		atomic.AddUint64(&stats.nfiles, +1)
		if record.FileInfo.Mode().IsRegular() {
			atomic.AddUint64(&stats.size, uint64(record.FileInfo.Size()))
		}
	}
}

func (snap *Builder) importerJob(sourceCtx *sourceContext) error {
	var ckers []*chunkers.Chunker
	for range snap.AppContext().MaxConcurrency {
		cker, err := snap.repository.Chunker(nil)
		if err != nil {
			return err
		}

		ckers = append(ckers, cker)
	}

	scanner, err := sourceCtx.imp.Scan(snap.AppContext())
	if err != nil {
		return err
	}

	wg := errgroup.Group{}
	ctx := snap.AppContext()

	snap.emitter.Info("snapshot.import.start", map[string]any{})

	var stats scanStats
	for i, cker := range ckers {
		ck := cker
		idx := i
		wg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()

				case record, ok := <-scanner:
					if !ok {
						return nil
					}

					if snap.skipExcludedPathname(sourceCtx, record) {
						if record.Record != nil {
							record.Record.Close()
						}
						continue
					}

					snap.processRecord(idx, sourceCtx, record, &stats, ck)
				}
			}
		})
	}

	if err := wg.Wait(); err != nil {
		return err
	}

	for range scanner {
		// drain the importer channel since we might
		// have been cancelled while the importer is
		// trying to still produce some records.
	}

	// Flush any left over entries.
	if err := sourceCtx.vfsEntBatch.Commit(); err != nil {
		return err
	}

	sourceCtx.vfsEntBatch = nil

	snap.emitter.Info("snapshot.import.done", map[string]any{
		"nfiles": stats.nfiles,
		"ndirs":  stats.ndirs,
		"size":   stats.size,
	})

	return nil
}

func (snap *Builder) Backup(imp importer.Importer) error {
	beginTime := time.Now()

	done, err := snap.Lock()
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}
	defer snap.Unlock(done)

	origin, err := imp.Origin(snap.AppContext())
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	typ, err := imp.Type(snap.AppContext())
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	root, err := imp.Root(snap.AppContext())
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	options := snap.builderOptions

	if !options.ForcedTimestamp.IsZero() {
		if options.ForcedTimestamp.Before(time.Now()) {
			snap.Header.Timestamp = options.ForcedTimestamp.UTC()
		}
	}

	snap.Header.Tags = append(snap.Header.Tags, options.Tags...)

	if options.Name == "" {
		snap.Header.Name = root + " @ " + origin
	} else {
		snap.Header.Name = options.Name
	}

	sourceCtx, err := snap.prepareBackup(imp)
	if sourceCtx != nil {
		defer sourceCtx.indexes.Close(snap.Logger())
	}
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	defer sourceCtx.scanLog.Close()

	/* meta store */
	metastore, err := caching.NewSQLiteDBStore[string, []byte](snap.tmpCacheDir(), "metaidx")
	if err != nil {
		return err
	}
	defer metastore.Close()

	sourceCtx.metaidx, err = btree.New(metastore, vfs.PathCmp, 50)
	if err != nil {
		return err
	}

	/* importer */
	if err := snap.importerJob(sourceCtx); err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* tree builders */
	vfsHeader, rootSummary, indexes, err := snap.persistTrees(sourceCtx)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	snap.Header.Duration = time.Since(beginTime)

	source := header.NewSource()
	source.Importer.Origin = origin
	source.Importer.Type = typ
	source.Importer.Directory = root
	source.VFS = *vfsHeader
	source.Summary = *rootSummary
	source.Indexes = indexes

	snap.Header.Sources = append(snap.Header.Sources, source)

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

func (snap *Builder) chunkifyDirpack(chk *chunkers.Chunker, rd io.Reader) (objects.MAC, error) {
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
		return snap.repository.PutBlobIfNotExistsWithHint(0, resources.RT_CHUNK, chunk.ContentMAC, data)
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
	if err := snap.repository.PutBlobIfNotExistsWithHint(0, resources.RT_OBJECT, objectMAC, objectSerialized); err != nil {
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
	if bi.erridx != nil {
		if err := bi.erridx.Close(); err != nil {
			log.Warn("Failed to close index btree: %s", err)
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

	errstore, err := caching.NewSQLiteDBStore[string, []byte](snap.tmpCacheDir(), "error")
	if err != nil {
		return nil, err
	}

	xattrstore, err := caching.NewSQLiteDBStore[string, []byte](snap.tmpCacheDir(), "xattr")
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

func (snap *Builder) prepareBackup(imp importer.Importer) (*sourceContext, error) {
	scanLog, err := scanlog.New(snap.tmpCacheDir())
	if err != nil {
		return nil, err
	}

	sourceCtx := &sourceContext{
		imp:         imp,
		vfsEntBatch: scanLog.NewBatch(),
		vfsCache:    snap.vfsCache,
		scanLog:     scanLog,
	}

	sourceCtx.excludes = exclude.NewRuleSet()
	if err := sourceCtx.excludes.AddRulesFromArray(snap.builderOptions.Excludes); err != nil {
		return nil, fmt.Errorf("failed to setup exclude rules: %w", err)
	}

	if bi, err := snap.makeBackupIndexes(); err != nil {
		return nil, err
	} else {
		sourceCtx.indexes = bi
	}

	return sourceCtx, nil
}

func (snap *Builder) checkVFSCache(sourceCtx *sourceContext, record *importer.ScanRecord) (*objects.CachedPath, error) {
	if sourceCtx.vfsCache == nil {
		return nil, nil
	}

	if record.IsXattr {
		return nil, nil
	}

	entry, err := sourceCtx.vfsCache.GetEntryNoFollow(record.Pathname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	same := entry.Stat().Equal(&record.FileInfo)
	if record.FileInfo.Size() == -1 {
		same = entry.Stat().EqualIgnoreSize(&record.FileInfo)
	}
	if !same {
		return nil, nil
	}
	record.FileInfo = entry.FileInfo

	if record.FileInfo.Mode()&os.ModeSymlink != 0 {
		return &objects.CachedPath{
			MAC:      entry.MAC,
			FileInfo: entry.FileInfo,
		}, nil
	}

	return &objects.CachedPath{
		MAC:         entry.MAC,
		ObjectMAC:   entry.Object,
		FileInfo:    entry.FileInfo,
		Chunks:      uint64(len(entry.ResolvedObject.Chunks)),
		Entropy:     entry.ResolvedObject.Entropy,
		ContentType: entry.ResolvedObject.ContentType,
	}, nil
}

type contentMeta struct {
	ObjectMAC   objects.MAC
	Size        int64
	Chunks      uint64
	Entropy     float64
	ContentType string
}

func (snap *Builder) computeContent(idx int, chunker *chunkers.Chunker, cachedPath *objects.CachedPath, record *importer.ScanRecord) (*contentMeta, error) {
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

func (snap *Builder) writeFileEntry(idx int, sourceCtx *sourceContext, meta *contentMeta, cachedPath *objects.CachedPath, record *importer.ScanRecord) error {
	if meta.ContentType == "" {
		return fmt.Errorf("content type cannot be empty!")
	}

	fileEntry := vfs.NewEntry(path.Dir(record.Pathname), record)
	if record.FileInfo.Mode().IsRegular() && meta.ObjectMAC != (objects.MAC{}) {
		fileEntry.Object = meta.ObjectMAC
	}

	var fileEntryMAC objects.MAC
	var serializedCachedPath []byte
	var err error

	if cachedPath != nil && snap.repository.BlobExists(resources.RT_VFS_ENTRY, cachedPath.MAC) {
		fileEntryMAC = cachedPath.MAC
		if fileEntry.Object == (objects.MAC{}) && cachedPath.ObjectMAC != (objects.MAC{}) {
			fileEntry.Object = cachedPath.ObjectMAC
		}
		serializedCachedPath, err = cachedPath.Serialize()
		if err != nil {
			return err
		}
	} else {
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

		cp := &objects.CachedPath{
			MAC:         fileEntryMAC,
			ObjectMAC:   fileEntry.Object,
			FileInfo:    fileEntry.FileInfo,
			Chunks:      meta.Chunks,
			Entropy:     meta.Entropy,
			ContentType: meta.ContentType,
		}

		serializedCachedPath, err = cp.Serialize()
		if err != nil {
			return err
		}
	}

	if err := sourceCtx.metaidx.Insert(record.Pathname, serializedCachedPath); err != nil && err != btree.ErrExists {
		return err
	}

	parts := strings.SplitN(meta.ContentType, ";", 2)
	mime := parts[0]
	k := fmt.Sprintf("/%s%s", mime, record.Pathname)
	if err := sourceCtx.indexes.ctidx.Insert(k, fileEntryMAC); err != nil {
		return err
	}

	return sourceCtx.batchRecordEntry(fileEntry)
}

func (snap *Builder) processFileRecord(idx int, sourceCtx *sourceContext, record *importer.ScanRecord, chunker *chunkers.Chunker) error {
	cachedPath, err := snap.checkVFSCache(sourceCtx, record)
	if err != nil {
		snap.Logger().Warn("VFS CACHE: %v", err)
	}

	meta, err := snap.computeContent(idx, chunker, cachedPath, record)
	if err != nil {
		return err
	}

	if record.IsXattr {
		return sourceCtx.recordXattr(record, meta.ObjectMAC, meta.Size)
	}

	return snap.writeFileEntry(idx, sourceCtx, meta, cachedPath, record)
}

func (snap *Builder) persistTrees(sourceCtx *sourceContext) (*header.VFS, *vfs.Summary, []header.Index, error) {

	vfsHeader, summary, err := snap.persistVFS(sourceCtx)
	if err != nil {
		return nil, nil, nil, err
	}

	indexes, err := snap.persistIndexes(sourceCtx)
	if err != nil {
		return nil, nil, nil, err
	}

	return vfsHeader, summary, indexes, nil
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

func (sourceCtx *sourceContext) processFile(dirEntry *vfs.Entry, bytes []byte, path string) error {
	// bytes is a slice that will be reused in the next iteration,
	// swapping below our feet, so make a copy out of it
	dupBytes := make([]byte, len(bytes))
	copy(dupBytes, bytes)

	if err := sourceCtx.fileidx.Insert(path, dupBytes); err != nil && err != btree.ErrExists {
		return err
	}

	data, found, err := sourceCtx.metaidx.Find(path)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("path %q not found in the meta index", path)
	}

	cachedPath, err := objects.NewCachedPathFromBytes(data)
	if err != nil {
		return err
	}

	fileSummary := &vfs.FileSummary{
		Size:        uint64(cachedPath.FileInfo.Size()),
		Chunks:      cachedPath.Chunks,
		Mode:        cachedPath.FileInfo.Mode(),
		ModTime:     cachedPath.FileInfo.ModTime().Unix(),
		ContentType: cachedPath.ContentType,
		Entropy:     cachedPath.Entropy,
	}
	if cachedPath.ObjectMAC != (objects.MAC{}) {
		fileSummary.Objects++
	}

	dirEntry.Summary.Directory.Children++
	dirEntry.Summary.UpdateWithFileSummary(fileSummary)

	return nil
}

func (sourceCtx *sourceContext) processChildren(builder *Builder, dirEntry *vfs.Entry, w io.Writer, parent string) error {
	if parent != "/" {
		parent = strings.TrimRight(parent, "/")
	}

	for e := range sourceCtx.scanLog.ListDirectPathnames(parent, false) {
		switch e.Kind {
		case scanlog.KindFile:
			err := sourceCtx.processFile(dirEntry, e.Payload, e.Path)
			if err != nil {
				return err
			}
			if err := writeFrame(builder, w, TypeVFSFile, e.Payload); err != nil {
				return err
			}
		case scanlog.KindDirectory:
			val, found, err := sourceCtx.fileidx.Find(e.Path)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("missing summary for directory %q", e.Path)
			}

			childDirEntry, err := vfs.EntryFromBytes(val)
			if err != nil {
				return err
			}

			dirEntry.Summary.Directory.Children++
			dirEntry.Summary.Directory.Directories++
			dirEntry.Summary.UpdateBelow(childDirEntry.Summary)

			if err := writeFrame(builder, w, TypeVFSDirectory, val); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unexpected entry kind %d for path %q", e.Kind, e.Path)
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
		dirEntry := vfs.NewEntry(parent, &importer.ScanRecord{
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
	}

	if parent == pathname {
		// reached root
		return nil
	}
	return snap.relinkNodesRecursive(sourceCtx, parent)
}

func (snap *Builder) relinkNodes(sourceCtx *sourceContext) error {
	var missingMap = make(map[string]struct{})

	for e := range sourceCtx.scanLog.ListPathnames("/", true) {
		missingMap[path.Dir(e.Path)] = struct{}{}
		delete(missingMap, e.Path)
	}

	for missingDir := range missingMap {
		if err := snap.relinkNodesRecursive(sourceCtx, missingDir); err != nil {
			return err
		}
		delete(missingMap, missingDir)
	}

	return snap.relinkNodesRecursive(sourceCtx, "/")
}

func (snap *Builder) persistVFS(sourceCtx *sourceContext) (*header.VFS, *vfs.Summary, error) {
	snap.emitter.Info("snapshot.vfs.start", map[string]any{})
	defer snap.emitter.Info("snapshot.vfs.end", map[string]any{})

	if err := snap.relinkNodes(sourceCtx); err != nil {
		return nil, nil, err
	}

	errcsum, err := persistMACIndex(snap, sourceCtx.indexes.erridx,
		resources.RT_ERROR_BTREE, resources.RT_ERROR_NODE, resources.RT_ERROR_ENTRY)
	if err != nil {
		return nil, nil, err
	}

	filestore, err := caching.NewSQLiteDBStore[string, []byte](snap.tmpCacheDir(), "path")
	if err != nil {
		return nil, nil, err
	}

	sourceCtx.fileidx, err = btree.New(filestore, vfs.PathCmp, 50)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err := sourceCtx.fileidx.Close(); err != nil {
			snap.Logger().Warn("Failed to close fileidx btree: %s", err)
		}
	}()

	chunker, err := snap.repository.Chunker(nil)
	if err != nil {
		return nil, nil, err
	}

	var rootSummary *vfs.Summary

	dirPaths := make([]string, 0)
	for e := range sourceCtx.scanLog.ListDirectories("/", true) {
		dirPaths = append(dirPaths, e.Path)
	}

	for _, pathname := range dirPaths {
		dirPath := pathname

		dirBytes, err := sourceCtx.scanLog.GetDirectory(dirPath)
		if err != nil {
			return nil, nil, err
		}

		if err := snap.AppContext().Err(); err != nil {
			return nil, nil, err
		}

		pr, pw := io.Pipe()
		resCh := make(chan chunkifyResult, 1)

		go func() {
			mac, err := snap.chunkifyDirpack(chunker, pr)
			resCh <- chunkifyResult{mac: mac, err: err}
		}()

		dirEntry, err := vfs.EntryFromBytes(dirBytes)
		if err != nil {
			return nil, nil, err
		}

		prefix := dirPath
		if prefix != "/" {
			prefix += "/"
		}

		if err := sourceCtx.processChildren(snap, dirEntry, pw, prefix); err != nil {
			pw.CloseWithError(err)
			return nil, nil, err
		}

		erriter, err := sourceCtx.indexes.erridx.ScanFrom(prefix)
		if err != nil {
			return nil, nil, err
		}
		for erriter.Next() {
			path, _ := erriter.Current()
			if !strings.HasPrefix(path, prefix) {
				break
			}
			if strings.Contains(path[len(prefix):], "/") {
				break
			}
			dirEntry.Summary.Below.Errors++
		}
		if err := erriter.Err(); err != nil {
			return nil, nil, err
		}

		dirEntry.Summary.UpdateAverages()

		pw.Close()
		res := <-resCh
		if res.err != nil {
			return nil, nil, fmt.Errorf("chunkify failed for %s: %w", dirPath, res.err)
		}

		if err := sourceCtx.indexes.dirpackidx.Insert(dirPath, res.mac); err != nil {
			return nil, nil, err
		}

		snap.emitter.DirectoryOk(dirPath)

		if dirPath == "/" {
			if rootSummary != nil {
				return nil, nil, fmt.Errorf("importer yield a double root!")
			}
			rootSummary = dirEntry.Summary
		}

		serialized, err := dirEntry.ToBytes()
		if err != nil {
			return nil, nil, err
		}

		mac := snap.repository.ComputeMAC(serialized)
		if err := snap.repository.PutBlobIfNotExists(resources.RT_VFS_ENTRY, mac, serialized); err != nil {
			return nil, nil, err
		}

		if err := sourceCtx.fileidx.Insert(dirPath, serialized); err != nil && err != btree.ErrExists {
			return nil, nil, err
		}
	}

	rootcsum, err := persistIndex(snap, sourceCtx.fileidx, resources.RT_VFS_BTREE,
		resources.RT_VFS_NODE, func(data []byte) (objects.MAC, error) {
			return snap.repository.ComputeMAC(data), nil
		})
	if err != nil {
		return nil, nil, err
	}

	xattrcsum, err := persistMACIndex(snap, sourceCtx.indexes.xattridx,
		resources.RT_XATTR_BTREE, resources.RT_XATTR_NODE, resources.RT_XATTR_ENTRY)
	if err != nil {
		return nil, nil, err
	}

	vfsHeader := &header.VFS{
		Root:   rootcsum,
		Xattrs: xattrcsum,
		Errors: errcsum,
	}

	return vfsHeader, rootSummary, nil
}

func (snap *Builder) persistIndexes(sourceCtx *sourceContext) ([]header.Index, error) {
	snap.emitter.Info("snapshot.index.start", map[string]any{})
	defer snap.emitter.Info("snapshot.index.end", map[string]any{})
	ctmac, err := persistIndex(snap, sourceCtx.indexes.ctidx,
		resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
			return mac, nil
		})
	if err != nil {
		return nil, err
	}

	dirpackmac, err := persistIndex(snap, sourceCtx.indexes.dirpackidx,
		resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
			return mac, nil
		})
	if err != nil {
		return nil, err
	}

	return []header.Index{
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
	}, nil
}
