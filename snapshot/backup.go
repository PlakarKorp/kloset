package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"mime"
	"path"
	"strings"
	"sync/atomic"
	"time"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/gabriel-vasile/mimetype"
	"github.com/gobwas/glob"
	"golang.org/x/sync/errgroup"
)

type BackupIndexes struct {
	erridx    *btree.BTree[string, int, []byte]
	xattridx  *btree.BTree[string, int, []byte]
	ctidx     *btree.BTree[string, int, objects.MAC]
	prefixidx *btree.BTree[string, int, objects.MAC]
}

type BackupContext struct {
	imp            importer.Importer
	excludes       []glob.Glob
	maxConcurrency uint64

	scanCache *caching.ScanCache
	vfsCache  *caching.VFSCache

	stateId objects.MAC

	flushTick  *time.Ticker
	flushEnd   chan bool
	flushEnded chan bool

	indexes []*BackupIndexes // Aligned with the number of importers.
}

type scanStats struct {
	nfiles uint64
	ndirs  uint64
	size   uint64
}

type BackupOptions struct {
	MaxConcurrency  uint64
	Name            string
	Tags            []string
	Excludes        []string
	NoCheckpoint    bool
	NoCommit        bool
	CleanupVFSCache bool
}

func (bc *BackupContext) recordEntry(entry *vfs.Entry) error {
	path := entry.Path()

	bytes, err := entry.ToBytes()
	if err != nil {
		return err
	}

	if entry.FileInfo.IsDir() {
		return bc.scanCache.PutDirectory(0, path, bytes)
	}
	return bc.scanCache.PutFile(0, path, bytes)
}

func (bc *BackupContext) recordError(path string, err error) error {
	entry := vfs.NewErrorItem(path, err.Error())
	serialized, e := entry.ToBytes()
	if e != nil {
		return err
	}

	return bc.indexes[0].erridx.Insert(path, serialized)
}

func (bc *BackupContext) recordXattr(record *importer.ScanRecord, objectMAC objects.MAC, size int64) error {
	xattr := vfs.NewXattr(record, objectMAC, size)
	serialized, err := xattr.ToBytes()
	if err != nil {
		return err
	}

	return bc.indexes[0].xattridx.Insert(xattr.ToPath(), serialized)
}

func (snapshot *Builder) skipExcludedPathname(backupCtx *BackupContext, record *importer.ScanResult) bool {
	var pathname string
	switch {
	case record.Record != nil:
		pathname = record.Record.Pathname
	case record.Error != nil:
		pathname = record.Error.Pathname
	}

	if pathname == "/" {
		return false
	}

	doExclude := false
	for _, exclude := range backupCtx.excludes {
		if exclude.Match(pathname) {
			doExclude = true
			break
		}
	}
	return doExclude
}

func (snap *Builder) processRecord(idx int, backupCtx *BackupContext, record *importer.ScanResult, stats *scanStats, chunker *chunkers.Chunker) {
	switch {
	case record.Error != nil:
		record := record.Error
		backupCtx.recordError(record.Pathname, record.Err)
		snap.Event(events.PathErrorEvent(snap.Header.Identifier, record.Pathname, record.Err.Error()))

	case record.Record != nil:
		record := record.Record
		defer record.Close()

		snap.Event(events.PathEvent(snap.Header.Identifier, record.Pathname))

		// XXX: Remove this when we introduce the Location object.
		repoLocation, err := snap.repository.Location()
		if err != nil {
			snap.Event(events.FileErrorEvent(snap.Header.Identifier, record.Pathname, err.Error()))
			backupCtx.recordError(record.Pathname, err)
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
			if err := backupCtx.recordEntry(entry); err != nil {
				backupCtx.recordError(record.Pathname, err)
			}
			return
		}

		snap.Event(events.FileEvent(snap.Header.Identifier, record.Pathname))
		if err := snap.processFileRecord(idx, backupCtx, record, chunker); err != nil {
			snap.Event(events.FileErrorEvent(snap.Header.Identifier, record.Pathname, err.Error()))
			backupCtx.recordError(record.Pathname, err)
		} else {
			snap.Event(events.FileOKEvent(snap.Header.Identifier, record.Pathname, record.FileInfo.Size()))
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

func (snap *Builder) importerJob(backupCtx *BackupContext) error {
	var ckers []*chunkers.Chunker
	for range backupCtx.maxConcurrency {
		cker, err := snap.repository.Chunker(nil)
		if err != nil {
			return err
		}

		ckers = append(ckers, cker)
	}

	scanner, err := backupCtx.imp.Scan(snap.AppContext())
	if err != nil {
		return err
	}

	wg := errgroup.Group{}
	ctx := snap.AppContext()

	startEvent := events.StartImporterEvent()
	startEvent.SnapshotID = snap.Header.Identifier
	snap.Event(startEvent)

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

					if snap.skipExcludedPathname(backupCtx, record) {
						if record.Record != nil {
							record.Record.Close()
						}
						continue
					}

					snap.processRecord(idx, backupCtx, record, &stats, ck)
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

	doneEvent := events.DoneImporterEvent()
	doneEvent.SnapshotID = snap.Header.Identifier
	doneEvent.NumFiles = stats.nfiles
	doneEvent.NumDirectories = stats.ndirs
	doneEvent.Size = stats.size
	snap.Event(doneEvent)

	return nil
}

func (snap *Builder) flushDeltaState(bc *BackupContext) {
	for {
		select {
		case <-snap.repository.AppContext().Done():
			return
		case <-bc.flushEnd:
			// End of backup we push the last and final State. No need to take any locks at this point.
			err := snap.repository.CommitTransaction(bc.stateId)
			if err != nil {
				// XXX: ERROR HANDLING
				snap.Logger().Warn("Failed to push the final state to the repository %s", err)
			}

			// See below
			if snap.deltaCache != snap.scanCache {
				snap.deltaCache.Close()
			}

			bc.flushEnded <- true
			close(bc.flushEnded)
			return
		case <-bc.flushTick.C:
			// Now make a new state backed by a new cache.
			snap.deltaMtx.Lock()
			oldCache := snap.deltaCache
			oldStateId := bc.stateId

			identifier := objects.RandomMAC()
			deltaCache, err := snap.repository.AppContext().GetCache().Scan(identifier)
			if err != nil {
				// XXX: ERROR HANDLING
				snap.deltaMtx.Unlock()
				snap.Logger().Warn("Failed to open deltaCache %s\n", err)
				break
			}

			bc.stateId = identifier
			snap.deltaMtx.Unlock()

			// Now that the backup is free to progress we can serialize and push
			// the resulting statefile to the repo.
			err = snap.repository.FlushTransaction(deltaCache, oldStateId)
			if err != nil {
				// XXX: ERROR HANDLING
				snap.Logger().Warn("Failed to push the state to the repository %s", err)
			}

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

func (snap *Builder) Backup(imp importer.Importer, options *BackupOptions) error {
	beginTime := time.Now()
	snap.Event(events.StartEvent())
	defer snap.Event(events.DoneEvent())

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

	snap.Header.GetSource(0).Importer.Origin = origin
	snap.Header.GetSource(0).Importer.Type = typ
	snap.Header.Tags = append(snap.Header.Tags, options.Tags...)

	if options.Name == "" {
		snap.Header.Name = root + " @ " + snap.Header.GetSource(0).Importer.Origin
	} else {
		snap.Header.Name = options.Name
	}

	backupCtx, err := snap.prepareBackup(imp, options)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* checkpoint handling */
	if !options.NoCheckpoint {
		backupCtx.flushTick = time.NewTicker(1 * time.Hour)
		go snap.flushDeltaState(backupCtx)
	}

	/* importer */
	if err := snap.importerJob(backupCtx); err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* tree builders */
	vfsHeader, rootSummary, indexes, err := snap.persistTrees(backupCtx)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return nil
	}

	snap.Header.Duration = time.Since(beginTime)
	snap.Header.GetSource(0).Importer.Directory = root
	snap.Header.GetSource(0).VFS = *vfsHeader
	snap.Header.GetSource(0).Summary = *rootSummary
	snap.Header.GetSource(0).Indexes = indexes

	return snap.Commit(backupCtx, !options.NoCommit)
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

func (snap *Builder) chunkify(cIdx int, chk *chunkers.Chunker, pathname string, rd io.Reader) (*objects.Object, int64, error) {
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
				return nil, -1, err
			}
		}

		cdcChunk, err := chk.Next()
		if err != nil && err != io.EOF {
			return nil, -1, err
		}

		chunkCopy := make([]byte, len(cdcChunk))
		copy(chunkCopy, cdcChunk)

		objectHasher.Write(chunkCopy)

		if err := processChunk(i, chunkCopy); err != nil {
			return nil, -1, err
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
	}

	copy(object_t32[:], objectHasher.Sum(nil))
	object.ContentMAC = object_t32
	return object, totalDataSize, nil
}

func (snap *Builder) Commit(bc *BackupContext, commit bool) error {
	// First thing is to stop the ticker, as we don't want any concurrent flushes to run.
	// Maybe this could be stopped earlier.

	// If we end up in here without a BackupContext we come from Sync and we
	// can't rely on the flusher
	if bc != nil && bc.flushTick != nil {
		bc.flushTick.Stop()
	}

	serializedHdr, err := snap.Header.Serialize()
	if err != nil {
		return err
	}

	if kp := snap.AppContext().Keypair; kp != nil {
		serializedHdrMAC := snap.repository.ComputeMAC(serializedHdr)
		signature := kp.Sign(serializedHdrMAC[:])
		if err := snap.repository.PutBlob(resources.RT_SIGNATURE, snap.Header.Identifier, signature); err != nil {
			return err
		}
	}

	if err := snap.repository.PutBlob(resources.RT_SNAPSHOT, snap.Header.Identifier, serializedHdr); err != nil {
		return err
	}

	if !commit {
		return nil
	}

	snap.repository.PackerManager.Wait()

	// We are done with packfiles we can flush the last state, either through
	// the flusher, or manually here.
	if bc != nil && bc.flushTick != nil {
		bc.flushEnd <- true
		close(bc.flushEnd)
		<-bc.flushEnded
	} else {
		err = snap.repository.CommitTransaction(snap.Header.Identifier)
		if err != nil {
			snap.Logger().Warn("Failed to push the state to the repository %s", err)
			return err
		}
	}

	cache, err := snap.AppContext().GetCache().Repository(snap.repository.Configuration().RepositoryID)
	if err == nil {
		_ = cache.PutSnapshot(snap.Header.Identifier, serializedHdr)
	}

	snap.Logger().Trace("snapshot", "%x: Commit()", snap.Header.GetIndexShortID())
	return nil
}

func (snap *Builder) makeBackupIndexes() (*BackupIndexes, error) {
	bi := &BackupIndexes{}

	errstore := caching.DBStore[string, []byte]{
		Prefix: "__error__",
		Cache:  snap.scanCache,
	}

	xattrstore := caching.DBStore[string, []byte]{
		Prefix: "__xattr__",
		Cache:  snap.scanCache,
	}

	ctstore := caching.DBStore[string, objects.MAC]{
		Prefix: "__contenttype__",
		Cache:  snap.scanCache,
	}

	prefixstore := caching.DBStore[string, objects.MAC]{
		Prefix: "__prefix__",
		Cache:  snap.scanCache,
	}

	var err error
	if bi.erridx, err = btree.New(&errstore, strings.Compare, 50); err != nil {
		return nil, err
	}

	if bi.xattridx, err = btree.New(&xattrstore, vfs.PathCmp, 50); err != nil {
		return nil, err
	}

	if bi.ctidx, err = btree.New(&ctstore, strings.Compare, 50); err != nil {
		return nil, err
	}

	if bi.prefixidx, err = btree.New(&prefixstore, strings.Compare, 50); err != nil {
		return nil, err
	}

	return bi, nil
}

func (snap *Builder) prepareBackup(imp importer.Importer, backupOpts *BackupOptions) (*BackupContext, error) {
	maxConcurrency := backupOpts.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.AppContext().MaxConcurrency)
	}

	typ, err := imp.Type(snap.AppContext())
	if err != nil {
		return nil, err
	}

	origin, err := imp.Origin(snap.AppContext())
	if err != nil {
		return nil, err
	}

	vfsCache, err := snap.AppContext().GetCache().VFS(snap.repository.Configuration().RepositoryID, typ, origin, backupOpts.CleanupVFSCache)
	if err != nil {
		return nil, err
	}

	backupCtx := &BackupContext{
		imp:            imp,
		maxConcurrency: maxConcurrency,
		scanCache:      snap.scanCache,
		vfsCache:       vfsCache,
		flushEnd:       make(chan bool),
		flushEnded:     make(chan bool),
		stateId:        snap.Header.Identifier,
	}

	for i := range backupOpts.Excludes {
		g, err := glob.Compile(backupOpts.Excludes[i])
		if err != nil {
			err = fmt.Errorf("failed to compile exclude pattern %s: %w",
				backupOpts.Excludes[i], err)
			return nil, err
		}
		backupCtx.excludes = append(backupCtx.excludes, g)
	}

	if bi, err := snap.makeBackupIndexes(); err != nil {
		return nil, err
	} else {
		backupCtx.indexes = append(backupCtx.indexes, bi)
	}

	return backupCtx, nil
}

func (snap *Builder) processFileRecord(idx int, backupCtx *BackupContext, record *importer.ScanRecord, chunker *chunkers.Chunker) error {
	vfsCache := backupCtx.vfsCache

	var err error
	var fileEntry *vfs.Entry
	var object *objects.Object
	var objectMAC objects.MAC
	var objectSerialized []byte

	var cachedFileEntry *vfs.Entry
	var cachedFileEntryMAC objects.MAC

	// Check if the file entry and underlying objects are already in the cache
	if !record.IsXattr {
		if data, err := vfsCache.GetFilename(record.Pathname); err != nil {
			snap.Logger().Warn("VFS CACHE: Error getting filename: %v", err)
		} else if data != nil {
			cachedFileEntry, err = vfs.EntryFromBytes(data)
			if err != nil {
				snap.Logger().Warn("VFS CACHE: Error unmarshaling filename: %v", err)
			} else {
				cachedFileEntryMAC = snap.repository.ComputeMAC(data)
				if (record.FileInfo.Size() == -1 && cachedFileEntry.Stat().EqualIgnoreSize(&record.FileInfo)) || cachedFileEntry.Stat().Equal(&record.FileInfo) {
					fileEntry = cachedFileEntry
					if fileEntry.FileInfo.Mode().IsRegular() {
						data, err := vfsCache.GetObject(cachedFileEntry.Object)
						if err != nil {
							snap.Logger().Warn("VFS CACHE: Error getting object: %v", err)
						} else if data != nil {
							cachedObject, err := objects.NewObjectFromBytes(data)
							if err != nil {
								snap.Logger().Warn("VFS CACHE: Error unmarshaling object: %v", err)
							} else {
								object = cachedObject
								objectMAC = snap.repository.ComputeMAC(data)
								objectSerialized = data
							}
						}
					}
				}
			}
		}
	}

	// Chunkify the file if it is a regular file and we don't have a cached object
	if record.FileInfo.Mode().IsRegular() {
		if record.Reader == nil {
			return fmt.Errorf("got a regular file without an associated reader: %s", record.Pathname)
		}

		if object == nil || !snap.repository.BlobExists(resources.RT_OBJECT, objectMAC) {
			var dataSize int64
			object, dataSize, err = snap.chunkify(idx, chunker, record.Pathname, record.Reader)
			if err != nil {
				return err
			}

			// file size may have changed between the scan and chunkify
			record.FileInfo.Lsize = dataSize

			objectSerialized, err = object.Serialize()
			if err != nil {
				return err
			}
			objectMAC = snap.repository.ComputeMAC(objectSerialized)

			if !record.IsXattr {
				if err := vfsCache.PutObject(objectMAC, objectSerialized); err != nil {
					return err
				}
			}

			if err := snap.repository.PutBlobWithHint(idx, resources.RT_OBJECT, objectMAC, objectSerialized); err != nil {
				return err
			}
		}
	}

	// xattrs are a special case
	if record.IsXattr {
		backupCtx.recordXattr(record, objectMAC, object.Size())
		return nil
	}

	if fileEntry == nil || !snap.repository.BlobExists(resources.RT_VFS_ENTRY, cachedFileEntryMAC) {
		fileEntry = vfs.NewEntry(path.Dir(record.Pathname), record)
		if object != nil {
			fileEntry.Object = objectMAC
		}

		serialized, err := fileEntry.ToBytes()
		if err != nil {
			return err
		}

		fileEntryMAC := snap.repository.ComputeMAC(serialized)
		if err := snap.repository.PutBlobWithHint(idx, resources.RT_VFS_ENTRY, fileEntryMAC, serialized); err != nil {
			return err
		}

		// Store the newly generated FileEntry in the cache for future runs
		if err := vfsCache.PutFilename(record.Pathname, serialized); err != nil {
			return err
		}

		fileSummary := &vfs.FileSummary{
			Size:    uint64(record.FileInfo.Size()),
			Mode:    record.FileInfo.Mode(),
			ModTime: record.FileInfo.ModTime().Unix(),
		}
		if object != nil {
			fileSummary.Objects++
			fileSummary.Chunks += uint64(len(object.Chunks))
			fileSummary.ContentType = object.ContentType
			fileSummary.Entropy = object.Entropy
		}

		seralizedFileSummary, err := fileSummary.Serialize()
		if err != nil {
			return err
		}

		if err := vfsCache.PutFileSummary(record.Pathname, seralizedFileSummary); err != nil {
			return err
		}
	}

	if object != nil {
		parts := strings.SplitN(object.ContentType, ";", 2)
		mime := parts[0]

		k := fmt.Sprintf("/%s%s", mime, fileEntry.Path())
		bytes, err := fileEntry.ToBytes()
		if err != nil {
			return err
		}
		if err := backupCtx.indexes[0].ctidx.Insert(k, snap.repository.ComputeMAC(bytes)); err != nil {
			return err
		}
	}

	return backupCtx.recordEntry(fileEntry)
}

func (snap *Builder) persistTrees(backupCtx *BackupContext) (*header.VFS, *vfs.Summary, []header.Index, error) {
	vfsHeader, summary, err := snap.persistVFS(backupCtx)
	if err != nil {
		return nil, nil, nil, err
	}

	indexes, err := snap.persistIndexes(backupCtx)
	if err != nil {
		return nil, nil, nil, err
	}

	return vfsHeader, summary, indexes, nil
}

type FrameHeader struct {
	Type   uint8  // Entry type (directory, file, xattr, etc.)
	Length uint32 // Length of the payload following this header
}

const (
	TypeVFSDirectory = 1
	TypeVFSFile      = 2
)

type chunkifyResult struct {
	obj *objects.Object
	err error
}

func (snap *Builder) persistVFS(backupCtx *BackupContext) (*header.VFS, *vfs.Summary, error) {
	errcsum, err := persistMACIndex(snap, backupCtx.indexes[0].erridx,
		resources.RT_ERROR_BTREE, resources.RT_ERROR_NODE, resources.RT_ERROR_ENTRY)
	if err != nil {
		return nil, nil, err
	}

	filestore := caching.DBStore[string, []byte]{
		Prefix: "__path__",
		Cache:  snap.scanCache,
	}
	fileidx, err := btree.New(&filestore, vfs.PathCmp, 50)
	if err != nil {
		return nil, nil, err
	}

	chunker, err := snap.repository.Chunker(nil)
	if err != nil {
		return nil, nil, err
	}

	var rootSummary *vfs.Summary

	diriter := backupCtx.scanCache.EnumerateKeysWithPrefix(fmt.Sprintf("%s:%s:", "__directory__", "0"), true)
	for dirPath, bytes := range diriter {
		select {
		case <-snap.AppContext().Done():
			return nil, nil, snap.AppContext().Err()
		default:
		}

		pr, pw := io.Pipe()
		resCh := make(chan chunkifyResult, 1)

		go func() {
			obj, _, err := snap.chunkify(-1, chunker, fmt.Sprintf("dirpack:%s", dirPath), pr)
			resCh <- chunkifyResult{obj: obj, err: err}
		}()

		writeFrame := func(typ uint8, data []byte) error {
			hdr := FrameHeader{Type: typ, Length: uint32(len(data))}
			if err := binary.Write(pw, binary.BigEndian, hdr); err != nil {
				return err
			}
			_, err := pw.Write(data)
			return err
		}

		dirEntry, err := vfs.EntryFromBytes(bytes)
		if err != nil {
			return nil, nil, err
		}

		prefix := dirPath
		if prefix != "/" {
			prefix += "/"
		}

		dirHash := snap.repository.GetMACHasher()

		childiter := backupCtx.scanCache.EnumerateKeysWithPrefix(fmt.Sprintf("%s:%s:%s", "__file__", "0", prefix), false)

		for relpath, bytes := range childiter {
			if strings.Contains(relpath, "/") {
				continue
			}

			// bytes is a slice that will be reused in the next iteration,
			// swapping below our feet, so make a copy out of it
			dupBytes := make([]byte, len(bytes))
			copy(dupBytes, bytes)

			childPath := prefix + relpath

			err = writeFrame(TypeVFSFile, dupBytes)
			if err != nil {
				pw.CloseWithError(err)
				return nil, nil, err
			}

			dirHash.Write(bytes)

			if err := fileidx.Insert(childPath, dupBytes); err != nil && err != btree.ErrExists {
				return nil, nil, err
			}

			data, err := backupCtx.vfsCache.GetFileSummary(childPath)
			if err != nil {
				continue
			}

			fileSummary, err := vfs.FileSummaryFromBytes(data)
			if err != nil {
				continue
			}

			dirEntry.Summary.Directory.Children++
			dirEntry.Summary.UpdateWithFileSummary(fileSummary)
		}

		subDirIter := backupCtx.scanCache.EnumerateKeysWithPrefix(fmt.Sprintf("%s:%s:%s", "__directory__", "0", prefix), false)
		for relpath := range subDirIter {
			if relpath == "" || strings.Contains(relpath, "/") {
				continue
			}

			dirHash.Write(bytes)
			err = writeFrame(TypeVFSDirectory, bytes)
			if err != nil {
				pw.CloseWithError(err)
				return nil, nil, err
			}

			childPath := prefix + relpath
			data, err := snap.scanCache.GetSummary(0, childPath)
			if err != nil {
				continue
			}

			childSummary, err := vfs.SummaryFromBytes(data)
			if err != nil {
				continue
			}
			dirEntry.Summary.Directory.Children++
			dirEntry.Summary.Directory.Directories++
			dirEntry.Summary.UpdateBelow(childSummary)
		}

		erriter, err := backupCtx.indexes[0].erridx.ScanFrom(prefix)
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
		logSerialized, err := res.obj.Serialize()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to serialize object for %s: %w", dirPath, err)
		}

		// Compute MAC and insert into prefix index
		dirMac := dirHash.Sum(nil)

		if err := snap.repository.PutBlobIfNotExistsWithHint(0, resources.RT_OBJECT, objects.MAC(dirMac), logSerialized); err != nil {
			return nil, nil, fmt.Errorf("failed to store dirpack object for %s: %w", dirPath, err)
		}

		if err := backupCtx.indexes[0].prefixidx.Insert(dirPath, objects.MAC(dirMac)); err != nil {
			return nil, nil, err
		}
		snap.Logger().Trace("prefixindex", "dir=%s mac=%x", dirPath, dirMac)

		//

		serializedSummary, err := dirEntry.Summary.ToBytes()
		if err != nil {
			backupCtx.recordError(dirPath, err)
			return nil, nil, err
		}

		err = snap.scanCache.PutSummary(0, dirPath, serializedSummary)
		if err != nil {
			backupCtx.recordError(dirPath, err)
			return nil, nil, err
		}

		snap.Event(events.DirectoryOKEvent(snap.Header.Identifier, dirPath))
		if dirPath == "/" {
			if rootSummary != nil {
				panic("double /!")
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

		if err := fileidx.Insert(dirPath, serialized); err != nil && err != btree.ErrExists {
			return nil, nil, err
		}

		if err := backupCtx.recordEntry(dirEntry); err != nil {
			return nil, nil, err
		}
	}

	rootcsum, err := persistIndex(snap, fileidx, resources.RT_VFS_BTREE,
		resources.RT_VFS_NODE, func(data []byte) (objects.MAC, error) {
			return snap.repository.ComputeMAC(data), nil
		})
	if err != nil {
		return nil, nil, err
	}

	xattrcsum, err := persistMACIndex(snap, backupCtx.indexes[0].xattridx,
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

func (snap *Builder) persistIndexes(backupCtx *BackupContext) ([]header.Index, error) {
	ctmac, err := persistIndex(snap, backupCtx.indexes[0].ctidx,
		resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
			return mac, nil
		})
	if err != nil {
		return nil, err
	}

	prefixmac, err := persistIndex(snap, backupCtx.indexes[0].prefixidx,
		resources.RT_BTREE_ROOT, resources.RT_BTREE_NODE, func(mac objects.MAC) (objects.MAC, error) {
			return mac, nil
		})
	if err != nil {
		return nil, err
	}

	return []header.Index{
		{
			Name:  "prefix",
			Type:  "btree",
			Value: prefixmac,
		},
		{
			Name:  "content-type",
			Type:  "btree",
			Value: ctmac,
		},
	}, nil
}
