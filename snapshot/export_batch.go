package snapshot

import (
	"bytes"
	"io"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

var (
	exportBatchFiles     = 512      // files per batch
	exportBatchBytes     = 32 << 20 // file bytes per batch
	exportSmallFileBytes = 1 << 20  // files up to this size are read in batch
)

const exportBatchesInFlight = 3

type exportFile struct {
	entry  *vfs.Entry
	chunks [][]byte // file content, in order, when the batch read it
	err    error    // fetch error, returned when the record is opened
}

type exportBatch struct {
	records []*connectors.Record
	files   []*exportFile
	size    int64
	done    chan struct{} // closed when the fetch is over
}

func newExportBatch() *exportBatch {
	return &exportBatch{done: make(chan struct{})}
}

func (b *exportBatch) full() bool {
	return len(b.files) >= exportBatchFiles ||
		len(b.records) >= 4*exportBatchFiles ||
		b.size >= int64(exportBatchBytes)
}

func (snap *Snapshot) fetchBatch(b *exportBatch) {
	defer close(b.done)
	if len(b.files) == 0 {
		return
	}

	if err := snap.repository.CheckReadable(); err != nil {
		for _, f := range b.files {
			f.err = err
		}
		return
	}

	ctx := snap.AppContext()
	opts := &repository.GetBlobsOpts{Concurrency: uint32(max(ctx.MaxConcurrency, 1))}

	var macs []objects.MAC
	for _, f := range b.files {
		if f.entry.HasObject() && f.entry.ResolvedObject == nil {
			macs = append(macs, f.entry.Object)
		}
	}
	objs, objErrs := snap.repository.CollectObjects(ctx, macs, opts)

	var chunkReqs []repository.BlobReq
	var small []*exportFile
	for _, f := range b.files {
		e := f.entry
		if !e.HasObject() {
			continue
		}
		if e.ResolvedObject == nil {
			if f.err = objErrs[e.Object]; f.err != nil {
				continue
			}
			e.ResolvedObject = objs[e.Object]
		}
		if e.ResolvedObject.Size() > int64(exportSmallFileBytes) {
			continue
		}
		small = append(small, f)
		chunkReqs = append(chunkReqs, repository.ChunkRequests(e.ResolvedObject)...)
	}
	if len(chunkReqs) == 0 {
		return
	}

	data, errs := snap.repository.CollectBlobs(ctx, chunkReqs, opts)
	for _, f := range small {
		f.chunks, f.err = repository.ObjectChunks(f.entry.ResolvedObject, data, errs)
	}
}

func (f *exportFile) open(pvfs *vfs.Filesystem) (io.ReadCloser, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.chunks != nil {
		readers := make([]io.Reader, len(f.chunks))
		for i, c := range f.chunks {
			readers[i] = bytes.NewReader(c)
		}
		f.chunks = nil
		return io.NopCloser(io.MultiReader(readers...)), nil
	}
	return f.entry.Open(pvfs)
}
