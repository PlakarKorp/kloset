package vfs

import (
	"bytes"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
)

const (
	dirpackBatchSize     = 64
	dirpackBatchMaxBytes = 8 << 20
)

func (p *dirpackPrefetcher) collect() {
	defer close(p.batches)

	for job := range p.jobs {
		batch := []prefetchJob{job}
	fill:
		for len(batch) < dirpackBatchSize {
			select {
			case j, ok := <-p.jobs:
				if !ok {
					break fill
				}
				batch = append(batch, j)
			default:
				break fill
			}
		}
		p.batches <- batch
	}
}

type pendingDirpack struct {
	done    chan struct{}
	listing *dirpackListing
}

func (fsc *Filesystem) pendingDirpackLoad(path string) *pendingDirpack {
	fsc.pendingMu.Lock()
	defer fsc.pendingMu.Unlock()
	return fsc.pending[path]
}

func (fsc *Filesystem) loadDirpackBatch(jobs []prefetchJob) {
	todo := make([]prefetchJob, 0, len(jobs))
	waits := make(map[string]*pendingDirpack, len(jobs))
	fsc.pendingMu.Lock()
	if fsc.pending == nil {
		fsc.pending = make(map[string]*pendingDirpack)
	}
	for _, j := range jobs {
		if _, ok := fsc.pending[j.path]; ok {
			continue
		}
		if _, ok := fsc.dirpackCache.Get(j.path); ok {
			continue
		}
		w := &pendingDirpack{done: make(chan struct{})}
		fsc.pending[j.path] = w
		waits[j.path] = w
		todo = append(todo, j)
	}
	fsc.pendingMu.Unlock()
	if len(todo) == 0 {
		return
	}

	listings, large := fsc.fetchDirpackListings(todo)
	for _, j := range large {
		if listing, err := fsc.loadDirpackListingByMAC(j.path, j.mac); err == nil {
			listings[j.path] = listing
		}
	}

	for _, j := range todo {
		w := waits[j.path]
		if listing, ok := listings[j.path]; ok {
			if cached, ok := fsc.dirpackCache.Get(j.path); ok {
				listing = cached
			} else {
				_ = fsc.dirpackCache.Put(j.path, listing)
			}
			w.listing = listing
		}
		fsc.pendingMu.Lock()
		delete(fsc.pending, j.path)
		fsc.pendingMu.Unlock()
		close(w.done)
	}
}

func (fsc *Filesystem) fetchDirpackListings(jobs []prefetchJob) (map[string]*dirpackListing, []prefetchJob) {
	ctx := fsc.repo.AppContext()
	opts := &repository.GetBlobsOpts{Concurrency: 4}

	macs := make([]objects.MAC, 0, len(jobs))
	for _, j := range jobs {
		macs = append(macs, j.mac)
	}
	objs, _ := fsc.repo.CollectObjects(ctx, macs, opts)

	var chunkReqs []repository.BlobReq
	var large []prefetchJob
	for _, j := range jobs {
		obj, ok := objs[j.mac]
		if !ok {
			continue
		}
		if obj.Size() > dirpackBatchMaxBytes {
			large = append(large, j)
			delete(objs, j.mac)
			continue
		}
		chunkReqs = append(chunkReqs, repository.ChunkRequests(obj)...)
	}
	data, errs := fsc.repo.CollectBlobs(ctx, chunkReqs, opts)

	listings := make(map[string]*dirpackListing, len(jobs))
	var metaMacs []objects.MAC
	for _, j := range jobs {
		obj, ok := objs[j.mac]
		if !ok {
			continue
		}
		chunks, err := repository.ObjectChunks(obj, data, errs)
		if err != nil {
			continue
		}
		readers := make([]io.Reader, len(chunks))
		for i, c := range chunks {
			readers[i] = bytes.NewReader(c)
		}
		listing, err := decodeDirpackListing(j.path, io.MultiReader(readers...))
		if err != nil {
			continue
		}
		listings[j.path] = listing
		for _, e := range listing.order {
			if needsObjectMetadata(e) {
				metaMacs = append(metaMacs, e.Object)
			}
		}
	}
	if len(metaMacs) == 0 {
		return listings, large
	}

	metaObjs, metaErrs := fsc.repo.CollectObjects(ctx, metaMacs, opts)
	for path, listing := range listings {
		for _, e := range listing.order {
			if !needsObjectMetadata(e) {
				continue
			}
			if metaErrs[e.Object] != nil {
				delete(listings, path)
				break
			}
			setObjectMetadata(e, metaObjs[e.Object])
		}
	}
	return listings, large
}
