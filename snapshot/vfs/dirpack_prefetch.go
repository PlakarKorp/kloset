package vfs

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

func (fsc *Filesystem) PrefetchDirs(ctx context.Context, dirs []string) error {
	if fsc.dirpack == nil || fsc.dirpackCache == nil {
		return nil
	}

	t0 := time.Now()
	warmed := 0
	var findsDuration time.Duration
	defer func() {
		fsc.repo.Logger().Trace("vfs", "PrefetchDirs(%d dirs): %d warmed: finds %s, total %s",
			len(dirs), warmed, findsDuration, time.Since(t0))
	}()

	// Phase 1 dirs -> object MAC, parallelized
	cached := 0
	reqs := map[string]repository.BlobReq{}
	reqsMtx := sync.Mutex{}
	toFind := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if _, ok := fsc.dirpackCache.Get(dir); ok {
			cached++
			continue
		}
		toFind = append(toFind, dir)
	}

	/*
		var eg errgroup.Group
		eg.SetLimit(16)


		for _, dir := range dirs {
			if _, ok := fsc.dirpackCache.Get(dir); ok {
				cached++
				continue
			}

			eg.Go(func() error {
				mac, found, err := fsc.dirpack.Find(dir)
				if err != nil || !found {
					return nil
				}

				reqsMtx.Lock()
				reqs[dir] = repository.BlobReq{
					Type: resources.RT_OBJECT,
					MAC:  mac,
				}
				reqsMtx.Unlock()

				return nil
			})
		}

		eg.Wait()
	*/

	const shards = 32
	var wg sync.WaitGroup
	per := (len(toFind) + shards - 1) / shards
	for start := 0; start < len(toFind); start += per {
		chunk := toFind[start:min(start+per, len(toFind))]
		wg.Go(func() {
			for _, dir := range chunk {
				mac, found, err := fsc.dirpack.Find(dir)
				if err != nil || !found {
					continue
				}
				reqsMtx.Lock()
				reqs[dir] = repository.BlobReq{Type: resources.RT_OBJECT, MAC: mac}
				reqsMtx.Unlock()
			}
		})
	}
	wg.Wait()

	findsDuration = time.Since(t0)
	warmed = cached // already-warm dirs count as warmed for the trace

	if len(reqs) == 0 {
		return nil
	}

	// Phase 2 batch resolve RT_OBJECT->dirpack
	objs := map[objects.MAC]*objects.Object{}
	chunksReq := []repository.BlobReq{}
	for b, err := range fsc.repo.GetBlobs(ctx, slices.Collect(maps.Values(reqs)), &repository.GetBlobsOpts{Concurrency: 32}) {
		if err != nil {
			// Soft error
			continue
		}

		if obj, err := objects.NewObjectFromBytes(b.Data); err == nil {
			objs[b.MAC] = obj

			for _, c := range obj.Chunks {
				chunksReq = append(chunksReq, repository.BlobReq{
					Type: resources.RT_CHUNK,
					MAC:  c.ContentMAC,
				})
			}
		}
	}

	// Phase 3 batch resolve the Content of dirpacks
	chunks := map[objects.MAC][]byte{}
	for b, err := range fsc.repo.GetBlobs(ctx, chunksReq, &repository.GetBlobsOpts{Concurrency: 32}) {
		if err != nil {
			// Soft error
			continue
		}

		chunks[b.MAC] = b.Data
	}

	// Phase 4 assemble everything, we have to go through reqs to tie everything
	// together because GetBlobs only return mac->[]byte and we are lacking the
	// original string.
Outer:
	for path, req := range reqs {
		if obj, ok := objs[req.MAC]; ok {
			dirpackData := []byte{}

			for _, c := range obj.Chunks {
				if cData, ok := chunks[c.ContentMAC]; ok {
					dirpackData = append(dirpackData, cData...)
				} else {
					continue Outer
				}
			}

			if _, err, _ := fsc.dirpackSF.Do(path, func() (any, error) {
				if m, ok := fsc.dirpackCache.Get(path); ok {
					return m, nil
				}
				m, err := fsc.decodeDirpackMap(bytes.NewReader(dirpackData))
				if err == nil {
					fsc.dirpackCache.Put(path, m)
				}

				return m, err
			}); err == nil {
				warmed++
			}
		}
	}

	return nil
}
