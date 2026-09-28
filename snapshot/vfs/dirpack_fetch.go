package vfs

import (
	"context"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

// dirpackFetchBudget caps the payload bytes in flight per chunk round so a
// batch of large directories cannot grow memory with the source size.
const dirpackFetchBudget = 32 << 20

func (fsc *Filesystem) dirpackFetchConcurrency() uint32 {
	n := fsc.repo.AppContext().MaxConcurrency
	if n > 16 {
		n = 16
	}
	if n < 1 {
		n = 1
	}
	return uint32(n)
}

// fetchDirpacks resolves the payloads of the given dirpack objects in two
// batched rounds coalesced through GetBlobs: the RT_OBJECT descriptors
// first, then their content chunks, the latter in byte-budgeted waves.
//
// It is best effort: a directory whose payload could not be fetched — an
// error, or a single payload above the budget — is absent from the result
// and the caller is expected to load it on demand.
func (fsc *Filesystem) fetchDirpacks(ctx context.Context, reqs map[string]objects.MAC) map[string][]byte {
	t0 := time.Now()
	res := make(map[string][]byte, len(reqs))
	defer func() {
		fsc.repo.Logger().Trace("vfs", "fetchDirpacks(%d dirs): %d fetched in %s",
			len(reqs), len(res), time.Since(t0))
	}()

	opts := &repository.GetBlobsOpts{Concurrency: fsc.dirpackFetchConcurrency()}

	objReqs := make([]repository.BlobReq, 0, len(reqs))
	for _, mac := range reqs {
		objReqs = append(objReqs, repository.BlobReq{Type: resources.RT_OBJECT, MAC: mac})
	}

	objs := make(map[objects.MAC]*objects.Object, len(reqs))
	for b, err := range fsc.repo.GetBlobs(ctx, objReqs, opts) {
		if err != nil {
			continue
		}
		obj, err := objects.NewObjectFromBytes(b.Data)
		if err != nil {
			continue
		}
		objs[b.MAC] = obj
	}

	type pending struct {
		path string
		obj  *objects.Object
		size int64
	}
	var (
		wave      []pending
		waveBytes int64
		chunkReqs []repository.BlobReq
	)

	flush := func() {
		if len(wave) == 0 {
			return
		}
		chunks := make(map[objects.MAC][]byte, len(chunkReqs))
		for b, err := range fsc.repo.GetBlobs(ctx, chunkReqs, opts) {
			if err != nil {
				continue
			}
			chunks[b.MAC] = b.Data
		}
	assemble:
		for _, p := range wave {
			data := make([]byte, 0, p.size)
			for _, c := range p.obj.Chunks {
				cdata, ok := chunks[c.ContentMAC]
				if !ok {
					continue assemble
				}
				data = append(data, cdata...)
			}
			res[p.path] = data
		}
		wave = wave[:0]
		chunkReqs = chunkReqs[:0]
		waveBytes = 0
	}

	for path, mac := range reqs {
		obj, ok := objs[mac]
		if !ok {
			continue
		}
		var size int64
		for _, c := range obj.Chunks {
			size += int64(c.Length)
		}
		if size > dirpackFetchBudget {
			continue
		}
		if waveBytes+size > dirpackFetchBudget {
			flush()
		}
		wave = append(wave, pending{path: path, obj: obj, size: size})
		waveBytes += size
		for _, c := range obj.Chunks {
			chunkReqs = append(chunkReqs, repository.BlobReq{Type: resources.RT_CHUNK, MAC: c.ContentMAC})
		}
	}
	flush()

	return res
}
