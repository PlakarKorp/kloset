package repository

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
)

type BlobReq struct {
	Type resources.Type
	MAC  objects.MAC
}

// Output type of GetBlobs, embedding the requested blob (mac+type), and its
// associated data. If there was an error data is nil and the error is in the
// second part of the iterator.
type BlobResp struct {
	BlobReq
	Data []byte
}

type GetBlobsOpts struct {
	Concurrency uint32 // default=1.
}

func makeDefaultGetBlobsOpts() *GetBlobsOpts {
	return &GetBlobsOpts{
		Concurrency: 1,
	}
}

const (
	// Default maximum range get size, 4 MB
	maxSize = 4 << 20
)

type blobLocation struct {
	BlobReq
	state.Location
}

type rangeRead struct {
	loc   state.Location // Actual range of data to get
	blobs []blobLocation // blobs covered by that range.
}

func (r *Repository) GetBlobs(ctx context.Context, blobs []BlobReq, opts *GetBlobsOpts) iter.Seq2[BlobResp, error] {
	if opts == nil {
		opts = makeDefaultGetBlobsOpts()
	}

	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}

	return func(yield func(BlobResp, error) bool) {
		t0 := time.Now()

		locs := make([]blobLocation, 0, len(blobs))
		seen := make(map[BlobReq]struct{})

		for _, b := range blobs {
			if _, ok := seen[b]; ok {
				continue
			}

			seen[b] = struct{}{}

			loc, ok, err := r.state.GetSubpartForBlob(b.Type, b.MAC)
			if err == nil && !ok {
				err = ErrBlobNotFound
			}

			if err != nil {
				if !yield(BlobResp{b, nil}, err) {
					return
				}

				continue
			}

			locs = append(locs, blobLocation{b, loc})
		}

		plan := readPlanner(locs, maxSize, 0.0)
		defer func() {
			r.Logger().Trace("repository", "GetBlobs(%d blobs): %d reads: %s",
				len(blobs), len(plan), time.Since(t0))
		}()

		// Derived context to handle cancellation from the iterator loop,
		// otherwise an early exit will leak all goroutines.
		innerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Feed the read plans to the workers.
		readsChannel := make(chan *rangeRead)
		go func() {
			defer close(readsChannel)

			for _, p := range plan {
				select {
				case readsChannel <- p:
				case <-innerCtx.Done():
					return
				}

			}
		}()

		type iterItem struct {
			b   BlobResp
			err error
		}

		// Workers read the plan in input and stack them on a return channel to
		// be delivered.
		returnChannel := make(chan iterItem, opts.Concurrency)
		var wg sync.WaitGroup
		for range opts.Concurrency {
			wg.Go(func() {
				for rp := range readsChannel {
					data, err := r.GetPackfileRange(rp.loc)

					for _, b := range rp.blobs {
						it := iterItem{b: BlobResp{b.BlobReq, nil}}
						if err != nil {
							it.err = err
						} else {
							blobStart := b.Offset - rp.loc.Offset
							blobBytes := data[blobStart : blobStart+uint64(b.Length)]
							it.b.Data, it.err = r.decodeBuffer(blobBytes)
						}

						select {
						case returnChannel <- it:
						case <-innerCtx.Done():
							return
						}
					}
				}
			})
		}

		go func() {
			wg.Wait()

			close(returnChannel)
		}()

		// Finally yield the results to the caller.
		for ret := range returnChannel {
			if !yield(ret.b, ret.err) {
				return
			}
		}
	}
}

// readPlanner tries to merge reads according to the policy given.
func readPlanner(locs []blobLocation, maxRdSize uint32, _ float64) []*rangeRead {
	if len(locs) == 0 {
		return nil
	}

	// First sort by packfile then offset.
	slices.SortFunc(locs, func(a, b blobLocation) int {
		if c := bytes.Compare(a.Packfile[:], b.Packfile[:]); c == 0 {
			return cmp.Compare(a.Offset, b.Offset)
		} else {
			return c
		}
	})

	plan := make([]*rangeRead, 0)

	// append the first blob to simplify the following loop
	rr := &rangeRead{
		loc:   locs[0].Location,
		blobs: []blobLocation{locs[0]},
	}
	plan = append(plan, rr)

	for _, l := range locs[1:] {
		curMerge := plan[len(plan)-1]

		if curMerge.loc.Packfile == l.Packfile {
			size := (l.Offset + uint64(l.Length)) - curMerge.loc.Offset
			if size <= uint64(maxRdSize) {
				curMerge.loc.Length = uint32(size)
				curMerge.blobs = append(curMerge.blobs, l)
				continue
			}
		}

		rr = &rangeRead{
			loc:   l.Location,
			blobs: []blobLocation{l},
		}
		plan = append(plan, rr)
	}

	return plan
}

func (r *Repository) CollectBlobs(ctx context.Context, reqs []BlobReq, opts *GetBlobsOpts) (map[objects.MAC][]byte, map[objects.MAC]error) {
	data := make(map[objects.MAC][]byte, len(reqs))
	errs := make(map[objects.MAC]error)
	for resp, err := range r.GetBlobs(ctx, reqs, opts) {
		if err != nil {
			errs[resp.MAC] = err
			continue
		}
		data[resp.MAC] = resp.Data
	}

	for _, req := range reqs {
		if _, ok := data[req.MAC]; ok {
			continue
		}
		if _, ok := errs[req.MAC]; ok {
			continue
		}
		err := ctx.Err()
		if err == nil {
			err = fmt.Errorf("blob %x was not returned", req.MAC)
		}
		errs[req.MAC] = err
	}
	return data, errs
}

func (r *Repository) CollectObjects(ctx context.Context, macs []objects.MAC, opts *GetBlobsOpts) (map[objects.MAC]*objects.Object, map[objects.MAC]error) {
	reqs := make([]BlobReq, 0, len(macs))
	for _, mac := range macs {
		reqs = append(reqs, BlobReq{Type: resources.RT_OBJECT, MAC: mac})
	}
	data, errs := r.CollectBlobs(ctx, reqs, opts)

	objs := make(map[objects.MAC]*objects.Object, len(data))
	for mac, buf := range data {
		obj, err := objects.NewObjectFromBytes(buf)
		if err != nil {
			errs[mac] = err
			continue
		}
		objs[mac] = obj
	}
	return objs, errs
}

func ChunkRequests(obj *objects.Object) []BlobReq {
	reqs := make([]BlobReq, 0, len(obj.Chunks))
	for _, c := range obj.Chunks {
		reqs = append(reqs, BlobReq{Type: resources.RT_CHUNK, MAC: c.ContentMAC})
	}
	return reqs
}

func ObjectChunks(obj *objects.Object, data map[objects.MAC][]byte, errs map[objects.MAC]error) ([][]byte, error) {
	chunks := make([][]byte, 0, len(obj.Chunks))
	for _, c := range obj.Chunks {
		if err := errs[c.ContentMAC]; err != nil {
			return nil, err
		}
		chunks = append(chunks, data[c.ContentMAC])
	}
	return chunks, nil
}
