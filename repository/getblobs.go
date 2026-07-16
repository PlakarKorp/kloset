package repository

import (
	"bytes"
	"cmp"
	"context"
	"iter"
	"slices"
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

		for _, g := range plan {
			data, err := r.GetPackfileRange(g.loc)

			for _, b := range g.blobs {
				if err != nil {
					if !yield(BlobResp{b.BlobReq, nil}, err) {
						return
					}

					continue
				}

				blobStart := b.Offset - g.loc.Offset
				blobBytes := data[blobStart : blobStart+uint64(b.Length)]
				blobData, err := r.decodeBuffer(blobBytes)
				if err != nil {
					if !yield(BlobResp{b.BlobReq, nil}, err) {
						return
					}

					continue
				}

				if !yield(BlobResp{b.BlobReq, blobData}, nil) {
					return
				}
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
