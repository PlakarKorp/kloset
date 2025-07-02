package vfs

import (
	"errors"
	"io"
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

const prefetchCnt = 10

type prefetchedChk struct {
	rd  io.ReadSeeker
	err error
}

type ObjectReader struct {
	object *objects.Object
	repo   *repository.Repository
	size   int64

	objoff  int
	pobjoff int // offset into the rds
	off     int64
	rds     []prefetchedChk
}

func NewObjectReader(repo *repository.Repository, object *objects.Object, size int64) *ObjectReader {
	return &ObjectReader{
		object: object,
		repo:   repo,
		size:   size,
	}
}

func (or *ObjectReader) prefetch(n, offset int) []prefetchedChk {
	ret := make([]prefetchedChk, n)
	wg := sync.WaitGroup{}
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rd, err := or.repo.GetBlob(resources.RT_CHUNK, or.object.Chunks[or.objoff+i].ContentMAC)

			ret[i] = prefetchedChk{rd, err}
		}(i)
	}

	wg.Wait()
	return ret
}

func (or *ObjectReader) Read(p []byte) (int, error) {
	for or.objoff < len(or.object.Chunks) {
		if or.rds == nil {
			or.pobjoff = 0
			or.rds = or.prefetch(min(prefetchCnt, len(or.object.Chunks)-or.objoff), or.objoff)
		}

		for i := or.pobjoff; i < len(or.rds); i++ {
			chk := or.rds[i]
			if chk.err != nil {
				return -1, chk.err
			}

			n, err := chk.rd.Read(p)
			if errors.Is(err, io.EOF) {
				or.objoff++
				or.pobjoff++

				// We consummed all the prefetched blobs signal it for next loop
				if or.pobjoff == len(or.rds) {
					or.rds = nil
				}

				continue
			}

			return n, err
		}
	}

	return 0, io.EOF
}

func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	panic("FOOBARED")
}
