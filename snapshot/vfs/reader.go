package vfs

import (
	"bytes"
	"io"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

const prefetchSize = 20 * 1024 * 1024

type ObjectReader struct {
	object *objects.Object
	repo   *repository.Repository
	size   int64

	objoff int
	off    int64
	rd     io.ReadSeeker

	dirty          bool
	prefetchBuffer bytes.Buffer
}

func NewObjectReader(repo *repository.Repository, object *objects.Object, size int64) *ObjectReader {
	return &ObjectReader{
		object:         object,
		repo:           repo,
		size:           size,
		dirty:          true,
		prefetchBuffer: bytes.Buffer{},
	}
}

func (or *ObjectReader) prefetch() error {
	if or.prefetchBuffer.Len() == 0 {
		or.dirty = true
	}

	if !or.dirty {
		return nil
	}

	if or.objoff >= len(or.object.Chunks) {
		return nil
	}

	t0 := time.Now()
	defer func() {
		or.repo.AppContext().GetLogger().Trace("repository", "object-rd-prefect %x %v", or.object.ContentMAC, time.Since(t0))
	}()

	for data, err := range or.repo.GetObjectContent(or.object, or.objoff, prefetchSize) {
		if err != nil {
			return err
		}

		if _, err := or.prefetchBuffer.Write(data); err != nil {
			return err
		}
		or.objoff++
	}

	or.dirty = false
	return nil
}

func (or *ObjectReader) Read(p []byte) (int, error) {
	if err := or.prefetch(); err != nil {
		return 0, err
	}
	return or.prefetchBuffer.Read(p)
}

func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	chunks := or.object.Chunks

	savedOffset := or.off

	switch whence {
	case io.SeekStart:
		or.rd = nil
		or.off = 0
		for or.objoff = 0; or.objoff < len(chunks); or.objoff++ {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off += clen
				offset -= clen
				continue
			}
			or.off += offset
			rd, err := or.repo.GetBlob(resources.RT_CHUNK,
				chunks[or.objoff].ContentMAC)
			if err != nil {
				return 0, err
			}
			if _, err := rd.Seek(offset, whence); err != nil {
				return 0, err
			}
			or.rd = rd
			break
		}

	case io.SeekEnd:
		or.rd = nil
		or.off = or.size
		for or.objoff = len(chunks) - 1; or.objoff >= 0; or.objoff-- {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off -= clen
				offset -= clen
				continue
			}
			or.off -= offset
			rd, err := or.repo.GetBlob(resources.RT_CHUNK,
				chunks[or.objoff].ContentMAC)
			if err != nil {
				return 0, err
			}
			if _, err := rd.Seek(offset, whence); err != nil {
				return 0, err
			}
			or.rd = rd
			break
		}

	case io.SeekCurrent:
		if or.rd != nil {
			n, err := or.rd.Seek(offset, whence)
			if err != nil {
				return 0, err
			}
			diff := n - or.off
			or.off += diff
			offset -= diff
		}

		if offset == 0 {
			break
		}

		or.objoff++
		for or.objoff < len(chunks) {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off += clen
				offset -= clen
			}
			or.off += offset
			rd, err := or.repo.GetBlob(resources.RT_CHUNK,
				chunks[or.objoff].ContentMAC)
			if err != nil {
				return 0, err
			}
			if _, err := rd.Seek(offset, whence); err != nil {
				return 0, err
			}
			or.rd = rd
		}
	}

	if savedOffset != or.off {
		or.dirty = true
	}

	return or.off, nil
}
