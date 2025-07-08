package vfs

import (
	"errors"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

type ObjectReader struct {
	object *objects.Object
	repo   *repository.Repository
	size   int64

	objoff int
	off    int64
	rd     io.ReadSeeker
}

func NewObjectReader(repo *repository.Repository, object *objects.Object, size int64) *ObjectReader {
	return &ObjectReader{
		object: object,
		repo:   repo,
		size:   size,
	}
}

func (or *ObjectReader) Read(p []byte) (int, error) {
	for or.objoff < len(or.object.Chunks) {
		if or.rd == nil {
			rd, err := or.repo.GetBlob(resources.RT_CHUNK,
				or.object.Chunks[or.objoff].ContentMAC)
			if err != nil {
				return 0, err
			}
			or.rd = rd
		}

		n, err := or.rd.Read(p)
		if errors.Is(err, io.EOF) {
			or.objoff++
			or.rd = nil
			continue
		}
		or.off += int64(n)
		return n, err
	}

	return 0, io.EOF
}

func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	chunks := or.object.Chunks

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

	return or.off, nil
}
