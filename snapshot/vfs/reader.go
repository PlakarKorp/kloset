package vfs

import (
	"bytes"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
)

const prefetchSize = 20 * 1024 * 1024

type ObjectReader struct {
	object *objects.Object
	repo   *repository.Repository
	size   int64

	objoff       int   // Chunk offset at which we currently are reading.
	bufferOffset int64 // Offset inside the chunk we are currently reading.
	off          int64 // Global file offset.

	doSeek bool // Flag to invalidate the prefetchBuffer

	prefetchedObjoff int // Keeps track of the chunk position we prefetched up to.
	prefetchBuffer   bytes.Buffer
}

func NewObjectReader(repo *repository.Repository, object *objects.Object, size int64) *ObjectReader {
	return &ObjectReader{
		object:         object,
		repo:           repo,
		size:           size,
		prefetchBuffer: bytes.Buffer{},
	}
}

func (or *ObjectReader) prefetch() error {
	var seekBytes int64
	if or.doSeek {
		or.prefetchBuffer.Reset()
		or.prefetchedObjoff = or.objoff
		seekBytes = or.bufferOffset
	}

	// We are not seeking and we have data in the prefectBuffer just use it.
	if or.prefetchBuffer.Len() != 0 {
		return nil
	}

	// We are past the last chunk, and not seeking so it's the end of the file.
	if or.prefetchedObjoff >= len(or.object.Chunks) {
		return io.EOF
	}

	for data, err := range or.repo.GetObjectContent(or.object, or.prefetchedObjoff, prefetchSize) {
		if err != nil {
			return err
		}

		if seekBytes != 0 {
			data = data[seekBytes:]
			seekBytes = 0
		}

		if _, err := or.prefetchBuffer.Write(data); err != nil {
			return err
		}
		or.prefetchedObjoff++
	}

	return nil
}

func (or *ObjectReader) Read(p []byte) (int, error) {
	if err := or.prefetch(); err != nil {
		return 0, err
	}

	read, err := or.prefetchBuffer.Read(p)
	// (Ab)use Seek to keep the offset and objoff up to date according to the
	// Read we did.
	or.Seek(int64(read), io.SeekCurrent)
	// It is not a real seek, we did not move.
	or.doSeek = false

	// EOF on the prefetchBuffer just means we exhausted it not the underlying
	// file
	if err == io.EOF {
		err = nil
	}

	return read, err
}

func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	chunks := or.object.Chunks

	switch whence {
	case io.SeekStart:
		or.off = 0

		for or.objoff = 0; or.objoff < len(chunks); or.objoff++ {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off += clen
				offset -= clen
				continue
			}

			or.bufferOffset = offset
			or.off += offset
			or.doSeek = true

			break
		}

	case io.SeekEnd:
		or.off = or.size
		for or.objoff = len(chunks) - 1; or.objoff >= 0; or.objoff-- {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off -= clen
				offset -= clen
				continue
			}
			or.bufferOffset = clen - offset
			or.off -= offset
			or.doSeek = true
			break
		}

	case io.SeekCurrent:
		if offset == 0 {
			break
		}

		left := int64(chunks[or.objoff].Length) - or.bufferOffset
		if left > offset {
			or.bufferOffset += offset
			or.off += offset
			or.doSeek = true
			break
		}

		offset -= left
		for or.objoff += 1; or.objoff < len(chunks); or.objoff++ {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off += clen
				offset -= clen
				continue
			}
			or.off += offset
			or.bufferOffset = offset
			or.doSeek = true
			break
		}
	}

	return or.off, nil
}
