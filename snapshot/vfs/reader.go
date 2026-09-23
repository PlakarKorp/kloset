package vfs

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
)

const default_prefetchSize = 4 * 1024 * 1024

// sequentialWindows is how many prefetch windows a sequential reader keeps in
// flight.  Hardcoded, and per reader, until the budget is shared.
const sequentialWindows = 8

type ObjectReader struct {
	object *objects.Object
	repo   *repository.Repository
	size   int64

	objoff       int   // Chunk offset at which we currently are reading.
	bufferOffset int64 // Offset inside the chunk we are currently reading.
	off          int64 // Global file offset.

	doSeek bool // Flag to invalidate the prefetchBuffer

	prefetchSize     int32
	prefetchedObjoff int // Keeps track of the chunk position we prefetched up to.
	prefetchBuffer   bytes.Buffer

	windows int        // read-ahead depth, 0 reads one window at a time.
	ra      *readahead // started on the first Read when windows > 0.
}

func NewObjectReader(repo *repository.Repository, object *objects.Object, size int64, prefetchSize int32) *ObjectReader {
	if prefetchSize <= 0 {
		prefetchSize = default_prefetchSize
	}
	return &ObjectReader{
		object:         object,
		repo:           repo,
		size:           size,
		prefetchSize:   prefetchSize,
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

	for data, err := range or.repo.GetObjectContent(or.object, or.prefetchedObjoff, uint32(or.prefetchSize)) {
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
	if or.windows > 0 && or.ra == nil {
		or.ra = or.startReadahead()
	}
	if or.ra != nil {
		n, err := or.ra.read(p)
		or.advance(n)
		return n, err
	}

	if err := or.prefetch(); err != nil {
		return 0, err
	}

	read, err := or.prefetchBuffer.Read(p)
	or.advance(read)

	// EOF on the prefetchBuffer just means we exhausted it not the underlying
	// file
	if err == io.EOF {
		err = nil
	}

	return read, err
}

// advance keeps objoff and bufferOffset in step with the n bytes just read.
func (or *ObjectReader) advance(n int) {
	or.seek(int64(n), io.SeekCurrent)
	// It is not a real seek, we did not move.
	or.doSeek = false
}

// Seek stops any read-ahead: from here on, reads are served one window at a
// time from the new position.
func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	if or.ra != nil {
		or.ra.stop()
		or.ra = nil
		// the serial path has nothing buffered: restart it from here.
		or.doSeek = true
	}
	or.windows = 0
	return or.seek(offset, whence)
}

// Close stops any read-ahead in flight.
func (or *ObjectReader) Close() error {
	if or.ra != nil {
		or.ra.stop()
		or.ra = nil
	}
	or.windows = 0
	return nil
}

func (or *ObjectReader) seek(offset int64, whence int) (int64, error) {
	chunks := or.object.Chunks

	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, os.ErrInvalid
		}

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
		if offset > 0 {
			return 0, os.ErrInvalid
		}

		if offset == 0 {
			or.objoff = len(chunks)
			or.bufferOffset = 0
			or.off = or.size
			or.doSeek = true
			break
		}

		offset *= -1
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

		if offset > 0 {
			var left int64
			if or.objoff < len(chunks) {
				left = int64(chunks[or.objoff].Length) - or.bufferOffset
			}
			if left > offset {
				or.bufferOffset += offset
				or.off += offset
				or.doSeek = true
				break
			}

			or.off += left
			offset -= left

			if offset == 0 {
				or.objoff = len(chunks)
				or.bufferOffset = 0
				or.off = or.size
				or.doSeek = true
				break
			}

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
		} else {
			offset *= -1

			left := or.bufferOffset
			if left > offset {
				or.bufferOffset -= offset
				or.off -= offset
				or.doSeek = true
				break
			}

			or.off -= left
			offset -= left
			for or.objoff -= 1; or.objoff >= 0; or.objoff-- {
				clen := int64(chunks[or.objoff].Length)
				if offset > clen {
					or.off -= clen
					offset -= clen
					continue
				}
				or.off -= offset
				or.bufferOffset = clen - offset
				or.doSeek = true
				break
			}
		}

	}

	return or.off, nil
}

func (or *ObjectReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if off >= or.size {
		return 0, io.EOF
	}

	cr := NewObjectReader(or.repo, or.object, or.size, or.prefetchSize)
	if _, err := cr.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	n, err := io.ReadFull(cr, p)
	if err == io.ErrUnexpectedEOF {
		return n, io.EOF
	}
	return n, err
}

type window struct {
	data []byte
	err  error
}

// readahead fetches and decodes up to cap(slots) windows ahead of the reader,
// each in its own goroutine, and hands them over in file order.
type readahead struct {
	ctx     context.Context
	cancel  context.CancelFunc
	pending []chan window // one per window, in file order.
	slots   chan struct{} // windows fetched or in flight, not yet consumed.
	next    int
	skip    int64 // bytes to drop from the first window.
	buf     []byte
	err     error
}

func (or *ObjectReader) startReadahead() *readahead {
	chunks := or.object.Chunks

	// Cut windows on decoded size, the same budget as the serial path.
	var bounds [][2]int
	for i := or.objoff; i < len(chunks); {
		start := i
		var size int64
		for i < len(chunks) && size < int64(or.prefetchSize) {
			size += int64(chunks[i].Length)
			i++
		}
		bounds = append(bounds, [2]int{start, i})
	}

	ctx, cancel := context.WithCancel(or.repo.AppContext())
	ra := &readahead{
		ctx:     ctx,
		cancel:  cancel,
		pending: make([]chan window, len(bounds)),
		slots:   make(chan struct{}, or.windows),
		skip:    or.bufferOffset,
	}
	for i := range ra.pending {
		// buffered, so a fetch never blocks on a reader that went away.
		ra.pending[i] = make(chan window, 1)
	}

	go func() {
		for i, b := range bounds {
			select {
			case ra.slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func() {
				data, err := or.fetchWindow(ctx, b[0], b[1])
				ra.pending[i] <- window{data, err}
			}()
		}
	}()

	return ra
}

func (or *ObjectReader) fetchWindow(ctx context.Context, start, end int) ([]byte, error) {
	var size int64
	for _, c := range or.object.Chunks[start:end] {
		size += int64(c.Length)
	}

	data := make([]byte, 0, size)
	for chunk, err := range or.repo.GetObjectChunks(or.object, start, end) {
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data = append(data, chunk...)
	}
	return data, nil
}

func (ra *readahead) read(p []byte) (int, error) {
	for len(ra.buf) == 0 {
		if ra.err != nil {
			return 0, ra.err
		}
		if ra.next == len(ra.pending) {
			ra.cancel()
			return 0, io.EOF
		}

		var w window
		select {
		case w = <-ra.pending[ra.next]:
		case <-ra.ctx.Done():
			ra.err = ra.ctx.Err()
			return 0, ra.err
		}
		<-ra.slots
		ra.next++

		if w.err != nil {
			ra.err = w.err
			ra.cancel()
			return 0, ra.err
		}
		ra.buf = w.data[ra.skip:]
		ra.skip = 0
	}

	n := copy(p, ra.buf)
	ra.buf = ra.buf[n:]
	return n, nil
}

func (ra *readahead) stop() {
	ra.cancel()
}
