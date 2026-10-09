package vfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

const (
	default_prefetchSize = 4 * 1024 * 1024
	maxReadahead         = 32 << 20 // Max readahead segment size
	readaheadConcurrency = 16       // Concurrency of GetBlobs for the ReadAhead segment
)

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

	// Readahead part, when readahead is requested through the constructor
	readahead bool
	ra        *readahead
}

// Object reader exposes an io.ReadCloser like interface to read from VFS
// objects transparently.
// By default it uses a prefetcher to read bigger span at a time.
// Note that this object is not threadsafe.
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

// Same object, but spawns a goroutine in the background to do readahead in
// order to gain better performances when you know you are going to read the
// full file sequentially.
func NewReadaheadObjectReader(repo *repository.Repository, object *objects.Object, size int64) *ObjectReader {
	or := NewObjectReader(repo, object, size, -1)
	or.readahead = true
	return or
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
	if or.readahead {
		if or.ra == nil {
			if or.off != 0 {
				// Seeked before the first read: plain path.
				or.readahead = false
				return or.Read(p)
			}
			or.ra = newReadahead(or, int(or.prefetchSize))
		}

		n, err := or.ra.Read(p)
		if err != nil && err != io.EOF {
			// A retry goes through the plain path, from where we failed.
			or.stopReadahead()
		}
		return n, err
	}

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
	if or.ra != nil {
		// For now using Seek just disable readahead alltogether.
		// Revisit later when it's stabilised.
		or.stopReadahead()
	}

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

func (or *ObjectReader) stopReadahead() {
	or.readahead = false
	if or.ra == nil {
		return
	}

	ra := or.ra
	or.ra = nil
	ra.close()

	// Cannot fail
	or.Seek(ra.off, io.SeekStart)
}

func (or *ObjectReader) Close() error {
	or.stopReadahead()
	return nil
}

// readahead reads the object sequentially, on the first read the readahead
// mechanism kicks ahead and starts reading the next window with double the size
// up to maxReadahead
type readahead struct {
	o *ObjectReader

	off     int64    // object offset of the next byte Read returns
	buf     []byte   // unread part of the current chunk
	pending [][]byte // next chunks of the current window
	size    int      // size of the last window issued
	next    *raWindow
}

// raWindow is the next window of data being currently fetched by the background
// goroutine
type raWindow struct {
	start, end int
	bytes      int

	// The three next fields are valid once done is closed.
	data    [][]byte
	err     error
	elapsed time.Duration

	cancel context.CancelFunc
	done   chan struct{}
}

// newReadahead starts reading o from its beginning.
func newReadahead(o *ObjectReader, size int) *readahead {
	ra := &readahead{
		o:    o,
		size: size,
	}

	if len(o.object.Chunks) > 0 {
		ra.next = ra.startWindow(0, size)
	}
	return ra
}

func (ra *readahead) Read(p []byte) (int, error) {
	for len(ra.buf) == 0 {
		if len(ra.pending) == 0 {
			if err := ra.advance(); err != nil {
				return 0, err
			}
			continue
		}

		ra.buf = ra.pending[0]
		ra.pending[0] = nil // release consumed chunks as we go
		ra.pending = ra.pending[1:]
	}

	n := copy(p, ra.buf)
	ra.buf = ra.buf[n:]
	ra.off += int64(n)
	return n, nil
}

func (ra *readahead) advance() error {
	// Get the next window
	w := ra.next
	if w == nil {
		return io.EOF
	}

	// Start the next window possibly doubling its size.
	ra.next = nil
	if w.end < len(ra.o.object.Chunks) {
		ra.size = min(2*ra.size, maxReadahead)
		ra.next = ra.startWindow(w.end, ra.size)
	}

	// Wait for the current window to finish
	t0 := time.Now()
	<-w.done
	w.cancel()

	ra.o.repo.Logger().Trace("vfs", "readahead [%d, %d) %d bytes: fetched in %s, waited %s",
		w.start, w.end, w.bytes, w.elapsed, time.Since(t0))

	if w.err != nil {
		return w.err
	}

	ra.pending = w.data
	return nil
}

func (ra *readahead) startWindow(start, size int) *raWindow {
	// Find the next chunks we are going to fetch
	end, bytes := start, 0
	for end < len(ra.o.object.Chunks) && bytes < size {
		bytes += int(ra.o.object.Chunks[end].Length)
		end++
	}

	repo := ra.o.repo
	ctx, cancel := context.WithCancel(repo.AppContext())
	w := &raWindow{
		start:  start,
		end:    end,
		bytes:  bytes,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	chunks := ra.o.object.Chunks[start:end]
	go func() {
		defer close(w.done)

		t0 := time.Now()
		w.data, w.err = fetchChunks(ctx, repo, chunks)
		w.elapsed = time.Since(t0)
	}()

	return w
}

// close cancels the window in flight, if any, and waits for its goroutine.
func (ra *readahead) close() {
	if ra.next == nil {
		return
	}

	ra.next.cancel()
	<-ra.next.done
	ra.next = nil
}

// fetchChunks returns the decoded chunks, in order, fetched as one GetBlobs
// batch.
func fetchChunks(ctx context.Context, repo *repository.Repository, chunks []objects.Chunk) ([][]byte, error) {
	reqs := make([]repository.BlobReq, 0, len(chunks))
	for _, c := range chunks {
		reqs = append(reqs, repository.BlobReq{Type: resources.RT_CHUNK, MAC: c.ContentMAC})
	}

	blobs := make(map[objects.MAC][]byte, len(chunks))
	opts := &repository.GetBlobsOpts{Concurrency: readaheadConcurrency}
	for b, err := range repo.GetBlobs(ctx, reqs, opts) {
		if err != nil {
			return nil, fmt.Errorf("chunk %x: %w", b.MAC, err)
		}
		blobs[b.MAC] = b.Data
	}

	// GetBlobs stops early, without an error, when ctx is cancelled.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// GetBlobs dedups: a chunk repeated in the window comes back once.
	data := make([][]byte, 0, len(chunks))
	for _, c := range chunks {
		d, ok := blobs[c.ContentMAC]
		if !ok {
			return nil, fmt.Errorf("chunk %x: %w", c.ContentMAC, repository.ErrBlobNotFound)
		}
		data = append(data, d)
	}

	return data, nil
}
