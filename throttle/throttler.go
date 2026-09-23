package throttle

import (
	"context"
	"io"
)

// Throttle many streams against the same bucket, one for inbound one for
// outbound.
type Throttler struct {
	read  *bucket
	write *bucket
}

func NewThrottler(readBytesPerSec, writeBytesPerSec int64) *Throttler {
	return &Throttler{
		read:  newBucket(readBytesPerSec, readBytesPerSec),
		write: newBucket(writeBytesPerSec, writeBytesPerSec),
	}
}

func (l *Throttler) SetReadRate(bytesPerSec int64)  { l.read.SetRate(bytesPerSec) }
func (l *Throttler) SetWriteRate(bytesPerSec int64) { l.write.SetRate(bytesPerSec) }

func (l *Throttler) Reader(ctx context.Context, rd io.Reader) io.Reader {
	return &ThrottledReader{ctx: ctx, rd: rd, b: l.read}
}

func (l *Throttler) ReadCloser(ctx context.Context, rd io.ReadCloser) io.ReadCloser {
	return &ThrottledReadCloser{
		ThrottledReader: ThrottledReader{ctx: ctx, rd: rd, b: l.read},
		closer:          rd,
	}
}

// Charges against the write side of the throttling by consuming from a reader
// (upload style).
func (l *Throttler) WriteFromReader(ctx context.Context, rd io.Reader) io.Reader {
	return &ThrottledReader{ctx: ctx, rd: rd, b: l.write}
}

func (l *Throttler) WriteFromReadCloser(ctx context.Context, rd io.ReadCloser) io.ReadCloser {
	return &ThrottledReadCloser{
		ThrottledReader: ThrottledReader{ctx: ctx, rd: rd, b: l.write},
		closer:          rd,
	}
}
