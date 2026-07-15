/*
 * Copyright (c) 2026 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package throttle

import (
	"context"
	"io"
)

type Limiter struct {
	read  *bucket
	write *bucket
}

const (
	NOLIMIT = 0
)

func New(readBytesPerSec int64, writeBytesPerSec int64) *Limiter {
	if readBytesPerSec <= 0 || writeBytesPerSec <= 0 {
		return nil
	}
	readRate := float64(readBytesPerSec)
	writeRate := float64(writeBytesPerSec)
	return &Limiter{
		read:  newBucket(readRate, readRate),
		write: newBucket(writeRate, writeRate),
	}
}

func (l *Limiter) SetRate(readBytesPerSec int64, writeBytesPerSec int64) {
	l.read.SetRate(float64(readBytesPerSec))
	l.write.SetRate(float64(writeBytesPerSec))
}

func (l *Limiter) Reader(ctx context.Context, rd io.Reader) io.Reader {
	if rd == nil {
		return rd
	}
	return &throttledReader{ctx: ctx, rd: rd, b: l.read}
}

func (l *Limiter) ReadCloser(ctx context.Context, rc io.ReadCloser) io.ReadCloser {
	if rc == nil {
		return rc
	}
	return &throttledReadCloser{
		throttledReader: throttledReader{ctx: ctx, rd: rc, b: l.read},
		closer:          rc,
	}
}

func (l *Limiter) Writer(ctx context.Context, wr io.Writer) io.Writer {
	if wr == nil {
		return wr
	}
	return &throttledWriter{ctx: ctx, wr: wr, b: l.write}
}
