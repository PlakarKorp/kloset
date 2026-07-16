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

type ThrottledReader struct {
	ctx context.Context
	rd  io.Reader
	b   *bucket
}

func NewThrottledReader(ctx context.Context, rd io.Reader, readBytesPerSec int64) *ThrottledReader {
	if rd == nil {
		return nil
	}
	b := newBucket(readBytesPerSec, readBytesPerSec)
	return &ThrottledReader{ctx: ctx, rd: rd, b: b}
}

func (t *ThrottledReader) Read(p []byte) (int, error) {
	n, err := t.rd.Read(p)
	if n > 0 {
		// charge for the bytes actually read, not the buffer size
		if werr := t.b.wait(t.ctx, n); werr != nil {
			return n, werr
		}
	}
	return n, err
}

type ThrottledReadCloser struct {
	ThrottledReader
	closer io.Closer
}

func NewThrottledReadCloser(ctx context.Context, rd io.Reader, readBytesPerSec int64) *ThrottledReadCloser {
	if rd == nil {
		return nil
	}
	b := newBucket(readBytesPerSec, readBytesPerSec)
	return &ThrottledReadCloser{
		ThrottledReader: ThrottledReader{ctx: ctx, rd: rd, b: b},
		closer:          rd.(io.Closer),
	}
}

func (t *ThrottledReadCloser) Close() error {
	return t.closer.Close()
}
