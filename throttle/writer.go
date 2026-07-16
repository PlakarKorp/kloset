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

type ThrottledWriter struct {
	ctx context.Context
	wr  io.Writer
	b   *bucket
}

func NewThrottledWriter(ctx context.Context, wr io.Writer, writeBytesPerSec int64) *ThrottledWriter {
	if wr == nil {
		return nil
	}
	b := newBucket(writeBytesPerSec, writeBytesPerSec)
	return &ThrottledWriter{ctx: ctx, wr: wr, b: b}
}

func (t *ThrottledWriter) Write(p []byte) (int, error) {
	if err := t.b.wait(t.ctx, len(p)); err != nil {
		return 0, err
	}
	return t.wr.Write(p)
}
