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
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// test helpers
// ----------------------------------------------------------------------------

// errReader always returns the configured error after emitting n bytes.
type errReader struct {
	data []byte
	err  error
	off  int
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, r.err
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	if r.off >= len(r.data) {
		return n, r.err
	}
	return n, nil
}

// recordingCloser tracks whether Close was called and can return an error.
type recordingCloser struct {
	io.Reader
	closed   bool
	closeErr error
}

func (c *recordingCloser) Close() error {
	c.closed = true
	return c.closeErr
}

// countingWriter records every byte it is handed.
type countingWriter struct {
	buf      bytes.Buffer
	writeN   int
	writeErr error
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writeN += len(p)
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return w.buf.Write(p)
}

// ----------------------------------------------------------------------------
// NewThrottledReader / NewThrottledReadCloser (constructors)
// ----------------------------------------------------------------------------

func TestNewThrottledReaderNil(t *testing.T) {
	if got := NewThrottledReader(context.Background(), nil, 1000); got != nil {
		t.Errorf("NewThrottledReader(nil) = %v, want nil", got)
	}
}

func TestNewThrottledReaderConstructs(t *testing.T) {
	src := strings.NewReader("hello world")
	tr := NewThrottledReader(context.Background(), src, 1<<20) // 1 MiB/s, no delay for a tiny payload
	if tr == nil {
		t.Fatal("NewThrottledReader returned nil for a valid reader")
	}
	if tr.rd != src {
		t.Error("reader not bound to the supplied source")
	}
	if tr.b == nil {
		t.Fatal("reader left with a nil bucket")
	}
	if tr.b.rate != 1<<20 || tr.b.burst != 1<<20 {
		t.Errorf("bucket rate/burst = %v/%v, want %v/%v", tr.b.rate, tr.b.burst, 1<<20, 1<<20)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello world" {
		t.Errorf("read %q, want %q", got, "hello world")
	}
	if tr.b.consumed != int64(len("hello world")) {
		t.Errorf("consumed = %d, want %d", tr.b.consumed, len("hello world"))
	}
}

func TestNewThrottledReaderUnlimited(t *testing.T) {
	// A zero rate constructs an unlimited bucket: no blocking regardless of spend.
	tr := NewThrottledReader(context.Background(), strings.NewReader("payload"), 0)
	if tr == nil {
		t.Fatal("NewThrottledReader returned nil for a valid reader")
	}
	if tr.b.rate != 0 {
		t.Errorf("rate = %v, want 0 (unlimited)", tr.b.rate)
	}
	got, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("read %q, want %q", got, "payload")
	}
}

func TestNewThrottledReadCloserNil(t *testing.T) {
	if got := NewThrottledReadCloser(context.Background(), nil, 1000); got != nil {
		t.Errorf("NewThrottledReadCloser(nil) = %v, want nil", got)
	}
}

func TestNewThrottledReadCloserConstructsAndCloses(t *testing.T) {
	rc := &recordingCloser{Reader: strings.NewReader("payload")}
	trc := NewThrottledReadCloser(context.Background(), rc, 1<<20)
	if trc == nil {
		t.Fatal("NewThrottledReadCloser returned nil for a valid reader")
	}
	if trc.rd != rc {
		t.Error("read closer not bound to the supplied source")
	}
	if trc.closer != rc {
		t.Error("closer not bound to the supplied source")
	}
	if trc.b == nil || trc.b.rate != 1<<20 {
		t.Fatalf("bucket rate = %v, want %v", trc.b.rate, 1<<20)
	}
	got, err := io.ReadAll(trc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("read %q, want %q", got, "payload")
	}
	if err := trc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !rc.closed {
		t.Error("Close did not propagate to the underlying closer")
	}
}

// ----------------------------------------------------------------------------
// NewThrottledWriter (constructor)
// ----------------------------------------------------------------------------

func TestNewThrottledWriterNil(t *testing.T) {
	if got := NewThrottledWriter(context.Background(), nil, 1000); got != nil {
		t.Errorf("NewThrottledWriter(nil) = %v, want nil", got)
	}
}

func TestNewThrottledWriterConstructs(t *testing.T) {
	var sink countingWriter
	tw := NewThrottledWriter(context.Background(), &sink, 1<<20)
	if tw == nil {
		t.Fatal("NewThrottledWriter returned nil for a valid writer")
	}
	if tw.wr != &sink {
		t.Error("writer not bound to the supplied sink")
	}
	if tw.b == nil || tw.b.rate != 1<<20 || tw.b.burst != 1<<20 {
		t.Fatalf("bucket rate/burst = %v/%v, want %v/%v", tw.b.rate, tw.b.burst, 1<<20, 1<<20)
	}
	n, err := tw.Write([]byte("data"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 4 {
		t.Errorf("Write returned %d, want 4", n)
	}
	if sink.buf.String() != "data" {
		t.Errorf("sink got %q, want %q", sink.buf.String(), "data")
	}
	if tw.b.consumed != 4 {
		t.Errorf("consumed = %d, want 4", tw.b.consumed)
	}
}

// ----------------------------------------------------------------------------
// bucket construction & drain
// ----------------------------------------------------------------------------

func TestNewBucket(t *testing.T) {
	before := time.Now()
	b := newBucket(500, 800)
	if b.rate != 500 {
		t.Errorf("rate = %v, want 500", b.rate)
	}
	if b.burst != 800 {
		t.Errorf("burst = %v, want 800", b.burst)
	}
	if b.tokens != 800 {
		t.Errorf("available = %v, want 800 (full burst)", b.tokens)
	}
	if b.last.Before(before) {
		t.Error("last timestamp predates construction")
	}
	if b.consumed != 0 {
		t.Errorf("consumed = %d, want 0", b.consumed)
	}
}

func TestBucketDrain(t *testing.T) {
	b := newBucket(0, 0) // unlimited: wait never blocks but still accounts taken
	ctx := context.Background()
	if err := b.wait(ctx, 100); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if err := b.wait(ctx, 50); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if got := b.drain(); got != 150 {
		t.Errorf("drain = %d, want 150", got)
	}
	// Drain resets the counter.
	if got := b.drain(); got != 0 {
		t.Errorf("second drain = %d, want 0", got)
	}
}

// ----------------------------------------------------------------------------
// bucket.wait
// ----------------------------------------------------------------------------

func TestWaitUnlimitedNeverBlocks(t *testing.T) {
	b := newBucket(0, 0)
	start := time.Now()
	if err := b.wait(context.Background(), 1<<20); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("unlimited wait took %v, expected ~0", elapsed)
	}
	// Even unlimited buckets track spend for later sync.
	if b.consumed != 1<<20 {
		t.Errorf("consumed = %d, want %d", b.consumed, 1<<20)
	}
}

func TestWaitWithinBurstNoDelay(t *testing.T) {
	b := newBucket(1000, 1000) // 1000 tokens banked
	start := time.Now()
	if err := b.wait(context.Background(), 1000); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("in-burst wait took %v, expected ~0", elapsed)
	}
}

func TestWaitDeficitSleeps(t *testing.T) {
	// rate=burst=10000 bytes/s. Spending 11000 in one shot overdraws by 1000,
	// which is repaid at rate over 1000/10000 = 0.1s.
	b := newBucket(10000, 10000)
	start := time.Now()
	if err := b.wait(context.Background(), 11000); err != nil {
		t.Fatalf("wait: %v", err)
	}
	elapsed := time.Since(start)
	// Timers never fire early; allow a small slack for scheduling jitter.
	if elapsed < 80*time.Millisecond {
		t.Errorf("deficit wait took %v, expected >= ~100ms", elapsed)
	}
}

func TestWaitRefillsOverTime(t *testing.T) {
	b := newBucket(10000, 10000)
	// Drain the whole burst; still within credit, so no delay.
	if err := b.wait(context.Background(), 10000); err != nil {
		t.Fatalf("wait: %v", err)
	}
	// Let ~50ms of tokens (≈500 bytes) accrue.
	time.Sleep(60 * time.Millisecond)
	start := time.Now()
	// Spending 400 should be covered by the refill without sleeping.
	if err := b.wait(context.Background(), 400); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Errorf("refilled wait took %v, expected ~0", elapsed)
	}
}

func TestWaitTokensCappedAtBurst(t *testing.T) {
	b := newBucket(1000, 1000)
	// Idle a while so raw refill would exceed burst, then confirm the cap holds
	// by draining exactly burst worth without any delay.
	time.Sleep(30 * time.Millisecond)
	start := time.Now()
	if err := b.wait(context.Background(), 1000); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Errorf("wait took %v; tokens likely exceeded burst cap", elapsed)
	}
}

func TestWaitContextCancelledDuringSleep(t *testing.T) {
	// Tiny rate + large spend => multi-second delay we will interrupt.
	b := newBucket(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	start := time.Now()
	err := b.wait(ctx, 1_000_000)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("wait err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Errorf("cancelled wait blocked for %v, expected prompt return", elapsed)
	}
	// The spend is still charged even though the wait was interrupted.
	if b.consumed != 1_000_000 {
		t.Errorf("consumed = %d, want 1000000", b.consumed)
	}
}

func TestWaitContextDeadlineElapses(t *testing.T) {
	b := newBucket(1, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := b.wait(ctx, 1_000_000)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("returned after %v, expected to wait for the deadline", elapsed)
	}
}

// ----------------------------------------------------------------------------
// bucket.SetRate
// ----------------------------------------------------------------------------

func TestSetRateWarmStartFromUnlimited(t *testing.T) {
	b := newBucket(0, 0) // unlimited / cold
	b.SetRate(5000)
	if b.rate != 5000 || b.burst != 5000 {
		t.Fatalf("rate/burst = %v/%v, want 5000/5000", b.rate, b.burst)
	}
	// Warm start: a full burst is granted so the stream doesn't stall.
	if b.tokens != 5000 {
		t.Errorf("available = %v, want 5000 (warm start)", b.tokens)
	}
	// Spending the granted burst incurs no delay.
	start := time.Now()
	if err := b.wait(context.Background(), 5000); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("post-warm-start wait took %v, expected ~0", elapsed)
	}
}

func TestSetRateCapsExistingTokens(t *testing.T) {
	b := newBucket(10000, 10000) // tokens == 10000
	b.SetRate(1000)              // burst shrinks to 1000
	if b.burst != 1000 {
		t.Errorf("burst = %v, want 1000", b.burst)
	}
	if b.tokens != 1000 {
		t.Errorf("available = %v, want 1000 (capped to new burst)", b.tokens)
	}
}

func TestSetRateAlreadyLimitedNoWarmStart(t *testing.T) {
	b := newBucket(1000, 1000)
	b.tokens = 200 // simulate a partially depleted bucket
	b.SetRate(2000)
	if b.rate != 2000 || b.burst != 2000 {
		t.Fatalf("rate/burst = %v/%v, want 2000/2000", b.rate, b.burst)
	}
	// Coming from an already-limited rate must NOT reset tokens to a full burst.
	if b.tokens != 200 {
		t.Errorf("available = %v, want 200 (no warm start when already limited)", b.tokens)
	}
}

func TestSetRateToUnlimited(t *testing.T) {
	b := newBucket(1000, 1000)
	b.SetRate(0)
	if b.rate != 0 {
		t.Errorf("rate = %v, want 0", b.rate)
	}
	// Unlimited: wait must not block regardless of spend.
	start := time.Now()
	if err := b.wait(context.Background(), 1<<20); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("unlimited wait took %v, expected ~0", elapsed)
	}
}

// ----------------------------------------------------------------------------
// throttledReader / throttledReadCloser
// ----------------------------------------------------------------------------

func TestThrottledReaderChargesBytesRead(t *testing.T) {
	b := newBucket(0, 0) // unlimited: focus on accounting, not timing
	tr := &ThrottledReader{ctx: context.Background(), rd: strings.NewReader("abcdef"), b: b}
	buf := make([]byte, 4)
	n, err := tr.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 4 {
		t.Fatalf("Read n = %d, want 4", n)
	}
	if string(buf) != "abcd" {
		t.Errorf("Read %q, want %q", buf[:n], "abcd")
	}
	if b.consumed != 4 {
		t.Errorf("charged %d bytes, want 4 (bytes read, not buffer size)", b.consumed)
	}
}

func TestThrottledReaderZeroBytesNotCharged(t *testing.T) {
	b := newBucket(0, 0)
	// Reader that immediately hits EOF with 0 bytes.
	tr := &ThrottledReader{ctx: context.Background(), rd: strings.NewReader(""), b: b}
	n, err := tr.Read(make([]byte, 4))
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("err = %v, want io.EOF", err)
	}
	if b.consumed != 0 {
		t.Errorf("consumed = %d, want 0 (no charge on zero-byte read)", b.consumed)
	}
}

func TestThrottledReaderPropagatesUnderlyingError(t *testing.T) {
	b := newBucket(0, 0)
	sentinel := errors.New("boom")
	tr := &ThrottledReader{
		ctx: context.Background(),
		rd:  &errReader{data: []byte("xy"), err: sentinel},
		b:   b,
	}
	n, err := tr.Read(make([]byte, 8))
	if n != 2 {
		t.Errorf("n = %d, want 2", n)
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want sentinel", err)
	}
	if b.consumed != 2 {
		t.Errorf("consumed = %d, want 2", b.consumed)
	}
}

func TestThrottledReaderWaitErrorTakesPrecedence(t *testing.T) {
	// Force a wait that blocks, then cancel so wait returns the ctx error even
	// though bytes were successfully read from the source.
	b := newBucket(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	tr := &ThrottledReader{ctx: ctx, rd: strings.NewReader("hello"), b: b}
	n, err := tr.Read(make([]byte, 5))
	if n != 5 {
		t.Errorf("n = %d, want 5 (bytes were read)", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestThrottledReadCloserClosePropagatesError(t *testing.T) {
	b := newBucket(0, 0)
	sentinel := errors.New("close failed")
	rc := &recordingCloser{Reader: strings.NewReader(""), closeErr: sentinel}
	trc := &ThrottledReadCloser{
		ThrottledReader: ThrottledReader{ctx: context.Background(), rd: rc, b: b},
		closer:          rc,
	}
	if err := trc.Close(); !errors.Is(err, sentinel) {
		t.Errorf("Close err = %v, want sentinel", err)
	}
	if !rc.closed {
		t.Error("underlying closer not invoked")
	}
}

// ----------------------------------------------------------------------------
// throttledWriter
// ----------------------------------------------------------------------------

func TestThrottledWriterWritesThrough(t *testing.T) {
	b := newBucket(0, 0)
	var sink countingWriter
	tw := &ThrottledWriter{ctx: context.Background(), wr: &sink, b: b}
	n, err := tw.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 5 {
		t.Errorf("n = %d, want 5", n)
	}
	if sink.buf.String() != "hello" {
		t.Errorf("sink = %q, want %q", sink.buf.String(), "hello")
	}
	if b.consumed != 5 {
		t.Errorf("consumed = %d, want 5", b.consumed)
	}
}

func TestThrottledWriterWaitErrorSkipsUnderlyingWrite(t *testing.T) {
	b := newBucket(1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var sink countingWriter
	tw := &ThrottledWriter{ctx: ctx, wr: &sink, b: b}
	n, err := tw.Write([]byte("hello"))
	if n != 0 {
		t.Errorf("n = %d, want 0 on wait error", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if sink.writeN != 0 {
		t.Error("underlying writer was called despite wait error")
	}
}

func TestThrottledWriterPropagatesUnderlyingError(t *testing.T) {
	b := newBucket(0, 0)
	sentinel := errors.New("disk full")
	sink := &countingWriter{writeErr: sentinel}
	tw := &ThrottledWriter{ctx: context.Background(), wr: sink, b: b}
	n, err := tw.Write([]byte("data"))
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want sentinel", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
}
