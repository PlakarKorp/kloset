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
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// ----------------------------------------------------------------------------
// NewThrottler
// ----------------------------------------------------------------------------

func TestNewThrottler(t *testing.T) {
	l := NewThrottler(1000, 2000)
	if l.read == nil || l.write == nil {
		t.Fatal("throttler constructed with nil buckets")
	}
	if l.read.rate != 1000 || l.read.burst != 1000 {
		t.Errorf("read rate/burst = %v/%v, want 1000/1000", l.read.rate, l.read.burst)
	}
	if l.write.rate != 2000 || l.write.burst != 2000 {
		t.Errorf("write rate/burst = %v/%v, want 2000/2000", l.write.rate, l.write.burst)
	}
}

// ----------------------------------------------------------------------------
// shared accounting: all streams charge the same buckets
// ----------------------------------------------------------------------------

func TestThrottlerReadersShareReadBucket(t *testing.T) {
	l := NewThrottler(0, 0) // unlimited: focus on accounting, not timing
	ctx := context.Background()

	r1 := l.Reader(ctx, strings.NewReader(strings.Repeat("a", 100)))
	r2 := l.Reader(ctx, strings.NewReader(strings.Repeat("b", 250)))
	if _, err := io.Copy(io.Discard, r1); err != nil {
		t.Fatalf("Copy r1: %v", err)
	}
	if _, err := io.Copy(io.Discard, r2); err != nil {
		t.Fatalf("Copy r2: %v", err)
	}

	if l.read.consumed != 350 {
		t.Errorf("read consumed = %d, want 350 (sum of both streams)", l.read.consumed)
	}
	if l.write.consumed != 0 {
		t.Errorf("write consumed = %d, want 0", l.write.consumed)
	}
}

func TestThrottlerWriteFromReaderChargesWriteBucket(t *testing.T) {
	l := NewThrottler(0, 0)
	rd := l.WriteFromReader(context.Background(), strings.NewReader("hello"))
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("read %q, want %q", got, "hello")
	}
	if l.write.consumed != 5 {
		t.Errorf("write consumed = %d, want 5", l.write.consumed)
	}
	if l.read.consumed != 0 {
		t.Errorf("read consumed = %d, want 0 (uploads must not charge reads)", l.read.consumed)
	}
}

func TestThrottlerWriteFromReadCloserChargesAndCloses(t *testing.T) {
	l := NewThrottler(0, 0)
	rc := &recordingCloser{Reader: strings.NewReader("payload")}
	trc := l.WriteFromReadCloser(context.Background(), rc)

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
	if l.write.consumed != int64(len("payload")) {
		t.Errorf("write consumed = %d, want %d", l.write.consumed, len("payload"))
	}
	if l.read.consumed != 0 {
		t.Errorf("read consumed = %d, want 0 (uploads must not charge reads)", l.read.consumed)
	}
}

// ----------------------------------------------------------------------------
// aggregate throttling: the cap applies to combined throughput
// ----------------------------------------------------------------------------

func TestThrottlerAggregateRateAcrossStreams(t *testing.T) {
	// rate=burst=100000 bytes/s. Each stream spends 40000, well within burst,
	// so per-stream buckets would never sleep. Combined the three spend 120000,
	// overdrawing the shared bucket by 20000, repaid at rate over ~200ms.
	l := NewThrottler(100_000, 0)
	ctx := context.Background()

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rd := l.Reader(ctx, strings.NewReader(strings.Repeat("z", 40_000)))
			if _, err := io.Copy(io.Discard, rd); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Copy: %v", err)
	}

	elapsed := time.Since(start)
	// Timers never fire early; allow a small slack for scheduling jitter.
	if elapsed < 150*time.Millisecond {
		t.Errorf("combined streams finished in %v, expected >= ~200ms (bucket not shared?)", elapsed)
	}
	if l.read.consumed != 120_000 {
		t.Errorf("read consumed = %d, want 120000", l.read.consumed)
	}
}

// ----------------------------------------------------------------------------
// live retune
// ----------------------------------------------------------------------------

func TestThrottlerSetRatesLive(t *testing.T) {
	l := NewThrottler(0, 0)
	rd := l.Reader(context.Background(), strings.NewReader("payload"))

	l.SetReadRate(5000)
	l.SetWriteRate(7000)

	if l.read.rate != 5000 || l.read.burst != 5000 {
		t.Errorf("read rate/burst = %v/%v, want 5000/5000", l.read.rate, l.read.burst)
	}
	if l.write.rate != 7000 || l.write.burst != 7000 {
		t.Errorf("write rate/burst = %v/%v, want 7000/7000", l.write.rate, l.write.burst)
	}

	// The already-wrapped stream draws on the retuned bucket: the warm-start
	// burst covers this tiny read with no delay.
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "payload" {
		t.Errorf("read %q, want %q", got, "payload")
	}
	if l.read.consumed != int64(len("payload")) {
		t.Errorf("read consumed = %d, want %d", l.read.consumed, len("payload"))
	}
}

// ----------------------------------------------------------------------------
// wrapper plumbing
// ----------------------------------------------------------------------------

func TestThrottlerReadCloserCloses(t *testing.T) {
	l := NewThrottler(0, 0)
	rc := &recordingCloser{Reader: strings.NewReader("payload")}
	trc := l.ReadCloser(context.Background(), rc)

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
	if l.read.consumed != int64(len("payload")) {
		t.Errorf("read consumed = %d, want %d", l.read.consumed, len("payload"))
	}
}

func TestThrottlerReaderContextCancelled(t *testing.T) {
	// Tiny rate + spend beyond burst => the wait blocks, then the cancelled
	// context interrupts it.
	l := NewThrottler(1, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	rd := l.Reader(ctx, strings.NewReader("hello"))
	n, err := rd.Read(make([]byte, 5))
	if n != 5 {
		t.Errorf("n = %d, want 5 (bytes were read)", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}
