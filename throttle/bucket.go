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
	"sync"
	"time"
)

type bucket struct {
	mu       sync.Mutex
	rate     int64 // bytes per second, 0 == unlimited
	burst    int64 // maximum accumulated bytes
	tokens   int64 // available credit in bytes, can be negative if we overspent and need to wait for refill
	consumed int64 // total bytes spent, monotonic, can be drained to control a centralized throttler
	last     time.Time
}

func newBucket(rate, burst int64) *bucket {
	return &bucket{
		rate:   rate,
		burst:  burst,
		tokens: burst,
		last:   time.Now(),
	}
}

func (b *bucket) drain() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.consumed
	b.consumed = 0
	return n
}

// SetRate updates the bucket rate live.
func (b *bucket) SetRate(rate int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// if we were unlimited and are now limited,
	// grant a full burst so the newly-limited stream starts warm rather than stall.
	if b.rate <= 0 && rate > 0 {
		b.tokens = rate
		b.last = time.Now()
	}

	// reset rate and burst to new rate, if tokens > new burst, cap it to new burst
	b.rate = rate
	b.burst = rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

// wait() does the actual wait for the bucket to have collected enough tokens to cover the spend of n bytes.
// mutex must be held during the entire accounting but released before sleeping.
func (b *bucket) wait(ctx context.Context, n int) error {
	b.mu.Lock()
	b.consumed += int64(n)

	if b.rate <= 0 {
		b.mu.Unlock()
		return nil
	}

	now := time.Now()
	b.tokens += b.rate * now.Sub(b.last).Nanoseconds() / int64(time.Second)
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now

	b.tokens -= int64(n)

	// Nothing owed: enough credit was banked to cover this spend.
	if b.tokens >= 0 {
		b.mu.Unlock()
		return nil
	}

	// Sleep long enough for the deficit to be repaid at rate.
	delay := time.Duration(-b.tokens * int64(time.Second) / b.rate)
	b.mu.Unlock()

	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
