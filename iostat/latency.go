package iostat

import (
	"math"
	"time"
)

// Latency percentiles are computed from a fixed-size geometric histogram:
// per-operation durations are counted in buckets whose width grows by ~9%
// per step (8 buckets per power of two), so memory stays constant no matter
// how many operations are observed. Percentiles are interpolated within
// their bucket and accurate to one bucket width.
const (
	latencyMinBucket        = time.Microsecond
	latencyBucketsPerOctave = 8
	latencyNumBuckets       = 256 // covers 1µs .. ~71min, clamps beyond
)

// LatencyStats describes the distribution of per-operation durations
// observed so far.
type LatencyStats struct {
	Count int64
	Total time.Duration

	Min time.Duration
	Avg time.Duration
	Max time.Duration

	P50 time.Duration
	P75 time.Duration
	P80 time.Duration
	P90 time.Duration
	P95 time.Duration
	P99 time.Duration
}

// latencyHistogram accumulates duration samples. It is not safe for
// concurrent use: the owning tracker's mutex guards it.
type latencyHistogram struct {
	count   int64
	total   time.Duration
	min     time.Duration
	max     time.Duration
	buckets [latencyNumBuckets]int64
}

func (h *latencyHistogram) observe(d time.Duration) {
	if h.count == 0 || d < h.min {
		h.min = d
	}
	if d > h.max {
		h.max = d
	}
	h.count++
	h.total += d
	h.buckets[latencyBucketIndex(d)]++
}

func (h *latencyHistogram) reset() {
	*h = latencyHistogram{}
}

func latencyBucketIndex(d time.Duration) int {
	if d <= latencyMinBucket {
		return 0
	}
	idx := int(math.Log2(float64(d)/float64(latencyMinBucket)) * latencyBucketsPerOctave)
	if idx >= latencyNumBuckets {
		return latencyNumBuckets - 1
	}
	return idx
}

// latencyBucketLower returns the lower bound of bucket idx in nanoseconds;
// the formula extends one past the last bucket so it also yields upper
// bounds.
func latencyBucketLower(idx int) float64 {
	return float64(latencyMinBucket) * math.Exp2(float64(idx)/latencyBucketsPerOctave)
}

func (h *latencyHistogram) percentile(p float64) time.Duration {
	if h.count == 0 {
		return 0
	}

	target := p / 100 * float64(h.count)
	var cum int64
	for i, c := range h.buckets {
		if c == 0 {
			continue
		}
		if float64(cum+c) >= target {
			lower := latencyBucketLower(i)
			upper := latencyBucketLower(i + 1)
			frac := (target - float64(cum)) / float64(c)
			v := time.Duration(lower + (upper-lower)*frac)
			// the histogram is approximate; the observed extremes are exact
			if v < h.min {
				v = h.min
			}
			if v > h.max {
				v = h.max
			}
			return v
		}
		cum += c
	}
	return h.max
}

func (h *latencyHistogram) stats() LatencyStats {
	if h.count == 0 {
		return LatencyStats{}
	}
	return LatencyStats{
		Count: h.count,
		Total: h.total,
		Min:   h.min,
		Avg:   h.total / time.Duration(h.count),
		Max:   h.max,
		P50:   h.percentile(50),
		P75:   h.percentile(75),
		P80:   h.percentile(80),
		P90:   h.percentile(90),
		P95:   h.percentile(95),
		P99:   h.percentile(99),
	}
}
