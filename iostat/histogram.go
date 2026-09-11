package iostat

import "math"

// histNumBuckets is derived from the octave count so changing either
// constant visibly changes the covered range with it.
const (
	histBucketsPerOctave = 8
	histOctaves          = 60
	histNumBuckets       = histBucketsPerOctave * histOctaves // covers 1 .. 2^60 in the caller's unit
)

// histogram is a fixed-size geometric histogram: values are counted in
// buckets whose width grows by ~9% per step (histBucketsPerOctave buckets
// per power of two), so memory stays constant no matter how many values are
// observed. Values are dimensionless: callers pick a base unit (values at or
// below 1 land in the first bucket, values at or above 2^60 in the last) and
// convert on the way in and out. The exact min, max and sum are tracked
// alongside, so only the percentiles are approximate — interpolated within
// their bucket, accurate to one bucket width, and always within the observed
// extremes. It is not safe for concurrent use: the owner's mutex guards it.
type histogram struct {
	count   int64
	sum     float64
	min     float64
	max     float64
	buckets [histNumBuckets]int64
}

func (h *histogram) observe(v float64) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		// a non-finite value would poison min/max/sum until the next reset;
		// this histogram tracks measurements, which are finite by
		// construction
		return
	}
	if h.count == 0 || v < h.min {
		h.min = v
	}
	if h.count == 0 || v > h.max {
		h.max = v
	}
	h.count++
	h.sum += v
	h.buckets[histBucketIndex(v)]++
}

func (h *histogram) reset() {
	*h = histogram{}
}

func (h *histogram) avg() float64 {
	if h.count == 0 {
		return 0
	}
	return h.sum / float64(h.count)
}

func histBucketIndex(v float64) int {
	if !(v > 1) { // also catches NaN, whose comparisons are all false
		return 0
	}
	idx := math.Log2(v) * histBucketsPerOctave
	if idx >= histNumBuckets { // compared as floats so +Inf lands here too
		return histNumBuckets - 1
	}
	// Log2 rounding can misfile a value a few ulps from a bucket boundary
	// into a neighboring bucket; one corrective step keeps the invariant
	// that bucket contents lie within the bucket's nominal bounds, which
	// percentile interpolation relies on
	i := int(idx)
	if v < histBucketLower(i) {
		i--
	} else if i+1 < histNumBuckets && v >= histBucketLower(i+1) {
		i++
	}
	return i
}

// histBucketLower returns the lower bound of bucket idx; the formula extends
// one past the last bucket so it also yields upper bounds.
func histBucketLower(idx int) float64 {
	return math.Exp2(float64(idx) / histBucketsPerOctave)
}

// percentile returns an estimate of the p-th percentile, interpolated
// within the bucket holding it.
func (h *histogram) percentile(p float64) float64 {
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
			// interpolate within the bucket, tightening its nominal bounds
			// to the exact observed extremes: the first and last buckets are
			// open-ended, and the buckets holding the extremes span past
			// them
			lower := histBucketLower(i)
			upper := histBucketLower(i + 1)
			if i == 0 || lower < h.min {
				lower = h.min
			}
			if i == histNumBuckets-1 || upper > h.max {
				upper = h.max
			}
			frac := (target - float64(cum)) / float64(c)
			return lower + (upper-lower)*frac
		}
		cum += c
	}
	return h.max
}
