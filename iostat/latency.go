package iostat

import (
	"math"
	"time"
)

// LatencyStats describes the distribution of per-operation durations
// observed so far. Count, Total, Min, Avg and Max are exact; the percentiles
// are estimated from a fixed-size geometric histogram (~9% bucket
// resolution) and are unreliable below a few dozen observations.
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

// latencyHistogram accumulates duration samples in a histogram in
// nanoseconds (~9% resolution from 1ns up to ~2^60ns, about 36 years). Only
// the total is tracked separately: float64 loses integer precision as the
// sum grows, while the min and max round-trip through float64 exactly for
// any duration below 2^53ns (~104 days).
type latencyHistogram struct {
	total time.Duration
	hist  histogram
}

func (h *latencyHistogram) observe(d time.Duration) {
	h.total += d
	h.hist.observe(float64(d))
}

func (h *latencyHistogram) reset() {
	*h = latencyHistogram{}
}

func (h *latencyHistogram) stats() LatencyStats {
	if h.hist.count == 0 {
		return LatencyStats{}
	}

	// nanoseconds are the histogram's base unit, so the estimates only need
	// rounding to the nearest integer duration; the histogram already keeps
	// them within the observed extremes
	toDur := func(p float64) time.Duration {
		return time.Duration(math.Round(h.hist.percentile(p)))
	}

	return LatencyStats{
		Count: h.hist.count,
		Total: h.total,
		Min:   time.Duration(h.hist.min),
		Avg:   h.total / time.Duration(h.hist.count),
		Max:   time.Duration(h.hist.max),
		P50:   toDur(50),
		P75:   toDur(75),
		P80:   toDur(80),
		P90:   toDur(90),
		P95:   toDur(95),
		P99:   toDur(99),
	}
}
