package iostat

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

const sampleWindow = 50 * time.Millisecond

// represents I/O statistics collected over a period of time.
//
// Two notions of throughput are reported and answer different questions:
//   - the active-time figures (Overall and the Min/Avg/Max/Pxx distribution)
//     divide bytes by the time actually spent inside read/write calls. They
//     characterise how fast the medium is *when working*, ignoring
//     idle/blocked time — the right metric for benchmarking the storage layer.
//   - OverallWall divides bytes by wall-clock elapsed (first to last operation),
//     so it includes stalls and waits. It is the throughput a user actually
//     experiences and the right basis for progress/ETA.
//
// Min, Avg and Max are exact; the Pxx percentiles are estimated from a
// fixed-size geometric histogram (~9% bucket resolution) and are unreliable
// below a few dozen samples — short operations emit one sample per 50ms of
// active I/O.
type IOStats struct {
	Duration     time.Duration // active time spent in I/O
	WallDuration time.Duration // wall-clock span first→last operation
	TotalBytes   int64

	Min     float64 // bytes/sec (active)
	Avg     float64 // bytes/sec (active)
	Max     float64 // bytes/sec (active)
	Overall float64 // overall active throughput (bytes/sec)

	OverallWall float64 // overall wall-clock throughput (bytes/sec)

	P50 float64
	P75 float64
	P80 float64
	P90 float64
	P95 float64
	P99 float64

	// Latency is the distribution of individual operation durations fed
	// through ObserveLatency; zero-valued when the tracker's owner does not
	// observe latency (today only packfile Puts to the storage backend do).
	Latency LatencyStats
}

type tracker struct {
	mu             sync.Mutex
	activeDuration time.Duration
	totalBytes     int64

	// wall-clock span of activity: the time of the first and most recent
	// accounted operation. wallDuration = lastAt - firstAt.
	firstAt time.Time
	lastAt  time.Time

	// active-time bucketing: emits a sample once bucketDuration (summed active
	// I/O time) reaches sampleWindow.
	bucketBytes    int64
	bucketDuration time.Duration

	tput histogram // throughput samples in bytes/sec (~9% buckets over 1 B/s .. 2^60 B/s)
	lat  latencyHistogram
}

func newTracker() *tracker {
	return &tracker{}
}

// add accounts for n bytes processed over dt of *active* time, completing at
// wall-clock time now (used to measure the wall-clock span of activity).
func (t *tracker) add(n int64, dt time.Duration, now time.Time) {
	if n == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalBytes += n

	// Track the wall-clock span (first → last operation) for OverallWall.
	if !now.IsZero() {
		if t.firstAt.IsZero() {
			t.firstAt = now
		}
		t.lastAt = now
	}

	if dt <= 0 {
		// count the bytes, but don't create an active-throughput sample
		return
	}

	t.activeDuration += dt

	// accumulate into sampling bucket
	t.bucketBytes += n
	t.bucketDuration += dt

	// if we haven't reached the sampling window yet, don't create a sample
	if t.bucketDuration < sampleWindow {
		return
	}

	// create a sample for this bucket
	t.tput.observe(float64(t.bucketBytes) / t.bucketDuration.Seconds())

	// reset the bucket
	t.bucketBytes = 0
	t.bucketDuration = 0
}

func (t *tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.activeDuration = 0
	t.totalBytes = 0
	t.firstAt = time.Time{}
	t.lastAt = time.Time{}
	t.bucketBytes = 0
	t.bucketDuration = 0
	t.tput.reset()
	t.lat.reset()
}

// ObserveLatency accounts for one operation that took d, independently of the
// byte/throughput accounting done through spans.
func (t *tracker) ObserveLatency(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lat.observe(d)
}

// TotalBytes returns the cumulative bytes accounted so far. Unlike Stats() it
// has no side effects (it does not flush the partial sampling bucket), so it is
// safe to poll at a high rate — e.g. from a UI render loop — without perturbing
// the throughput samples.
func (t *tracker) TotalBytes() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalBytes
}

func (t *tracker) Stats() IOStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	// flush any partially-filled active bucket as one last sample
	if t.bucketDuration > 0 && t.bucketBytes > 0 {
		t.tput.observe(float64(t.bucketBytes) / t.bucketDuration.Seconds())
		t.bucketBytes = 0
		t.bucketDuration = 0
	}

	duration := t.activeDuration
	wall := t.lastAt.Sub(t.firstAt)

	stats := IOStats{
		Duration:     duration,
		WallDuration: wall,
		TotalBytes:   t.totalBytes,
		Latency:      t.lat.stats(),
	}

	if duration > 0 && t.totalBytes > 0 {
		stats.Overall = float64(t.totalBytes) / duration.Seconds()
	}
	if wall > 0 && t.totalBytes > 0 {
		stats.OverallWall = float64(t.totalBytes) / wall.Seconds()
	}

	// min, max and avg are exact; the percentiles are interpolated from the
	// histogram buckets; everything is zero when no sample was recorded
	stats.Min = t.tput.min
	stats.Max = t.tput.max
	stats.Avg = t.tput.avg()

	stats.P50 = t.tput.percentile(50)
	stats.P75 = t.tput.percentile(75)
	stats.P80 = t.tput.percentile(80)
	stats.P90 = t.tput.percentile(90)
	stats.P95 = t.tput.percentile(95)
	stats.P99 = t.tput.percentile(99)

	return stats
}

// An IOTracker maintains separate trackers for read and write operations
type IOTracker struct {
	Read  *tracker
	Write *tracker
}

func New() *IOTracker {
	return &IOTracker{
		Read:  newTracker(),
		Write: newTracker(),
	}
}

func (ioT *IOTracker) Reset() {
	ioT.Read.Reset()
	ioT.Write.Reset()
}

// Span represents a time span during which I/O operations are tracked
// this is required to measure active time vs wall-clock time
type Span struct {
	t    *tracker
	last time.Time
}

func (ioT *IOTracker) GetWriteSpan() *Span {
	now := time.Now()
	return &Span{
		t:    ioT.Write,
		last: now,
	}
}

func (ioT *IOTracker) GetReadSpan() *Span {
	now := time.Now()
	return &Span{
		t:    ioT.Read,
		last: now,
	}
}

func (s *Span) Add(n int64) {
	if s == nil {
		return
	}
	now := time.Now()
	dt := now.Sub(s.last)
	s.last = now
	s.t.add(n, dt, now)
}

func formatBytes(b int64) string {
	if b <= 0 {
		return "0 B"
	}
	return humanize.IBytes(uint64(b))
}

func formatThroughput(bps float64) string {
	if bps <= 0 || math.IsNaN(bps) || math.IsInf(bps, 0) {
		return "0 B/s"
	}
	return fmt.Sprintf("%s/s", humanize.IBytes(uint64(bps)))
}

func formatLatency(l LatencyStats) string {
	if l.Count == 0 {
		return ""
	}
	return fmt.Sprintf(", lat: n=%d, min=%s, avg=%s, p50=%s, p90=%s, p95=%s, p99=%s, max=%s",
		l.Count,
		l.Min.Round(time.Microsecond),
		l.Avg.Round(time.Microsecond),
		l.P50.Round(time.Microsecond),
		l.P90.Round(time.Microsecond),
		l.P95.Round(time.Microsecond),
		l.P99.Round(time.Microsecond),
		l.Max.Round(time.Microsecond),
	)
}

func (ioT *IOTracker) SummaryString() string {
	r := ioT.Read.Stats()
	w := ioT.Write.Stats()

	return fmt.Sprintf(
		"r:"+
			" dt=%s,"+
			" wall=%s,"+
			" bytes=%s (%d),"+
			" overall=%s,"+
			" overall_wall=%s,"+
			" min=%s,"+
			" avg=%s,"+
			" p50=%s,"+
			" p75=%s,"+
			" p80=%s,"+
			" p90=%s,"+
			" p95=%s,"+
			" p99=%s,"+
			" max=%s%s\n"+
			"w:"+
			" dt=%s,"+
			" wall=%s,"+
			" bytes=%s (%d),"+
			" overall=%s,"+
			" overall_wall=%s,"+
			" min=%s,"+
			" avg=%s,"+
			" p50=%s,"+
			" p75=%s,"+
			" p80=%s,"+
			" p90=%s,"+
			" p95=%s,"+
			" p99=%s,"+
			" max=%s%s\n",
		r.Duration, r.WallDuration, formatBytes(r.TotalBytes), r.TotalBytes,
		formatThroughput(r.Overall),
		formatThroughput(r.OverallWall),
		formatThroughput(r.Min),
		formatThroughput(r.Avg),
		formatThroughput(r.P50),
		formatThroughput(r.P75),
		formatThroughput(r.P80),
		formatThroughput(r.P90),
		formatThroughput(r.P95),
		formatThroughput(r.P99),
		formatThroughput(r.Max),
		formatLatency(r.Latency),

		w.Duration, w.WallDuration, formatBytes(w.TotalBytes), w.TotalBytes,
		formatThroughput(w.Overall),
		formatThroughput(w.OverallWall),
		formatThroughput(w.Min),
		formatThroughput(w.Avg),
		formatThroughput(w.P50),
		formatThroughput(w.P75),
		formatThroughput(w.P80),
		formatThroughput(w.P90),
		formatThroughput(w.P95),
		formatThroughput(w.P99),
		formatThroughput(w.Max),
		formatLatency(w.Latency),
	)
}
