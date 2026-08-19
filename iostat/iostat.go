package iostat

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
)

const sampleWindow = 50 * time.Millisecond

// represents I/O statistics collected over a period of time.
//
// Two notions of throughput are reported and answer different questions:
//   - the active-time figures (Overall and the Min/Avg/Median/Max/Pxx
//     distribution) divide bytes by the time actually spent inside read/write
//     calls. They characterise how fast the medium is *when working*, ignoring
//     idle/blocked time — the right metric for benchmarking the storage layer.
//   - OverallWall divides bytes by wall-clock elapsed (first to last operation),
//     so it includes stalls and waits. It is the throughput a user actually
//     experiences and the right basis for progress/ETA.
type IOStats struct {
	Duration     time.Duration // active time spent in I/O
	WallDuration time.Duration // wall-clock span first→last operation
	TotalBytes   int64

	Min     float64 // bytes/sec (active)
	Avg     float64 // bytes/sec (active)
	Median  float64 // bytes/sec (active)
	Max     float64 // bytes/sec (active)
	Overall float64 // overall active throughput (bytes/sec)

	OverallWall float64 // overall wall-clock throughput (bytes/sec)

	P50 float64
	P75 float64
	P80 float64
	P90 float64
	P95 float64
	P99 float64
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
	samples        []float64
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
	throughput := float64(t.bucketBytes) / t.bucketDuration.Seconds()
	t.samples = append(t.samples, throughput)

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
	t.samples = nil
	t.bucketBytes = 0
	t.bucketDuration = 0
}

func percentile(sorted []float64, p float64) float64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[n-1]
	}

	pos := (p / 100.0) * float64(n-1)
	lower := int(math.Floor(pos))
	upper := int(math.Ceil(pos))
	if lower == upper {
		return sorted[lower]
	}
	weight := pos - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
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
		throughput := float64(t.bucketBytes) / t.bucketDuration.Seconds()
		t.samples = append(t.samples, throughput)
		t.bucketBytes = 0
		t.bucketDuration = 0
	}

	duration := t.activeDuration
	wall := t.lastAt.Sub(t.firstAt)

	stats := IOStats{
		Duration:     duration,
		WallDuration: wall,
		TotalBytes:   t.totalBytes,
	}

	if duration > 0 && t.totalBytes > 0 {
		stats.Overall = float64(t.totalBytes) / duration.Seconds()
	}
	if wall > 0 && t.totalBytes > 0 {
		stats.OverallWall = float64(t.totalBytes) / wall.Seconds()
	}

	if len(t.samples) == 0 {
		return stats
	}

	sorted := make([]float64, len(t.samples))
	copy(sorted, t.samples)
	sort.Float64s(sorted)

	min := sorted[0]
	max := sorted[len(sorted)-1]

	var sum float64
	for _, v := range sorted {
		sum += v
	}
	avg := sum / float64(len(sorted))

	median := percentile(sorted, 50)

	stats.Min = min
	stats.Max = max
	stats.Avg = avg
	stats.Median = median

	stats.P50 = percentile(sorted, 50)
	stats.P75 = percentile(sorted, 75)
	stats.P80 = percentile(sorted, 80)
	stats.P90 = percentile(sorted, 90)
	stats.P95 = percentile(sorted, 95)
	stats.P99 = percentile(sorted, 99)

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
			" median=%s,"+
			" p75=%s,"+
			" p80=%s,"+
			" p90=%s,"+
			" p95=%s,"+
			" p99=%s,"+
			" max=%s\n"+
			"w:"+
			" dt=%s,"+
			" wall=%s,"+
			" bytes=%s (%d),"+
			" overall=%s,"+
			" overall_wall=%s,"+
			" min=%s,"+
			" avg=%s,"+
			" median=%s,"+
			" p75=%s,"+
			" p80=%s,"+
			" p90=%s,"+
			" p95=%s,"+
			" p99=%s,"+
			" max=%s\n",
		r.Duration, r.WallDuration, formatBytes(r.TotalBytes), r.TotalBytes,
		formatThroughput(r.Overall),
		formatThroughput(r.OverallWall),
		formatThroughput(r.Min),
		formatThroughput(r.Avg),
		formatThroughput(r.Median),
		formatThroughput(r.P75),
		formatThroughput(r.P80),
		formatThroughput(r.P90),
		formatThroughput(r.P95),
		formatThroughput(r.P99),
		formatThroughput(r.Max),

		w.Duration, w.WallDuration, formatBytes(w.TotalBytes), w.TotalBytes,
		formatThroughput(w.Overall),
		formatThroughput(w.OverallWall),
		formatThroughput(w.Min),
		formatThroughput(w.Avg),
		formatThroughput(w.Median),
		formatThroughput(w.P75),
		formatThroughput(w.P80),
		formatThroughput(w.P90),
		formatThroughput(w.P95),
		formatThroughput(w.P99),
		formatThroughput(w.Max),
	)
}
