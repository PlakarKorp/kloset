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

type IOStats struct {
	Duration   time.Duration
	TotalBytes int64

	Min     float64 // bytes/sec
	Avg     float64 // bytes/sec
	Median  float64 // bytes/sec
	Max     float64 // bytes/sec
	Overall float64 // overall throughput (bytes/sec)

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

	bucketBytes    int64
	bucketDuration time.Duration

	samples []float64
}

func newTracker() *tracker {
	return &tracker{}
}

// add accounts for n bytes processed over dt of *active* time.
func (t *tracker) add(n int64, dt time.Duration) {
	if n == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalBytes += n

	if dt <= 0 {
		// count the bytes, but don't create a throughput sample
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

func (t *tracker) Stats() IOStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	// flush any partially-filled bucket as one last sample
	if t.bucketDuration > 0 && t.bucketBytes > 0 {
		throughput := float64(t.bucketBytes) / t.bucketDuration.Seconds()
		t.samples = append(t.samples, throughput)
		t.bucketBytes = 0
		t.bucketDuration = 0
	}

	duration := t.activeDuration

	stats := IOStats{
		Duration:   duration,
		TotalBytes: t.totalBytes,
	}

	if duration > 0 && t.totalBytes > 0 {
		stats.Overall = float64(t.totalBytes) / duration.Seconds()
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

type Span struct {
	t    *tracker
	last time.Time
}

func (ioT *IOTracker) WriteSpan() *Span {
	now := time.Now()
	return &Span{
		t:    ioT.Write,
		last: now,
	}
}

func (ioT *IOTracker) ReadSpan() *Span {
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
	s.t.add(n, dt)
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
			" bytes=%s (%d),"+
			" overall=%s,"+
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
			" bytes=%s (%d),"+
			" overall=%s,"+
			" min=%s,"+
			" avg=%s,"+
			" median=%s,"+
			" p75=%s,"+
			" p80=%s,"+
			" p90=%s,"+
			" p95=%s,"+
			" p99=%s,"+
			" max=%s\n",
		r.Duration, formatBytes(r.TotalBytes), r.TotalBytes,
		formatThroughput(r.Overall),
		formatThroughput(r.Min),
		formatThroughput(r.Avg),
		formatThroughput(r.Median),
		formatThroughput(r.P75),
		formatThroughput(r.P80),
		formatThroughput(r.P90),
		formatThroughput(r.P95),
		formatThroughput(r.P99),
		formatThroughput(r.Max),

		w.Duration, formatBytes(w.TotalBytes), w.TotalBytes,
		formatThroughput(w.Overall),
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
