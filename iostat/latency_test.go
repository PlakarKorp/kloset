package iostat

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestLatencyEmpty(t *testing.T) {
	tr := newTracker()
	s := tr.Stats()
	if s.Latency != (LatencyStats{}) {
		t.Errorf("expected zero LatencyStats, got %+v", s.Latency)
	}
}

func TestLatencyExactFields(t *testing.T) {
	tr := newTracker()
	durations := []time.Duration{
		2 * time.Millisecond,
		8 * time.Millisecond,
		5 * time.Millisecond,
	}
	for _, d := range durations {
		tr.ObserveLatency(d)
	}

	s := tr.Stats().Latency
	if s.Count != 3 {
		t.Errorf("expected count 3, got %d", s.Count)
	}
	if s.Total != 15*time.Millisecond {
		t.Errorf("expected total 15ms, got %v", s.Total)
	}
	if s.Min != 2*time.Millisecond {
		t.Errorf("expected min 2ms, got %v", s.Min)
	}
	if s.Max != 8*time.Millisecond {
		t.Errorf("expected max 8ms, got %v", s.Max)
	}
	if s.Avg != 5*time.Millisecond {
		t.Errorf("expected avg 5ms, got %v", s.Avg)
	}
}

// A dense uniform distribution interpolates almost exactly (the ~9% bucket
// width only bounds the worst case), so 2% is tight enough to catch a broken
// interpolation, not just a broken clamp.
func TestLatencyPercentiles(t *testing.T) {
	tr := newTracker()
	// 1ms .. 1000ms, uniform
	for i := 1; i <= 1000; i++ {
		tr.ObserveLatency(time.Duration(i) * time.Millisecond)
	}

	s := tr.Stats().Latency
	checks := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"p50", s.P50, 500 * time.Millisecond},
		{"p75", s.P75, 750 * time.Millisecond},
		{"p80", s.P80, 800 * time.Millisecond},
		{"p90", s.P90, 900 * time.Millisecond},
		{"p95", s.P95, 950 * time.Millisecond},
		{"p99", s.P99, 990 * time.Millisecond},
	}
	for _, c := range checks {
		relErr := math.Abs(float64(c.got)-float64(c.want)) / float64(c.want)
		if relErr > 0.02 {
			t.Errorf("%s: got %v, want %v ±2%%", c.name, c.got, c.want)
		}
	}

	if s.P50 > s.P75 || s.P75 > s.P90 || s.P90 > s.P95 || s.P95 > s.P99 {
		t.Errorf("percentiles not monotonic: %+v", s)
	}
	if s.P99 > s.Max {
		t.Errorf("p99 %v exceeds max %v", s.P99, s.Max)
	}
}

func TestLatencyPercentilesClampedToObserved(t *testing.T) {
	tr := newTracker()
	tr.ObserveLatency(3 * time.Millisecond)

	s := tr.Stats().Latency
	for name, p := range map[string]time.Duration{
		"p50": s.P50, "p99": s.P99,
	} {
		if p < s.Min || p > s.Max {
			t.Errorf("%s %v outside observed range [%v, %v]", name, p, s.Min, s.Max)
		}
	}
}

func TestLatencyBucketExtremes(t *testing.T) {
	tr := newTracker()
	tr.ObserveLatency(0) // at the first bucket bound (1ns)
	tr.ObserveLatency(100 * time.Nanosecond)
	tr.ObserveLatency(50 * 365 * 24 * time.Hour) // beyond the last bucket (~36 years)

	s := tr.Stats().Latency
	if s.Count != 3 {
		t.Errorf("expected count 3, got %d", s.Count)
	}
	if s.Min != 0 {
		t.Errorf("expected min 0, got %v", s.Min)
	}
	if s.Max != 50*365*24*time.Hour {
		t.Errorf("expected max 50y, got %v", s.Max)
	}
	if s.P99 > s.Max {
		t.Errorf("p99 %v exceeds max %v", s.P99, s.Max)
	}
}

// Count, Total, Min, Avg and Max are exact integer durations: the float64
// nanosecond representation inside the histogram must not leak truncation
// into them.
func TestLatencyExactness(t *testing.T) {
	tr := newTracker()
	tr.ObserveLatency(1001 * time.Nanosecond)
	tr.ObserveLatency(1001 * time.Nanosecond)

	s := tr.Stats().Latency
	if s.Min != 1001 || s.Max != 1001 {
		t.Errorf("expected exact min/max 1001ns, got %v/%v", s.Min, s.Max)
	}
	if s.Total != 2002 {
		t.Errorf("expected exact total 2002ns, got %v", s.Total)
	}
	if s.Avg != 1001 {
		t.Errorf("expected exact avg 1001ns, got %v", s.Avg)
	}
	for name, p := range map[string]time.Duration{"p50": s.P50, "p99": s.P99} {
		if p != 1001 {
			t.Errorf("%s: expected 1001ns (single observed value), got %v", name, p)
		}
	}
}

// The nanosecond base unit must keep its ~9% resolution below 1µs: a
// sub-microsecond bulk (the realistic shape for a page-cache-backed local
// store) with one huge outlier may not drag the low percentiles upward.
func TestLatencySubMicrosecondWithOutlier(t *testing.T) {
	tr := newTracker()
	for i := 0; i < 999; i++ {
		tr.ObserveLatency(200 * time.Nanosecond)
	}
	tr.ObserveLatency(time.Second)

	s := tr.Stats().Latency
	for _, c := range []struct {
		name string
		got  time.Duration
	}{
		{"p50", s.P50},
		{"p90", s.P90},
	} {
		relErr := math.Abs(float64(c.got)-200) / 200
		if relErr > 0.10 {
			t.Errorf("%s: got %v, want ~200ns ±10%%", c.name, c.got)
		}
	}
}

// Sub-microsecond distributions must reflect the observed range instead of
// collapsing onto the extremes.
func TestLatencySubMicrosecond(t *testing.T) {
	tr := newTracker()
	for i := 100; i <= 900; i++ {
		tr.ObserveLatency(time.Duration(i) * time.Nanosecond)
	}

	s := tr.Stats().Latency
	for _, c := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"p50", s.P50, 500 * time.Nanosecond},
		{"p90", s.P90, 820 * time.Nanosecond},
	} {
		relErr := math.Abs(float64(c.got)-float64(c.want)) / float64(c.want)
		if relErr > 0.05 {
			t.Errorf("%s: got %v, want %v ±5%%", c.name, c.got, c.want)
		}
	}
}

func TestLatencyReset(t *testing.T) {
	tr := newTracker()
	tr.ObserveLatency(5 * time.Millisecond)
	tr.Reset()

	s := tr.Stats().Latency
	if s != (LatencyStats{}) {
		t.Errorf("expected zero LatencyStats after reset, got %+v", s)
	}
}

func TestSummaryStringLatency(t *testing.T) {
	ioT := New()

	// no observations: no latency segment at all
	if strings.Contains(ioT.SummaryString(), "lat:") {
		t.Errorf("expected no latency segment, got %q", ioT.SummaryString())
	}

	ioT.Write.ObserveLatency(5 * time.Millisecond)
	out := ioT.SummaryString()
	if !strings.Contains(out, "lat: n=1") {
		t.Errorf("expected latency segment in summary, got %q", out)
	}
}
