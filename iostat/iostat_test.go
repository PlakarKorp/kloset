package iostat

import (
	"strings"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tracker := New()
	if tracker == nil {
		t.Fatal("expected non-nil IOTracker")
	}
	if tracker.Read == nil {
		t.Fatal("expected non-nil Read tracker")
	}
	if tracker.Write == nil {
		t.Fatal("expected non-nil Write tracker")
	}
}

func TestTrackerAddAndStats(t *testing.T) {
	tr := newTracker()

	// Empty stats
	s := tr.Stats()
	if s.TotalBytes != 0 {
		t.Errorf("expected 0 total bytes, got %d", s.TotalBytes)
	}
	if s.Duration != 0 {
		t.Errorf("expected 0 duration, got %v", s.Duration)
	}

	// Add bytes with zero duration — counted but no throughput sample
	tr.add(100, 0)
	s = tr.Stats()
	if s.TotalBytes != 100 {
		t.Errorf("expected 100 total bytes, got %d", s.TotalBytes)
	}

	// Add bytes with a duration large enough to emit a sample
	tr.add(1<<20, 100*time.Millisecond)
	s = tr.Stats()
	if s.TotalBytes != 100+1<<20 {
		t.Errorf("unexpected total bytes %d", s.TotalBytes)
	}
	if s.Overall == 0 {
		t.Error("expected non-zero overall throughput")
	}
}

func TestTrackerAddZeroBytes(t *testing.T) {
	tr := newTracker()
	tr.add(0, 100*time.Millisecond)
	s := tr.Stats()
	if s.TotalBytes != 0 {
		t.Errorf("expected 0 bytes, got %d", s.TotalBytes)
	}
	if len(tr.samples) != 0 {
		t.Errorf("expected no samples after adding 0 bytes, got %d", len(tr.samples))
	}
}

func TestTrackerReset(t *testing.T) {
	tr := newTracker()
	tr.add(1<<20, 200*time.Millisecond)
	tr.Reset()

	s := tr.Stats()
	if s.TotalBytes != 0 {
		t.Errorf("expected 0 total bytes after reset, got %d", s.TotalBytes)
	}
	if s.Duration != 0 {
		t.Errorf("expected 0 duration after reset, got %v", s.Duration)
	}
}

func TestIOTrackerReset(t *testing.T) {
	iot := New()
	span := iot.GetWriteSpan()
	span.Add(1 << 20)

	iot.Reset()
	ws := iot.Write.Stats()
	if ws.TotalBytes != 0 {
		t.Errorf("expected 0 bytes after reset, got %d", ws.TotalBytes)
	}
}

func TestSpanAdd(t *testing.T) {
	iot := New()
	span := iot.GetWriteSpan()

	span.Add(4096)
	span.Add(8192)

	ws := iot.Write.Stats()
	if ws.TotalBytes != 4096+8192 {
		t.Errorf("expected %d bytes, got %d", 4096+8192, ws.TotalBytes)
	}
}

func TestSpanAddNil(t *testing.T) {
	var s *Span
	// should not panic
	s.Add(1000)
}

func TestGetReadSpan(t *testing.T) {
	iot := New()
	span := iot.GetReadSpan()

	span.Add(2048)
	span.Add(2048)

	rs := iot.Read.Stats()
	if rs.TotalBytes != 4096 {
		t.Errorf("expected 4096 bytes, got %d", rs.TotalBytes)
	}
}

func TestStatsMultipleSamples(t *testing.T) {
	tr := newTracker()

	// Add enough data with long durations so we get multiple samples
	for i := 0; i < 5; i++ {
		tr.add(1<<20, 200*time.Millisecond)
	}

	s := tr.Stats()
	if s.Min == 0 {
		t.Error("expected non-zero Min")
	}
	if s.Max == 0 {
		t.Error("expected non-zero Max")
	}
	if s.Avg == 0 {
		t.Error("expected non-zero Avg")
	}
	if s.P50 == 0 {
		t.Error("expected non-zero P50")
	}
	if s.P75 == 0 {
		t.Error("expected non-zero P75")
	}
	if s.P90 == 0 {
		t.Error("expected non-zero P90")
	}
	if s.P95 == 0 {
		t.Error("expected non-zero P95")
	}
	if s.P99 == 0 {
		t.Error("expected non-zero P99")
	}
}

func TestPercentileEdgeCases(t *testing.T) {
	// empty slice
	if v := percentile([]float64{}, 50); v != 0 {
		t.Errorf("expected 0 for empty slice, got %f", v)
	}

	// p <= 0 returns first element
	data := []float64{1.0, 2.0, 3.0}
	if v := percentile(data, 0); v != 1.0 {
		t.Errorf("expected 1.0 for p=0, got %f", v)
	}

	// p >= 100 returns last element
	if v := percentile(data, 100); v != 3.0 {
		t.Errorf("expected 3.0 for p=100, got %f", v)
	}

	// interpolation
	if v := percentile(data, 50); v == 0 {
		t.Error("expected non-zero for p=50")
	}
}

func TestSummaryString(t *testing.T) {
	iot := New()

	// Add some data so the summary is non-trivial
	wspan := iot.GetWriteSpan()
	wspan.Add(1 << 20)

	rspan := iot.GetReadSpan()
	rspan.Add(512)

	summary := iot.SummaryString()
	if !strings.Contains(summary, "r:") {
		t.Error("expected 'r:' in summary")
	}
	if !strings.Contains(summary, "w:") {
		t.Error("expected 'w:' in summary")
	}
}

func TestSummaryStringEmpty(t *testing.T) {
	iot := New()
	summary := iot.SummaryString()
	// Should not panic and should contain both sections
	if !strings.Contains(summary, "r:") || !strings.Contains(summary, "w:") {
		t.Error("expected both r: and w: sections in empty summary")
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		input    int64
		wantZero bool
	}{
		{0, true},
		{-1, true},
		{1024, false},
	}
	for _, c := range cases {
		got := formatBytes(c.input)
		isZero := got == "0 B"
		if isZero != c.wantZero {
			t.Errorf("formatBytes(%d) = %q, wantZero=%v", c.input, got, c.wantZero)
		}
	}
}

func TestFormatThroughput(t *testing.T) {
	cases := []struct {
		input    float64
		wantZero bool
	}{
		{0, true},
		{-1, true},
		{1024, false},
	}
	for _, c := range cases {
		got := formatThroughput(c.input)
		isZero := got == "0 B/s"
		if isZero != c.wantZero {
			t.Errorf("formatThroughput(%f) = %q, wantZero=%v", c.input, got, c.wantZero)
		}
	}
}

func TestBucketAccumulation(t *testing.T) {
	tr := newTracker()

	// Add bytes with a duration shorter than sampleWindow — no sample yet
	tr.add(1024, 10*time.Millisecond)
	if len(tr.samples) != 0 {
		t.Errorf("expected 0 samples for short duration, got %d", len(tr.samples))
	}

	// Stats flushes partial bucket
	s := tr.Stats()
	if s.TotalBytes != 1024 {
		t.Errorf("expected 1024 bytes, got %d", s.TotalBytes)
	}
}
