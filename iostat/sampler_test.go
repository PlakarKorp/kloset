package iostat

import (
	"context"
	"sync"
	"testing"
	"time"
)

type capture struct {
	mu      sync.Mutex
	byScope map[string]int // number of reports seen per scope
	lastR   map[string]IOStats
	lastW   map[string]IOStats
}

func newCapture() *capture {
	return &capture{
		byScope: map[string]int{},
		lastR:   map[string]IOStats{},
		lastW:   map[string]IOStats{},
	}
}

func (c *capture) ReportIOStats(scope string, read, write IOStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byScope[scope]++
	c.lastR[scope] = read
	c.lastW[scope] = write
}

func (c *capture) count(scope string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.byScope[scope]
}

func TestSamplerReportsOnTick(t *testing.T) {
	tr := New()
	// feed some read + write bytes so the snapshots are non-zero
	rs := tr.GetReadSpan()
	rs.Add(1024)
	time.Sleep(60 * time.Millisecond)
	rs.Add(1024)
	ws := tr.GetWriteSpan()
	ws.Add(2048)
	time.Sleep(60 * time.Millisecond)
	ws.Add(2048)

	cap := newCapture()
	s := NewSampler(cap, 20*time.Millisecond,
		ScopedTracker{Name: "storage", T: tr},
	)
	s.Start(context.Background())
	time.Sleep(120 * time.Millisecond)
	s.Stop()

	// at ~20ms cadence over ~120ms we expect several ticks plus the final one
	if got := cap.count("storage"); got < 2 {
		t.Fatalf("expected at least 2 reports, got %d", got)
	}

	if cap.lastR["storage"].TotalBytes != 2048 {
		t.Errorf("expected read total 2048, got %d", cap.lastR["storage"].TotalBytes)
	}
	if cap.lastW["storage"].TotalBytes != 4096 {
		t.Errorf("expected write total 4096, got %d", cap.lastW["storage"].TotalBytes)
	}
}

func TestSamplerStopEmitsFinalSnapshot(t *testing.T) {
	tr := New()
	tr.GetReadSpan().Add(512)

	cap := newCapture()
	// large interval so no periodic tick fires before Stop
	s := NewSampler(cap, time.Hour, ScopedTracker{Name: "storage", T: tr})
	s.Start(context.Background())
	s.Stop()

	if got := cap.count("storage"); got != 1 {
		t.Fatalf("expected exactly 1 (final) report, got %d", got)
	}
	if cap.lastR["storage"].TotalBytes != 512 {
		t.Errorf("expected read total 512, got %d", cap.lastR["storage"].TotalBytes)
	}
}

func TestSamplerNoReporterOrScopesIsNoop(t *testing.T) {
	// nil reporter
	s := NewSampler(nil, time.Millisecond, ScopedTracker{Name: "x", T: New()})
	s.Start(context.Background())
	s.Stop() // must not panic

	// no scopes
	cap := newCapture()
	s = NewSampler(cap, time.Millisecond)
	s.Start(context.Background())
	s.Stop()
	if len(cap.byScope) != 0 {
		t.Fatalf("expected no reports, got %v", cap.byScope)
	}
}

func TestSamplerStopIsIdempotent(t *testing.T) {
	tr := New()
	tr.GetWriteSpan().Add(1)
	cap := newCapture()
	s := NewSampler(cap, time.Hour, ScopedTracker{Name: "s", T: tr})
	s.Start(context.Background())
	s.Stop()
	s.Stop() // second Stop must not panic or double-report

	if got := cap.count("s"); got != 1 {
		t.Fatalf("expected 1 report after idempotent stop, got %d", got)
	}
}

func TestSamplerContextCancelStops(t *testing.T) {
	tr := New()
	tr.GetReadSpan().Add(100)
	cap := newCapture()
	ctx, cancel := context.WithCancel(context.Background())
	s := NewSampler(cap, 10*time.Millisecond, ScopedTracker{Name: "s", T: tr})
	s.Start(ctx)
	cancel()
	// give the goroutine a moment to observe cancellation
	time.Sleep(30 * time.Millisecond)
	s.Stop() // still emits the final snapshot
	// no panic, and we got at least the final report
	if got := cap.count("s"); got < 1 {
		t.Fatalf("expected at least the final report, got %d", got)
	}
}

func TestSamplerDefaultInterval(t *testing.T) {
	s := NewSampler(newCapture(), 0, ScopedTracker{Name: "s", T: New()})
	if s.interval != DefaultSampleInterval {
		t.Fatalf("expected default interval %v, got %v", DefaultSampleInterval, s.interval)
	}
}
