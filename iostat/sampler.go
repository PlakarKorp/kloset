package iostat

import (
	"context"
	"sync"
	"time"
)

const DefaultSampleInterval = time.Second

// ScopedTracker binds a scope name to an IOTracker so one Sampler can report
// several trackers side by side (e.g. source vs storage).
type ScopedTracker struct {
	Name string
	T    *IOTracker
}

// Reporter receives one snapshot per scope on every tick. It is an interface
// rather than *events.Emitter to keep iostat free of a dependency on events,
// which imports iostat.
type Reporter interface {
	ReportIOStats(scope string, read, write IOStats)
}

type ReporterFunc func(scope string, read, write IOStats)

func (f ReporterFunc) ReportIOStats(scope string, read, write IOStats) {
	f(scope, read, write)
}

// Sampler periodically snapshots a set of trackers and reports them, producing
// evenly spaced samples decoupled from the I/O rate for live graphing.
type Sampler struct {
	reporter Reporter
	interval time.Duration
	scopes   []ScopedTracker

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	started bool
	stopped bool
}

// NewSampler reports the given scopes on each tick; interval <= 0 uses
// DefaultSampleInterval.
func NewSampler(reporter Reporter, interval time.Duration, scopes ...ScopedTracker) *Sampler {
	if interval <= 0 {
		interval = DefaultSampleInterval
	}
	return &Sampler{
		reporter: reporter,
		interval: interval,
		scopes:   scopes,
	}
}

// Start launches the sampling goroutine, which runs until Stop or ctx
// cancellation. No-op with no reporter, no scopes, or if already started.
func (s *Sampler) Start(ctx context.Context) {
	if s == nil || s.reporter == nil || len(s.scopes) == 0 {
		return
	}

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()

	go func() {
		defer close(s.done)
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.report()
			}
		}
	}()
}

// Stop halts the goroutine and emits a final snapshot for the trailing window.
// Idempotent and safe to call without a prior Start.
func (s *Sampler) Stop() {
	if s == nil {
		return
	}

	s.mu.Lock()
	if !s.started || s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()

	cancel()
	<-done

	s.report()
}

func (s *Sampler) report() {
	for _, sc := range s.scopes {
		if sc.T == nil {
			continue
		}
		s.reporter.ReportIOStats(sc.Name, sc.T.Read.Stats(), sc.T.Write.Stats())
	}
}
