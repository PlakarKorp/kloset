package snapshot

import "time"

// SetSourceRoot overrides the root computed by NewSource, so tests can
// rebuild the layout left by builders that did not clean it.
func SetSourceRoot(s *Source, root string) {
	s.root = root
}

// StartCheckpointFlusherForTest replaces the hourly tick before starting the flusher.
func StartCheckpointFlusherForTest(builder *Builder, ticks <-chan time.Time) {
	builder.flushTick = time.NewTicker(time.Hour)
	builder.flushTick.C = ticks
	go builder.flushDeltaState()
}
