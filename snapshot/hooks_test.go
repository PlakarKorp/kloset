package snapshot

import "time"

// SetSourceRoot overrides the root computed by NewSource, so tests can
// rebuild the layout left by builders that did not clean it.
func SetSourceRoot(s *Source, root string) {
	s.root = root
}

// StartCheckpointFlusherForTest replaces the hourly tick with a test-controlled channel.
func StartCheckpointFlusherForTest(builder *Builder, ticks <-chan time.Time) {
	builder.flushTick = time.NewTicker(time.Hour)
	go builder.flushDeltaState(ticks)
}
