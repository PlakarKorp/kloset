package snapshot_test

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/stretchr/testify/require"
)

// batchExporter drains every record first, then dumps all the acks into the
// results buffer and returns without yielding. That leaves the acks pending
// at the instant Export() hands back control, which is precisely the window
// in which a detached drain goroutine used to keep emitting after
// Snapshot.Export had returned.
type batchExporter struct {
	err  error
	sent int
}

func (e *batchExporter) Origin() string        { return "batch" }
func (e *batchExporter) Type() string          { return "batch" }
func (e *batchExporter) Root() string          { return "/" }
func (e *batchExporter) Flags() location.Flags { return 0 }

func (e *batchExporter) Ping(ctx context.Context) error  { return nil }
func (e *batchExporter) Close(ctx context.Context) error { return nil }

func (e *batchExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)

	var acks []*connectors.Result
	for record := range records {
		acks = append(acks, record.Ok())
	}
	for _, ack := range acks {
		results <- ack
		e.sent++
	}
	return e.err
}

// listen consumes the events bus in the background, tallying events by type.
// The bus is unbuffered, so a consumer has to be running for emit() to make
// progress at all. The tally is only read once the bus is closed and the
// consumer has drained, so it never races with delivery.
func listen(bus *events.EventsBUS) (tally func() map[string]int, wait func()) {
	seen := make(map[string]int)
	done := make(chan struct{})

	ch := bus.Listen()
	go func() {
		defer close(done)
		for e := range ch {
			seen[e.Type]++
		}
	}()

	return func() map[string]int { return seen }, func() { <-done }
}

// TestExportDoesNotOutliveReturn is the regression test for the
// "send on closed channel" panic: closing the events bus as soon as
// Snapshot.Export returns must be safe, which it only is if no goroutine
// spawned by Export is still holding an emitter.
func TestExportDoesNotOutliveReturn(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	// widen the results buffer so the exporter can park every ack without
	// blocking, and so return with the whole batch still pending.
	snap.AppContext().MaxConcurrency = 64

	tally, wait := listen(snap.AppContext().Events())

	exp := &batchExporter{}
	require.NoError(t, snap.Export(exp, "/", &snapshot.ExportOptions{}))

	// Nothing spawned by Export may emit past this point: a detached drain
	// still holding the emitter panics here with "send on closed channel".
	snap.AppContext().Events().Close()
	wait()

	require.NotZero(t, exp.sent, "exporter acked nothing, the test proves nothing")
	require.Equal(t, exp.sent, tally()["path.ok"],
		"Export returned before handling every ack")
}
