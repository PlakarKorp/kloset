package snapshot

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/stretchr/testify/require"
)

// stageRec builds the minimal record the warm stage inspects (Pathname, Err).
func stageRec(p string) *connectors.Record {
	return connectors.NewRecord(p, "", objects.FileInfo{}, nil, nil)
}

// runWarmStage wires warmVFSStage against an inert filesystem (no dirpack →
// PrefetchDirs is a no-op) so the channel mechanics can be tested in
// isolation. It returns the output channel and a done channel closed when
// the stage function itself has returned.
func runWarmStage(ctx context.Context, in chan *connectors.Record, window int) (chan *connectors.Record, chan struct{}) {
	out := make(chan *connectors.Record)
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&Builder{}).warmVFSStage(ctx, &vfs.Filesystem{}, in, out, window)
	}()
	return out, done
}

// collect drains out until closed, failing the test if it takes too long —
// every stage bug in this area is a hang, so everything is deadline-guarded.
func collect(t *testing.T, out <-chan *connectors.Record) []*connectors.Record {
	t.Helper()
	var got []*connectors.Record
	deadline := time.After(5 * time.Second)
	for {
		select {
		case rec, ok := <-out:
			if !ok {
				return got
			}
			got = append(got, rec)
		case <-deadline:
			t.Fatalf("stage output did not close (got %d records so far)", len(got))
		}
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("warmVFSStage did not return")
	}
}

// TestWarmVFSStagePassthroughOrder: records cross the stage unmodified and
// in order, across multiple full windows plus a partial tail, and the output
// channel closes when the input does.
func TestWarmVFSStagePassthroughOrder(t *testing.T) {
	in := make(chan *connectors.Record)
	out, done := runWarmStage(context.Background(), in, 4)

	const n = 10 // window 4: two full batches + a tail of 2
	go func() {
		for i := range n {
			in <- stageRec(fmt.Sprintf("/dir/file%02d", i))
		}
		close(in)
	}()

	got := collect(t, out)
	require.Len(t, got, n)
	for i, rec := range got {
		require.Equal(t, fmt.Sprintf("/dir/file%02d", i), rec.Pathname, "order not preserved")
	}
	waitDone(t, done)
}

// TestWarmVFSStageTailFlush: fewer records than one window must still be
// delivered when the importer closes the channel — the blocking fill cannot
// hold the tail hostage.
func TestWarmVFSStageTailFlush(t *testing.T) {
	in := make(chan *connectors.Record)
	out, done := runWarmStage(context.Background(), in, 100)

	go func() {
		in <- stageRec("/a")
		in <- stageRec("/b")
		close(in)
	}()

	got := collect(t, out)
	require.Len(t, got, 2)
	waitDone(t, done)
}

// TestWarmVFSStageEmptyInput: closing the input with no records closes the
// output with no records, no hang.
func TestWarmVFSStageEmptyInput(t *testing.T) {
	in := make(chan *connectors.Record)
	out, done := runWarmStage(context.Background(), in, 8)
	close(in)

	require.Empty(t, collect(t, out))
	waitDone(t, done)
}

// TestWarmVFSStageErrorRecordsFlow: records carrying an importer error are
// not warmed but must flow through untouched — dropping them would silently
// lose error reporting for those paths.
func TestWarmVFSStageErrorRecordsFlow(t *testing.T) {
	in := make(chan *connectors.Record)
	out, done := runWarmStage(context.Background(), in, 4)

	go func() {
		r := stageRec("/broken")
		r.Err = fmt.Errorf("importer failed on this one")
		in <- r
		in <- stageRec("/fine")
		close(in)
	}()

	got := collect(t, out)
	require.Len(t, got, 2)
	require.Error(t, got[0].Err)
	require.NoError(t, got[1].Err)
	waitDone(t, done)
}

// TestWarmVFSStageCtxCancel: cancelling the context mid-stream terminates
// the stage — output closes and the stage function returns — even when the
// consumer has stopped reading. The importer side still owns closing `in`.
func TestWarmVFSStageCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan *connectors.Record)
	out, done := runWarmStage(ctx, in, 2)

	// Fill one window so a batch is in flight, then cancel and close.
	in <- stageRec("/a")
	in <- stageRec("/b")
	cancel()
	close(in)

	// Drain whatever the stage manages to deliver; it must close out.
	collect(t, out)
	waitDone(t, done)
}
