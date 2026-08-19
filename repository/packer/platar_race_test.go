package packer_test

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// TestPlatarPackerDrainsBufferOnWait is the regression test for the buffered-
// channel data-loss bug fixed in "platar: Fix flushing." (a8493b9e).
//
// The blob channel is buffered, so when Wait closes `closing` the worker's
// outer select sees both `<-closing` and `<-packerChan` ready, picks pseudo-
// randomly, and — before the fix — could exit on the closing arm while blobs
// were still buffered. Any Put that returned nil for those blobs had the
// manager's word that it was committed, but the blob never reached
// WriteBlob.
//
// Strategy: park the worker on the io.Pipe in WriteBlob for the first blob,
// stuff the channel with concurrent Puts so the buffer fills, then call Wait
// and only afterwards release the pipe. When the worker returns to its
// select, `closing` is closed AND the buffer is full — exactly the race
// condition the fix is targeting. We then assert every Put that returned
// nil shows up in PackWriter.Footer.Count.
func TestPlatarPackerDrainsBufferOnWait(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()

	flusherStart := make(chan struct{})
	var capturedPW *packer.PackWriter
	flusher := func(pw *packer.PackWriter) error {
		capturedPW = pw
		<-flusherStart
		_, err := io.ReadAll(pw.Reader)
		return err
	}

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, identityEncoder, sha256Factory, flusher)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	// Fire many concurrent Puts. The first one the worker picks up will park
	// it on the io.Pipe (flusher is blocked on flusherStart); the rest fill
	// the buffered packerChan, then later ones park on the send.
	const N = 256
	var landed atomic.Int64
	var producers sync.WaitGroup
	for i := range N {
		producers.Add(1)
		go func(i int) {
			defer producers.Done()
			mac := objects.MAC{byte(i), byte(i >> 8)}
			if err := mgr.Put(0, resources.RT_CONFIG, mac, []byte("payload"), false); err == nil {
				landed.Add(1)
			}
		}(i)
	}

	// Give producers time to either commit (land in the buffer) or park on
	// the send.
	time.Sleep(50 * time.Millisecond)

	// Call Wait while the worker is still parked. This closes `closing` (and
	// in the current code also closes packerChan). Wait itself blocks on
	// packerChanDone until Run returns.
	waitDone := make(chan struct{})
	go func() {
		mgr.Wait()
		close(waitDone)
	}()

	// Give Wait a moment to actually run its closes before we let the worker
	// unblock — this is what produces the "both arms ready" race in the
	// worker's outer select.
	time.Sleep(20 * time.Millisecond)

	close(flusherStart)

	producers.Wait()
	<-waitDone
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	require.NotNil(t, capturedPW, "flusher was never invoked")

	// Every Put that returned nil committed a blob; every committed blob must
	// reach WriteBlob (which is the only thing that bumps Footer.Count).
	expected := landed.Load()
	got := int64(capturedPW.Footer.Count)
	if got != expected {
		t.Fatalf("Puts committed: %d; blobs in packfile: %d; lost %d",
			expected, got, expected-got)
	}
}

// TestPlatarPackerPutAfterWait pins the contract that 2c01fd8 ("packer:
// don't panic on Put after Wait") introduced and that snapshot.Backup's
// error paths rely on: once Wait has returned, a stray Put must return an
// error, not panic. The current "Fix flushing" commit closes packerChan in
// Wait, which makes the Put select's send case panic on "send on closed
// channel" — see the review.
func TestPlatarPackerPutAfterWait(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	flusher := func(pw *packer.PackWriter) error {
		_, err := io.ReadAll(pw.Reader)
		return err
	}

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, identityEncoder, sha256Factory, flusher)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()
	mgr.Wait()
	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Put after Wait panicked: %v", r)
		}
	}()
	mac := objects.MAC{99}
	if err := mgr.Put(0, resources.RT_CHUNK, mac, []byte("x"), false); err == nil {
		t.Fatalf("expected Put after Wait to return an error, got nil")
	}
}

// TestPlatarPackerPutDuringWait races many concurrent Put callers against a
// single Wait — the platar equivalent of TestSeqPackerPutDuringWait. Under
// the contract pinned by 2c01fd8, no producer may panic.
func TestPlatarPackerPutDuringWait(t *testing.T) {
	const producers = 32
	const putsPerProducer = 200

	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	flusher := func(pw *packer.PackWriter) error {
		_, err := io.ReadAll(pw.Reader)
		return err
	}

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, identityEncoder, sha256Factory, flusher)
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	var producersDone sync.WaitGroup
	var panics atomic.Int32
	for p := range producers {
		producersDone.Add(1)
		go func(seed int) {
			defer producersDone.Done()
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
					t.Errorf("producer %d panicked: %v", seed, r)
				}
			}()
			for i := range putsPerProducer {
				mac := objects.MAC{byte(seed), byte(i), byte(i >> 8)}
				// After shutdown Put must return an error, not panic.
				_ = mgr.Put(0, resources.RT_CHUNK, mac, []byte("payload"), false)
			}
		}(p)
	}

	// Let producers ramp up so a meaningful subset is mid-Put when Wait fires.
	time.Sleep(10 * time.Millisecond)
	mgr.Wait()
	producersDone.Wait()

	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := panics.Load(); got != 0 {
		t.Fatalf("producers panicked %d times; expected 0", got)
	}
}
