package packer_test

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/resources"
)

// TestSeqPackerPutDuringWait is the regression test for
// https://github.com/PlakarKorp/plakar/issues/2106
//
// Before the fix, Put sent to packerChan unconditionally; Wait closed those
// channels. Any goroutine that called Put after Wait had begun would
// "panic: send on closed channel". After the fix, Put must return an error
// instead of panicking when the manager has been shut down.
//
// The test launches many concurrent Put callers and races them against a
// Wait. Done under -race -count=N to surface the panic reliably.
func TestSeqPackerPutDuringWait(t *testing.T) {
	const producers = 32
	const putsPerProducer = 200

	ctx := kcontext.NewKContext()
	ctx.MaxConcurrency = runtime.NumCPU()
	storageConf := storage.NewConfiguration()

	flusher := func(pf packfile.Packfile) error { return nil }
	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	// Producers continuously put blobs. They must never panic, even if they
	// happen to call Put after Wait has closed the channels.
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
				// Ignore the error: after shutdown Put will return one, which
				// is the contract under test.
				_ = mgr.Put(-1, resources.RT_CHUNK, mac, []byte("payload"), false)
			}
		}(p)
	}

	// Give the producers a moment to ramp up so that a meaningful subset of
	// them are mid-flight when Wait fires, then shut down.
	runtime.Gosched()
	mgr.Wait()
	producersDone.Wait()

	if err := <-runErr; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := panics.Load(); got != 0 {
		t.Fatalf("producers panicked %d times; expected 0", got)
	}
}

// TestSeqPackerPutAfterWait pins the explicit post-shutdown contract: once
// Wait has returned, any subsequent Put must return an error rather than
// panic.
func TestSeqPackerPutAfterWait(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.MaxConcurrency = 2
	storageConf := storage.NewConfiguration()
	flusher := func(pf packfile.Packfile) error { return nil }
	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

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
	if err := mgr.Put(-1, resources.RT_CHUNK, mac, []byte("x"), false); err == nil {
		t.Fatalf("expected Put after Wait to return an error, got nil")
	}
}
