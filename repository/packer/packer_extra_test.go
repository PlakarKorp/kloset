package packer_test

import (
	"crypto/sha256"
	"hash"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// newTestPackfileFactory returns a packfile.PackfileCtor that creates a real
// in-memory packfile.
func newTestPackfileFactory() packfile.PackfileCtor {
	return func(hf packfile.HashFactory) (packfile.Packfile, error) {
		return packfile.NewPackfileInMemory(hf)
	}
}

// identityEncoder passes data through unchanged.
func identityEncoder(r io.Reader) (io.Reader, error) { return r, nil }

// sha256Factory creates sha256 hashers.
func sha256Factory() hash.Hash { return sha256.New() }

// TestSeqPackerRunAndWait starts the packer manager, puts a single blob, then
// calls Wait and asserts the flusher was invoked.
func TestSeqPackerRunAndWait(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()

	var flushedCount int32
	var flushedPackfiles []packfile.Packfile
	var mu sync.Mutex
	flusher := func(pf packfile.Packfile) error {
		mu.Lock()
		flushedPackfiles = append(flushedPackfiles, pf)
		mu.Unlock()
		atomic.AddInt32(&flushedCount, 1)
		return nil
	}

	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)
	require.NotNil(t, mgr)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	err := mgr.Put(0, resources.RT_CONFIG, mac, []byte("test blob content"), false)
	require.NoError(t, err)

	mgr.Wait()

	require.NoError(t, <-runErr)
	// At least one packfile should have been flushed.
	require.Greater(t, atomic.LoadInt32(&flushedCount), int32(0))
}

// TestSeqPackerRunMultipleBlobs puts several blobs across different channels
// and verifies that all of them trigger flushing.
func TestSeqPackerRunMultipleBlobs(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	// Use a very small max packfile size so blobs get split across multiple packfiles.
	storageConf.Packfile.MaxSize = 100

	var totalFlushed int32
	flusher := func(pf packfile.Packfile) error {
		atomic.AddInt32(&totalFlushed, 1)
		return nil
	}

	ctx.MaxConcurrency = 2
	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	for i := range 8 {
		mac := objects.MAC{byte(i + 1)}
		data := make([]byte, 50) // 50 bytes × 8 blobs, max 100 bytes → forces splits
		for j := range data {
			data[j] = byte(i*10 + j)
		}
		require.NoError(t, mgr.Put(i%ctx.MaxConcurrency, resources.RT_CONFIG, mac, data, false))
	}

	mgr.Wait()
	require.NoError(t, <-runErr)
	require.Greater(t, atomic.LoadInt32(&totalFlushed), int32(0))
}

// TestSeqPackerPutChunkRouting verifies that RT_CHUNK blobs are routed to a
// data channel (not the metadata channel), completing without deadlock.
func TestSeqPackerPutChunkRouting(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.MaxConcurrency = 2 // need at least 1 data channel (nChan-1 ≥ 1)
	storageConf := storage.NewConfiguration()

	var flushed int32
	flusher := func(pf packfile.Packfile) error {
		atomic.AddInt32(&flushed, 1)
		return nil
	}

	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	for i := range 4 {
		mac := objects.MAC{byte(i + 100)}
		require.NoError(t, mgr.Put(-1, resources.RT_CHUNK, mac, []byte("chunk data"), false))
	}

	mgr.Wait()
	require.NoError(t, <-runErr)
	require.Greater(t, atomic.LoadInt32(&flushed), int32(0))
}

// TestSeqPackerInsertIfNotPresentPreventsDoubleFlush inserts the same MAC twice
// and asserts it is reported as already-present on the second call, before Run.
func TestSeqPackerInsertIfNotPresentPreventsDoubleFlush(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	flusher := func(pf packfile.Packfile) error { return nil }

	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	mac := objects.MAC{0xAB, 0xCD}

	present, err := mgr.InsertIfNotPresent(resources.RT_CHUNK, mac)
	require.NoError(t, err)
	require.False(t, present, "first insert should return false (was not present)")

	present, err = mgr.InsertIfNotPresent(resources.RT_CHUNK, mac)
	require.NoError(t, err)
	require.True(t, present, "second insert of same MAC should return true (already present)")
}

// TestSeqPackerExistsAfterInsert verifies Exists returns true after InsertIfNotPresent.
func TestSeqPackerExistsAfterInsert(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	flusher := func(pf packfile.Packfile) error { return nil }

	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	mac := objects.MAC{0xEF}
	exists, err := mgr.Exists(resources.RT_CONFIG, mac)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = mgr.InsertIfNotPresent(resources.RT_CONFIG, mac)
	require.NoError(t, err)

	exists, err = mgr.Exists(resources.RT_CONFIG, mac)
	require.NoError(t, err)
	require.True(t, exists)
}

// TestSeqPackerEmptyRun starts and immediately stops the packer without sending
// any blobs — verifies no deadlock / panic.
func TestSeqPackerEmptyRun(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	flusher := func(pf packfile.Packfile) error { return nil }

	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	mgr.Wait()
	require.NoError(t, <-runErr)
}

// TestSeqPackerObjectRouting verifies RT_OBJECT blobs are routed to the
// metadata channel (last channel), completing without deadlock.
func TestSeqPackerObjectRouting(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()

	var flushed int32
	flusher := func(pf packfile.Packfile) error {
		atomic.AddInt32(&flushed, 1)
		return nil
	}

	mgr := packer.NewSeqPackerManager(ctx, storageConf, identityEncoder, newTestPackfileFactory(), sha256Factory, flusher)

	runErr := make(chan error, 1)
	go func() { runErr <- mgr.Run() }()

	mac := objects.MAC{0x77}
	require.NoError(t, mgr.Put(0, resources.RT_OBJECT, mac, []byte("object data"), false))

	mgr.Wait()
	require.NoError(t, <-runErr)
	require.Greater(t, atomic.LoadInt32(&flushed), int32(0))
}
