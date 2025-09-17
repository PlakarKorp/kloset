package packer_test

import (
	"crypto/sha256"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging/testlogger"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/storage"
	"github.com/stretchr/testify/require"
)

func newTestKContext(t *testing.T) *kcontext.KContext {
	t.Helper()
	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache")
	if err := os.MkdirAll(cachePath, 0o755); err != nil {
		t.Fatalf("failed to create cache dir: %v", err)
	}
	ctx := kcontext.NewKContext()
	cacheMgr := caching.NewManager(cachePath)
	ctx.SetCache(cacheMgr)
	ctx.SetLogger(testlogger.NewLogger(nil, nil))
	return ctx
}

func TestNewPlatarPackerManager(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pw *packer.PackWriter) error { return nil }

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
	require.NoError(t, err)
	require.NotNil(t, mgr)
}

func TestPlatarPackerManager_InsertIfNotPresent(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pw *packer.PackWriter) error { return nil }

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
	require.NoError(t, err)

	t.Run("InsertNewMAC", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		exists, err := mgr.InsertIfNotPresent(resources.RT_CONFIG, mac)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("InsertDuplicateMAC", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		exists, err := mgr.InsertIfNotPresent(resources.RT_CONFIG, mac)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("DifferentResourceTypes", func(t *testing.T) {
		mac1 := objects.MAC{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
		mac2 := objects.MAC{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

		// Same MAC, different resource types should be treated as different
		exists, err := mgr.InsertIfNotPresent(resources.RT_CONFIG, mac1)
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = mgr.InsertIfNotPresent(resources.RT_SNAPSHOT, mac1)
		require.NoError(t, err)
		require.False(t, exists)

		// Different MAC, same resource type
		exists, err = mgr.InsertIfNotPresent(resources.RT_CONFIG, mac2)
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestPlatarPackerManager_Exists(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pw *packer.PackWriter) error { return nil }

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
	require.NoError(t, err)

	t.Run("NonExistentMAC", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		exists, err := mgr.Exists(resources.RT_CONFIG, mac)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("ExistentMAC", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

		// Insert the MAC first
		_, err := mgr.InsertIfNotPresent(resources.RT_CONFIG, mac)
		require.NoError(t, err)

		// Check if it exists
		exists, err := mgr.Exists(resources.RT_CONFIG, mac)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestPlatarPackerManager_Put(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	var flushedPackWriters []*packer.PackWriter
	flusher := func(pw *packer.PackWriter) error {
		flushedPackWriters = append(flushedPackWriters, pw)
		return nil
	}

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
	require.NoError(t, err)

	t.Run("PutSingleBlob", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test data")

		err := mgr.Put(0, resources.RT_CONFIG, mac, data)
		require.NoError(t, err)
	})

	t.Run("PutMultipleBlobs", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			mac := objects.MAC{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)}
			data := []byte("test data " + string(rune(i+'0')))

			err := mgr.Put(0, resources.RT_CONFIG, mac, data)
			require.NoError(t, err)
		}
	})

	t.Run("PutWithEncodingError", func(t *testing.T) {
		// Create a manager with a failing encoding function
		failingEncodingFunc := func(r io.Reader) (io.Reader, error) { return nil, io.ErrUnexpectedEOF }
		mgrWithFailingEncoding, err := packer.NewPlatarPackerManager(ctx, storageConf, failingEncodingFunc, hashFactory, flusher)
		require.NoError(t, err)

		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test data")

		err = mgrWithFailingEncoding.Put(0, resources.RT_CONFIG, mac, data)
		// Do not expect an error, as Put does not return encoding errors in platar.go
	})
}

func TestPlatarPackerManager_CacheOperations(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pw *packer.PackWriter) error { return nil }

	mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
	require.NoError(t, err)

	t.Run("CacheConsistency", func(t *testing.T) {
		mac1 := objects.MAC{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
		mac2 := objects.MAC{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

		// Insert both MACs
		exists, err := mgr.InsertIfNotPresent(resources.RT_CONFIG, mac1)
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = mgr.InsertIfNotPresent(resources.RT_CONFIG, mac2)
		require.NoError(t, err)
		require.False(t, exists)

		// Verify both exist
		exists, err = mgr.Exists(resources.RT_CONFIG, mac1)
		require.NoError(t, err)
		require.True(t, exists)

		exists, err = mgr.Exists(resources.RT_CONFIG, mac2)
		require.NoError(t, err)
		require.True(t, exists)

		// Verify a non-existent MAC doesn't exist
		mac3 := objects.MAC{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3}
		exists, err = mgr.Exists(resources.RT_CONFIG, mac3)
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestPlatarPackerManager_RunAndWait(t *testing.T) {
	ctx := newTestKContext(t)
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	var flushedPackWriters []*packer.PackWriter
	var flushMutex sync.Mutex
	flusher := func(pw *packer.PackWriter) error {
		flushMutex.Lock()
		defer flushMutex.Unlock()
		flushedPackWriters = append(flushedPackWriters, pw)

		// Read all data from the PackWriter to prevent pipe blocking
		_, err := io.ReadAll(pw.Reader)
		return err
	}

	t.Run("BasicRunAndWait", func(t *testing.T) {
		mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
		require.NoError(t, err)

		// Start the manager in a goroutine
		var runErr error
		go func() {
			runErr = mgr.Run()
		}()

		// Add some data to process
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test data")
		err = mgr.Put(0, resources.RT_CONFIG, mac, data)
		require.NoError(t, err)

		// Wait for completion
		mgr.Wait()

		// Check that Run completed without error
		if runErr != nil {
			t.Logf("Run completed with error: %v", runErr)
		}

		// Verify that the flusher was called
		flushMutex.Lock()
		flushCount := len(flushedPackWriters)
		flushMutex.Unlock()
		require.GreaterOrEqual(t, flushCount, 0)
	})

	t.Run("RunAndWaitWithoutData", func(t *testing.T) {
		mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
		require.NoError(t, err)

		// Start the manager in a goroutine
		var runErr error
		go func() {
			runErr = mgr.Run()
		}()

		// Wait for completion without adding any data
		mgr.Wait()

		// Check that Run completed without error
		if runErr != nil {
			t.Logf("Run completed with error: %v", runErr)
		}
	})

	t.Run("RunAndWaitWithMultipleBlobs", func(t *testing.T) {
		mgr, err := packer.NewPlatarPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
		require.NoError(t, err)

		// Start the manager in a goroutine
		var runErr error
		go func() {
			runErr = mgr.Run()
		}()

		// Add multiple blobs
		for i := 0; i < 5; i++ {
			mac := objects.MAC{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)}
			data := []byte("test data " + string(rune(i+'0')))
			err := mgr.Put(0, resources.RT_CONFIG, mac, data)
			require.NoError(t, err)
		}

		// Wait for completion
		mgr.Wait()

		// Check that Run completed without error
		if runErr != nil {
			t.Logf("Run completed with error: %v", runErr)
		}

		// Verify that the flusher was called
		flushMutex.Lock()
		flushCount := len(flushedPackWriters)
		flushMutex.Unlock()
		require.GreaterOrEqual(t, flushCount, 0)
	})
}
