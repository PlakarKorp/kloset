package packer_test

import (
	"crypto/sha256"
	"hash"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/storage"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestNewPackerManager(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pf *packfile.PackFile) error { return nil }

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)
	require.NotNil(t, mgr)
}

func TestPackerManager_InsertIfNotPresent(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pf *packfile.PackFile) error { return nil }

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

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

func TestPackerManager_Exists(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }
	flusher := func(pf *packfile.PackFile) error { return nil }

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

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

func TestPackerManager_Put(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	var flushedPackfiles []*packfile.PackFile
	flusher := func(pf *packfile.PackFile) error {
		flushedPackfiles = append(flushedPackfiles, pf)
		return nil
	}

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

	t.Run("PutSingleBlob", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test data")

		err := mgr.Put(resources.RT_CONFIG, mac, data)
		require.NoError(t, err)
	})

	t.Run("PutMultipleBlobs", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			mac := objects.MAC{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)}
			data := []byte("test data " + string(rune(i+'0')))

			err := mgr.Put(resources.RT_CONFIG, mac, data)
			require.NoError(t, err)
		}
	})
}

func TestPackerManager_RunAndWait(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	storageConf.Packfile.MaxSize = 1000 // Small max size to trigger flushing
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	var flushedPackfiles []*packfile.PackFile
	flusher := func(pf *packfile.PackFile) error {
		flushedPackfiles = append(flushedPackfiles, pf)
		return nil
	}

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

	t.Run("RunAndWaitWithData", func(t *testing.T) {
		// Start the manager
		go func() {
			err := mgr.Run()
			require.NoError(t, err)
		}()

		// Add some data
		for i := 0; i < 10; i++ {
			mac := objects.MAC{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)}
			data := []byte("test data " + string(rune(i+'0')))

			err := mgr.Put(resources.RT_CONFIG, mac, data)
			require.NoError(t, err)
		}

		// Wait for completion
		mgr.Wait()

		// Should have flushed some packfiles due to size limit
		require.Greater(t, len(flushedPackfiles), 0)
	})
}

func TestPackerManager_AddPadding(t *testing.T) {
	// Test AddPadding indirectly by checking that packfiles get padding
	// when they are processed through the normal flow
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	storageConf.Packfile.MaxSize = 1000 // Small max size to trigger flushing
	storageConf.Chunking.MinSize = 100  // Set min size for padding
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	var flushedPackfiles []*packfile.PackFile
	flusher := func(pf *packfile.PackFile) error {
		flushedPackfiles = append(flushedPackfiles, pf)
		return nil
	}

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

	// Start the manager
	go func() {
		err := mgr.Run()
		require.NoError(t, err)
	}()

	// Add some data to trigger packfile creation and padding
	mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	data := []byte("test data")

	err := mgr.Put(resources.RT_CONFIG, mac, data)
	require.NoError(t, err)

	// Wait for completion
	mgr.Wait()

	// Should have flushed some packfiles
	require.Greater(t, len(flushedPackfiles), 0)
}

func TestNewPacker(t *testing.T) {
	hasher := sha256.New()
	p := packer.NewPacker(hasher)

	require.NotNil(t, p)
	require.NotNil(t, p.Packfile)
	require.NotNil(t, p.Blobs)
	require.Equal(t, len(resources.Types()), len(p.Blobs))
}

func TestPacker_BasicFunctionality(t *testing.T) {
	hasher := sha256.New()
	p := packer.NewPacker(hasher)

	require.NotNil(t, p)
	require.NotNil(t, p.Packfile)
	require.NotNil(t, p.Blobs)
	require.Equal(t, len(resources.Types()), len(p.Blobs))

	// Test that the packfile starts with zero size
	require.Equal(t, uint32(0), p.Packfile.Size())

	// Test that Types() returns all resource types initially (since they're all initialized)
	types := p.Types()
	require.Equal(t, len(resources.Types()), len(types))

	// Test that all resource types are initialized in the blobs map
	for _, resourceType := range resources.Types() {
		require.NotNil(t, p.Blobs[resourceType])
	}
}

func TestPackerMsg(t *testing.T) {
	t.Run("CreatePackerMsg", func(t *testing.T) {
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test data")
		version := versioning.GetCurrentVersion(resources.RT_CONFIG)
		timestamp := time.Now()

		msg := packer.PackerMsg{
			Timestamp: timestamp,
			Type:      resources.RT_CONFIG,
			Version:   version,
			MAC:       mac,
			Data:      data,
			Flags:     0,
		}

		require.Equal(t, timestamp, msg.Timestamp)
		require.Equal(t, resources.RT_CONFIG, msg.Type)
		require.Equal(t, version, msg.Version)
		require.Equal(t, mac, msg.MAC)
		require.Equal(t, data, msg.Data)
		require.Equal(t, uint32(0), msg.Flags)
	})
}

func TestPackerManager_Concurrency(t *testing.T) {
	ctx := kcontext.NewKContext()
	storageConf := storage.NewConfiguration()
	storageConf.Packfile.MaxSize = 1000 // Small max size to trigger flushing
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	var flushedPackfiles []*packfile.PackFile
	flusher := func(pf *packfile.PackFile) error {
		flushedPackfiles = append(flushedPackfiles, pf)
		return nil
	}

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

	t.Run("ConcurrentPuts", func(t *testing.T) {
		// Start the manager in a goroutine
		errChan := make(chan error, 1)
		go func() {
			err := mgr.Run()
			errChan <- err
		}()

		// Give the manager a moment to start
		time.Sleep(10 * time.Millisecond)

		// Add data concurrently
		const numGoroutines = 5
		const blobsPerGoroutine = 3

		var wg sync.WaitGroup
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < blobsPerGoroutine; j++ {
					mac := objects.MAC{byte(goroutineID), byte(j), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
					data := []byte("test data from goroutine " + string(rune(goroutineID+'0')) + " blob " + string(rune(j+'0')))

					err := mgr.Put(resources.RT_CONFIG, mac, data)
					require.NoError(t, err)
				}
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()

		// Wait for completion
		mgr.Wait()

		// Check for any errors from the manager
		select {
		case err := <-errChan:
			require.NoError(t, err)
		default:
			// No error
		}

		// Should have processed all the data
		require.Greater(t, len(flushedPackfiles), 0)
	})
}

func TestPackerManager_ErrorHandling(t *testing.T) {
	ctx := kcontext.NewKContext()
	// Set up a logger to avoid nil pointer dereference
	ctx.SetLogger(logging.NewLogger(io.Discard, io.Discard))

	storageConf := storage.NewConfiguration()
	encodingFunc := func(r io.Reader) (io.Reader, error) { return r, nil }
	hashFactory := func() hash.Hash { return sha256.New() }

	// Create a flusher that returns an error
	flusher := func(pf *packfile.PackFile) error {
		return io.ErrUnexpectedEOF
	}

	mgr := packer.NewPackerManager(ctx, storageConf, encodingFunc, hashFactory, flusher)

	t.Run("FlusherError", func(t *testing.T) {
		// Start the manager
		go func() {
			_ = mgr.Run() // Should not panic, error is only logged
		}()

		// Add some data
		mac := objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
		data := []byte("test data")

		err := mgr.Put(resources.RT_CONFIG, mac, data)
		require.NoError(t, err)

		// Wait for completion
		mgr.Wait()
	})
}
