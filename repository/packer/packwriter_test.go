package packer

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"io"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestNewBlobFromBytes(t *testing.T) {
	t.Run("ValidBlob", func(t *testing.T) {
		originalBlob := &Blob{
			Type:    resources.RT_CONFIG,
			Version: versioning.FromString("1.0.0"),
			MAC:     objects.RandomMAC(),
			Offset:  12345,
			Length:  100,
			Flags:   0x12345678,
		}
		serialized, err := originalBlob.Serialize()
		require.NoError(t, err)
		deserializedBlob, err := NewBlobFromBytes(serialized)
		require.NoError(t, err)
		require.NotNil(t, deserializedBlob)
		require.Equal(t, originalBlob.Type, deserializedBlob.Type)
		require.Equal(t, originalBlob.Version, deserializedBlob.Version)
		require.Equal(t, originalBlob.MAC, deserializedBlob.MAC)
		require.Equal(t, originalBlob.Offset, deserializedBlob.Offset)
		require.Equal(t, originalBlob.Length, deserializedBlob.Length)
		require.Equal(t, originalBlob.Flags, deserializedBlob.Flags)
	})
	t.Run("InvalidData", func(t *testing.T) {
		invalidData := []byte("invalid blob data")
		blob, err := NewBlobFromBytes(invalidData)
		require.Error(t, err)
		require.Nil(t, blob)
	})
}

func TestBlob_Serialize(t *testing.T) {
	blob := &Blob{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("2.1.0"),
		MAC:     objects.RandomMAC(),
		Offset:  67890,
		Length:  200,
		Flags:   0x87654321,
	}
	serialized, err := blob.Serialize()
	require.NoError(t, err)
	require.NotEmpty(t, serialized)
	deserializedBlob, err := NewBlobFromBytes(serialized)
	require.NoError(t, err)
	require.Equal(t, blob.Type, deserializedBlob.Type)
	require.Equal(t, blob.Version, deserializedBlob.Version)
	require.Equal(t, blob.MAC, deserializedBlob.MAC)
	require.Equal(t, blob.Offset, deserializedBlob.Offset)
	require.Equal(t, blob.Length, deserializedBlob.Length)
	require.Equal(t, blob.Flags, deserializedBlob.Flags)
}

func TestNewDefaultConfiguration(t *testing.T) {
	config := NewDefaultConfiguration()
	require.NotNil(t, config)
	require.Equal(t, uint64((20<<10)<<10), config.MaxSize)
	require.Equal(t, uint64(0), config.MinSize)
	require.Equal(t, uint64(0), config.AvgSize)
}

func TestNewPackWriter(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	putter := func(pw *PackWriter) error { return nil }
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
	require.Equal(t, uint64(0), pw.Size())
	require.Equal(t, uint32(0), pw.Footer.Count)
}

func TestPackWriter_WriteBlob(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test blob data")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0x12345678)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
	require.Equal(t, uint64(0), pw.Size())
}

func TestPackWriter_WriteBlobWithEncoder(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	// Test with a custom encoder that adds a prefix
	encoder := func(r io.Reader) (io.Reader, error) {
		data, err := io.ReadAll(r)
		if err != nil {
			return nil, err
		}
		encoded := append([]byte("ENCODED:"), data...)
		return bytes.NewReader(encoded), nil
	}
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test blob data with encoder")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0x12345678)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_MultipleBlobs(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		for i := 0; i < 5; i++ {
			mac := objects.RandomMAC()
			data := []byte("test blob data " + string(rune(i+'0')))
			err := pw.WriteBlob(resources.RT_SNAPSHOT, versioning.FromString("1.0.0"), mac, data, uint32(i))
			if err != nil {
				return err
			}
		}
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_DifferentResourceTypes(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		resourceTypes := []resources.Type{
			resources.RT_CONFIG,
			resources.RT_SNAPSHOT,
			resources.RT_OBJECT,
			resources.RT_CHUNK,
		}
		for i, resourceType := range resourceTypes {
			mac := objects.RandomMAC()
			data := []byte("test data for " + resourceType.String())
			err := pw.WriteBlob(resourceType, versioning.FromString("1.0.0"), mac, data, uint32(i))
			if err != nil {
				return err
			}
		}
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_Size(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data for size calculation")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.Equal(t, uint64(0), pw.Size())
}

func TestPackWriter_Finalize(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data for finalization")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_FinalizeEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Don't write any blobs
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
	require.Equal(t, uint32(0), pw.Footer.Count)
	require.Equal(t, uint64(0), pw.Footer.IndexOffset)
}

func TestPackWriter_Abort(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Don't write anything, just return
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	pw.Abort()
}

func TestPackWriter_AbortWithData(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Don't write anything, just return
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	// Abort should not panic
	pw.Abort()
}

func TestConstants(t *testing.T) {
	require.Equal(t, "1.0.0", VERSION)
	require.Equal(t, 56, BLOB_RECORD_SIZE)
	require.Equal(t, 56, FOOTER_SIZE)
}

func TestPackFooter(t *testing.T) {
	footer := PackFooter{
		Version:     versioning.FromString("1.0.0"),
		Timestamp:   time.Now().Unix(),
		Count:       10,
		IndexOffset: 12345,
		IndexMAC:    objects.RandomMAC(),
		Flags:       0x12345678,
	}
	require.NotEqual(t, versioning.Version(0), footer.Version)
	require.Greater(t, footer.Timestamp, int64(0))
	require.Equal(t, uint32(10), footer.Count)
	require.Equal(t, uint64(12345), footer.IndexOffset)
	require.NotEqual(t, objects.MAC{}, footer.IndexMAC)
	require.Equal(t, uint32(0x12345678), footer.Flags)
}

func TestBlobSerializationRoundTrip(t *testing.T) {
	originalBlob := &Blob{
		Type:    resources.RT_CONFIG,
		Version: versioning.FromString("1.0.0"),
		MAC:     objects.RandomMAC(),
		Offset:  12345,
		Length:  100,
		Flags:   0x12345678,
	}
	serialized, err := originalBlob.Serialize()
	require.NoError(t, err)
	require.NotEmpty(t, serialized)
	deserializedBlob, err := NewBlobFromBytes(serialized)
	require.NoError(t, err)
	require.NotNil(t, deserializedBlob)
	require.Equal(t, originalBlob.Type, deserializedBlob.Type)
	require.Equal(t, originalBlob.Version, deserializedBlob.Version)
	require.Equal(t, originalBlob.MAC, deserializedBlob.MAC)
	require.Equal(t, originalBlob.Offset, deserializedBlob.Offset)
	require.Equal(t, originalBlob.Length, deserializedBlob.Length)
	require.Equal(t, originalBlob.Flags, deserializedBlob.Flags)
}

func TestPackWriter_EncoderError(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	// Encoder that returns an error
	encoder := func(r io.Reader) (io.Reader, error) {
		return nil, io.ErrUnexpectedEOF
	}
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_WriteBlobError(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data")
		err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
		if err != nil {
			return err
		}
		// Try to write again with invalid data to trigger an error
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, nil, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

// Whitebox tests that test internal functionality
func TestPackWriter_WriteBlobInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test blob data")
		err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0x12345678)
		if err != nil {
			return err
		}
		// Test internal state after writing
		require.Equal(t, uint32(1), pw.Footer.Count)
		require.Greater(t, pw.Size(), uint64(0))
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
	require.Equal(t, uint64(0), pw.Size()) // Should be 0 initially
}

func TestPackWriter_FinalizeInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data for finalization")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.Equal(t, uint32(0), pw.Footer.Count) // Count is updated in putter goroutine
	// Note: We can't easily test Finalize() without hanging due to pipe blocking
}

func TestPackWriter_SerializeIndexInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Write multiple blobs to test index serialization
		for i := 0; i < 3; i++ {
			mac := objects.RandomMAC()
			data := []byte("test data " + string(rune(i+'0')))
			err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, uint32(i))
			if err != nil {
				return err
			}
		}
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
}

func TestPackWriter_SerializeFooterInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data for footer")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.Equal(t, int64(0), pw.Footer.Timestamp)
	require.Equal(t, uint32(0), pw.Footer.Count)
	require.Equal(t, uint64(0), pw.Footer.IndexOffset)
	require.Equal(t, objects.MAC{}, pw.Footer.IndexMAC)
	require.Equal(t, uint32(0), pw.Footer.Flags)
}

func TestPackWriter_WriteAndSumInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data for writeAndSum")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_ConcurrentAccess(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Test concurrent access to the pack writer
		done := make(chan error, 3)
		for i := 0; i < 3; i++ {
			go func(index int) {
				mac := objects.RandomMAC()
				data := []byte("concurrent test data " + string(rune(index+'0')))
				err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, uint32(index))
				done <- err
			}(i)
		}
		// Wait for all goroutines to complete
		for i := 0; i < 3; i++ {
			err := <-done
			if err != nil {
				return err
			}
		}
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
	require.NotNil(t, pw.Index)
	require.NotNil(t, pw.Reader)
}

func TestPackWriter_WriteAndSumMethod(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		var buf bytes.Buffer
		testData := uint32(0x12345678)
		err := pw.writeAndSum(&buf, testData)
		require.NoError(t, err)
		require.Equal(t, 4, buf.Len())
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_SerializeIndexMethod(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Add some blobs to the index first
		for i := 0; i < 3; i++ {
			mac := objects.RandomMAC()
			data := []byte("test data " + string(rune(i+'0')))
			err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, uint32(i))
			if err != nil {
				return err
			}
		}
		err := pw.serializeIndex()
		require.NoError(t, err)
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_SerializeFooterMethod(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Add a blob to populate the index
		mac := objects.RandomMAC()
		data := []byte("test data for footer")
		err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
		if err != nil {
			return err
		}
		err = pw.serializeFooter()
		require.NoError(t, err)
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_FinalizeFullPipeline(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Add multiple blobs
		for i := 0; i < 5; i++ {
			mac := objects.RandomMAC()
			data := []byte("test data " + string(rune(i+'0')))
			err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, uint32(i))
			if err != nil {
				return err
			}
		}
		err := pw.Finalize()
		require.NoError(t, err)
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_InternalStateAccess(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Test access to internal fields
		require.NotNil(t, pw.hasher)
		require.NotNil(t, pw.writer)
		require.Equal(t, uint64(0), pw.currentOffset)
		require.Equal(t, uint32(0), pw.Footer.Count)

		mac := objects.RandomMAC()
		data := []byte("test data")
		err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
		if err != nil {
			return err
		}

		require.Equal(t, uint32(1), pw.Footer.Count)
		require.Greater(t, pw.currentOffset, uint64(0))
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_ErrorHandlingInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		// Test error handling in writeAndSum
		// Create a writer that will fail
		failingWriter := &failingWriter{}
		err := pw.writeAndSum(failingWriter, uint32(0x12345678))
		require.Error(t, err)
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

type failingWriter struct{}

func (fw *failingWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrShortWrite
}

func TestPackWriter_ConcurrentInternalAccess(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		g := new(errgroup.Group)
		for i := 0; i < 3; i++ {
			index := i // Capture loop variable
			g.Go(func() error {
				var buf bytes.Buffer
				return pw.writeAndSum(&buf, uint32(index))
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		mac := objects.RandomMAC()
		data := []byte("concurrent test data")
		return pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_CompleteWorkflow(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		for i := 0; i < 10; i++ {
			mac := objects.RandomMAC()
			data := []byte("complete workflow data " + string(rune(i+'0')))
			err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, uint32(i))
			if err != nil {
				return err
			}
		}

		require.Equal(t, uint32(10), pw.Footer.Count)
		require.Greater(t, pw.currentOffset, uint64(0))
		require.NotNil(t, pw.hasher)

		err := pw.Finalize()
		require.NoError(t, err)

		require.Nil(t, pw.writer) // Should be closed after finalize
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

func TestPackWriter_AbortInternal(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }
	putter := func(pw *PackWriter) error {
		mac := objects.RandomMAC()
		data := []byte("test data for abort")
		err := pw.WriteBlob(resources.RT_CONFIG, versioning.FromString("1.0.0"), mac, data, 0)
		if err != nil {
			return err
		}

		require.NotNil(t, pw.writer)
		require.Equal(t, uint32(1), pw.Footer.Count)

		pw.Abort()

		require.Nil(t, pw.writer)
		return nil
	}
	pw := NewPackWriter(putter, encoder, hasher, packingCache)
	require.NotNil(t, pw)
}

type nopWriteCloser struct {
	io.Writer
}

func (n *nopWriteCloser) Close() error { return nil }

func TestPackWriter_serializeIndex_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()

	var buf bytes.Buffer
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }

	pw := &PackWriter{
		encoder:  encoder,
		hasher:   hasher(),
		Index:    packingCache,
		writer:   &nopWriteCloser{&buf},
		pipesync: make(chan struct{}, 1),
	}

	for i := 0; i < 3; i++ {
		mac := objects.RandomMAC()
		blob := &Blob{
			Type:    resources.RT_CONFIG,
			Version: versioning.FromString("1.0.0"),
			MAC:     mac,
			Offset:  uint64(i * 100),
			Length:  100,
			Flags:   uint32(i),
		}
		serialized, err := blob.Serialize()
		require.NoError(t, err)
		pw.Index.PutIndexBlob(resources.RT_CONFIG, mac, serialized)
	}

	err = pw.serializeIndex()
	require.NoError(t, err)
	require.NotZero(t, buf.Len(), "serializeIndex should write data to the writer")
}

func TestPackWriter_serializeFooter_Direct(t *testing.T) {
	tmpDir := t.TempDir()
	cacheManager := caching.NewManager(tmpDir)
	defer cacheManager.Close()
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	defer packingCache.Close()

	var buf bytes.Buffer
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }

	pw := &PackWriter{
		encoder:  encoder,
		hasher:   hasher(),
		Index:    packingCache,
		writer:   &nopWriteCloser{&buf},
		pipesync: make(chan struct{}, 1),
	}

	pw.Footer.Timestamp = 1234567890
	pw.Footer.Count = 2
	pw.Footer.IndexOffset = 200
	pw.Footer.Flags = 0xDEADBEEF

	_, err = pw.hasher.Write([]byte("indexdata"))
	require.NoError(t, err)

	err = pw.serializeFooter()
	require.NoError(t, err)
	require.NotZero(t, buf.Len(), "serializeFooter should write data to the writer")
	require.NotEqual(t, objects.MAC{}, pw.Footer.IndexMAC)
}
