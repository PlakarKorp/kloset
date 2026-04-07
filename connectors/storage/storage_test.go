package storage_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/chunking"
	"github.com/PlakarKorp/kloset/compression"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/encryption"
	"github.com/PlakarKorp/kloset/hashing"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestStorageResourceString(t *testing.T) {
	t.Run("StorageResourceUndefined", func(t *testing.T) {
		require.Equal(t, "undefined", storage.StorageResourceUndefined.String())
	})

	t.Run("StorageResourcePackfile", func(t *testing.T) {
		require.Equal(t, "packfile", storage.StorageResourcePackfile.String())
	})

	t.Run("StorageResourceState", func(t *testing.T) {
		require.Equal(t, "state", storage.StorageResourceState.String())
	})

	t.Run("StorageResourceLock", func(t *testing.T) {
		require.Equal(t, "lock", storage.StorageResourceLock.String())
	})

	t.Run("StorageResourceECCPackfile", func(t *testing.T) {
		require.Equal(t, "ECC packfile", storage.StorageResourceECCPackfile.String())
	})

	t.Run("StorageResourceECCState", func(t *testing.T) {
		require.Equal(t, "ECC state", storage.StorageResourceECCState.String())
	})

	t.Run("UnknownStorageResource", func(t *testing.T) {
		require.Equal(t, "unknown", storage.StorageResource(999).String())
	})
}

func TestNewConfiguration(t *testing.T) {
	t.Run("ReturnsDefaultConfiguration", func(t *testing.T) {
		before := time.Now()
		cfg := storage.NewConfiguration()
		after := time.Now()

		require.NotNil(t, cfg)
		require.Equal(t, versioning.FromString(storage.VERSION), cfg.Version)

		require.False(t, cfg.Timestamp.IsZero())
		require.False(t, cfg.Timestamp.Before(before))
		require.False(t, cfg.Timestamp.After(after))

		require.NotEqual(t, uuid.Nil, cfg.RepositoryID)

		require.Equal(t, *packfile.NewDefaultConfiguration(), cfg.Packfile)
		require.Equal(t, *chunking.NewDefaultConfiguration(), cfg.Chunking)
		require.Equal(t, *hashing.NewDefaultConfiguration(), cfg.Hashing)

		require.NotNil(t, cfg.Compression)
		require.Equal(t, *compression.NewDefaultConfiguration(), *cfg.Compression)

		expectedEncryption := encryption.NewDefaultConfiguration()
		require.NotNil(t, cfg.Encryption)
		require.Equal(t, expectedEncryption.SubKeyAlgorithm, cfg.Encryption.SubKeyAlgorithm)
		require.Equal(t, expectedEncryption.DataAlgorithm, cfg.Encryption.DataAlgorithm)
		require.Equal(t, expectedEncryption.ChunkSize, cfg.Encryption.ChunkSize)
		require.Equal(t, expectedEncryption.KDFParams.KDF, cfg.Encryption.KDFParams.KDF)
		require.Len(t, cfg.Encryption.KDFParams.Salt, len(expectedEncryption.KDFParams.Salt))
		require.NotNil(t, cfg.Encryption.KDFParams.Argon2idParams)
		require.Nil(t, cfg.Encryption.KDFParams.ScryptParams)
		require.Nil(t, cfg.Encryption.KDFParams.Pbkdf2Params)
		require.Equal(t, expectedEncryption.Canary, cfg.Encryption.Canary)
	})

	t.Run("TwoNewConfigurations_DistinctRepositoryIDs", func(t *testing.T) {
		cfg1 := storage.NewConfiguration()
		cfg2 := storage.NewConfiguration()

		require.NotNil(t, cfg1)
		require.NotNil(t, cfg2)
		require.NotEqual(t, uuid.Nil, cfg1.RepositoryID)
		require.NotEqual(t, uuid.Nil, cfg2.RepositoryID)
		require.NotEqual(t, cfg1.RepositoryID, cfg2.RepositoryID)
	})
}

func TestConfigurationToBytes(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		cfg.Version = versioning.Version(42)

		data, err := cfg.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, data)

		var decoded storage.Configuration
		err = msgpack.Unmarshal(data, &decoded)
		require.NoError(t, err)

		require.True(t, cfg.Timestamp.Equal(decoded.Timestamp))
		require.Equal(t, cfg.RepositoryID, decoded.RepositoryID)
		require.Equal(t, cfg.Packfile, decoded.Packfile)
		require.Equal(t, cfg.Chunking, decoded.Chunking)
		require.Equal(t, cfg.Hashing, decoded.Hashing)
		require.Equal(t, cfg.Compression, decoded.Compression)
		require.Equal(t, cfg.Encryption, decoded.Encryption)
		require.Equal(t, versioning.Version(0), decoded.Version)
	})

	t.Run("ConfigurationsWithoutCompressionAndEncryption", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		cfg.Compression = nil
		cfg.Encryption = nil
		data, err := cfg.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, data)

		var decoded storage.Configuration
		err = msgpack.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Nil(t, decoded.Compression)
		require.Nil(t, decoded.Encryption)
		require.Equal(t, versioning.Version(0), decoded.Version)
	})
}

func TestNewConfigurationFromBytes(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cfg := storage.NewConfiguration()

		data, err := cfg.ToBytes()
		require.NoError(t, err)

		version := versioning.Version(42)
		decoded, err := storage.NewConfigurationFromBytes(version, data)
		require.NoError(t, err)
		require.NotNil(t, decoded)

		require.Equal(t, version, decoded.Version)
		require.True(t, cfg.Timestamp.Equal(decoded.Timestamp))
		require.Equal(t, cfg.RepositoryID, decoded.RepositoryID)
		require.Equal(t, cfg.Packfile, decoded.Packfile)
		require.Equal(t, cfg.Chunking, decoded.Chunking)
		require.Equal(t, cfg.Hashing, decoded.Hashing)
		require.Equal(t, cfg.Compression, decoded.Compression)
		require.Equal(t, cfg.Encryption, decoded.Encryption)
	})

	t.Run("UsesVersionArgument", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		cfg.Version = versioning.Version(999)
		data, err := cfg.ToBytes()
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromBytes(versioning.Version(7), data)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		require.Equal(t, versioning.Version(7), decoded.Version)
	})

	t.Run("Fails_IfDataInvalid", func(t *testing.T) {
		decoded, err := storage.NewConfigurationFromBytes(versioning.Version(1), []byte("not msgpack"))
		require.Nil(t, decoded)
		require.Error(t, err)
	})
}

func TestNewConfigurationFromWrappedBytes(t *testing.T) {
	t.Run("DeserializesWrappedConfiguration", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		payload, err := cfg.ToBytes()
		require.NoError(t, err)

		hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
		require.NotNil(t, hasher)

		wrappedReader, err := storage.Serialize(
			hasher,
			resources.RT_CONFIG,
			versioning.Version(42),
			bytes.NewReader(payload),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		require.Equal(t, versioning.Version(42), decoded.Version)
		require.True(t, cfg.Timestamp.Equal(decoded.Timestamp))
		require.Equal(t, cfg.RepositoryID, decoded.RepositoryID)
		require.Equal(t, cfg.Packfile, decoded.Packfile)
		require.Equal(t, cfg.Chunking, decoded.Chunking)
		require.Equal(t, cfg.Hashing, decoded.Hashing)
		require.Equal(t, cfg.Compression, decoded.Compression)
		require.Equal(t, cfg.Encryption, decoded.Encryption)
	})

	t.Run("UsesVersionFromHeader", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		cfg.Version = versioning.Version(999)
		payload, err := cfg.ToBytes()
		require.NoError(t, err)

		hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
		require.NotNil(t, hasher)

		wrappedReader, err := storage.Serialize(
			hasher,
			resources.RT_CONFIG,
			versioning.Version(7),
			bytes.NewReader(payload),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		require.Equal(t, versioning.Version(7), decoded.Version)
	})

	t.Run("Fails_IfPayloadIsInvalid", func(t *testing.T) {
		wrappedReader, err := storage.Serialize(
			sha256.New(),
			resources.RT_CONFIG,
			versioning.Version(1),
			bytes.NewReader([]byte("not msgpack")),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.Error(t, err)
	})

	t.Run("Fails_IfDataIsTooShort", func(t *testing.T) {
		decoded, err := storage.NewConfigurationFromWrappedBytes([]byte("short"))
		require.Nil(t, decoded)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Fails_IfMagicIsInvalid", func(t *testing.T) {
		payload := []byte{0x80}
		wrappedData := expectedSerializedData(
			t,
			"INVALID!",
			resources.RT_CONFIG,
			versioning.Version(1),
			payload,
		)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.EqualError(t, err, "invalid plakar magic: INVALID!")
	})

	t.Run("FailsIfWrappedResourceTypeIsInvalid", func(t *testing.T) {
		payload := []byte{0x80}
		wrappedData := expectedSerializedData(
			t,
			"_KLOSET_",
			resources.Type(9999),
			versioning.Version(1),
			payload,
		)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.EqualError(t, err, "invalid resource type")
	})

	t.Run("Fails_IfFooterIsTruncated", func(t *testing.T) {
		hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
		require.NotNil(t, hasher)

		wrappedReader, err := storage.Serialize(
			hasher,
			resources.RT_CONFIG,
			versioning.Version(2),
			bytes.NewReader([]byte{}),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		wrappedData = wrappedData[:len(wrappedData)-1]
		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Fails_IfHMACIsInvalid", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		payload, err := cfg.ToBytes()
		require.NoError(t, err)

		wrappedReader, err := storage.Serialize(
			sha256.New(),
			resources.RT_CONFIG,
			versioning.Version(3),
			bytes.NewReader(payload),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		wrappedData[len(wrappedData)-1] ^= 0xff
		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.EqualError(t, err, "hmac mismatch")
	})
}

func TestNewStore(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	store, err := storage.New(ctx, map[string]string{"location": "mock:///test/location"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if loc := store.Origin(); loc != "mock:///test/location" {
		t.Errorf("expected location to be '/test/location', got %v", loc)
	}

	// should return an error as the backend does not exist
	_, err = storage.New(ctx, map[string]string{"location": "unknown:///test/location"})
	if err.Error() != "backend 'unknown' does not exist" {
		t.Fatalf("Expected %s but got %v", "backend 'unknown' does not exist", err)
	}
}

func TestCreateStore(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	config := storage.NewConfiguration()
	serializedConfig, err := config.ToBytes()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	_, err = storage.Create(ctx, map[string]string{"location": "mock:///test/location"}, serializedConfig)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// should return an error as the backend Create will return an error
	_, err = storage.Create(ctx, map[string]string{"location": "mock:///test/location/musterror"}, serializedConfig)
	if err.Error() != "creating error" {
		t.Fatalf("Expected %s but got %v", "opening error", err)
	}

	// should return an error as the backend does not exist
	_, err = storage.Create(ctx, map[string]string{"location": "unknown://dummy"}, serializedConfig)
	if err.Error() != "backend 'unknown' does not exist" {
		t.Fatalf("Expected %s but got %v", "backend 'unknown' does not exist", err)
	}
}

func TestOpenStore(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	store, _, err := storage.Open(ctx, map[string]string{"location": "mock:///test/location"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if loc := store.Origin(); loc != "mock:///test/location" {
		t.Errorf("expected location to be '/test/location', got %v", loc)
	}

	// should return an error as the backend Open will return an error
	_, _, err = storage.Open(ctx, map[string]string{"location": "mock:///test/location/musterror"})
	if err.Error() != "opening error" {
		t.Fatalf("Expected %s but got %v", "opening error", err)
	}

	// should return an error as the backend does not exist
	_, _, err = storage.Open(ctx, map[string]string{"location": "unknown://dummy"})
	if err.Error() != "backend 'unknown' does not exist" {
		t.Fatalf("Expected %s but got %v", "backend 'unknown' does not exist", err)
	}
}

func TestBackends(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	storage.Register("test", 0, func(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
		return &ptesting.MockBackend{}, nil
	})

	expected := []string{"mock", "test"}
	actual := storage.Backends()
	require.Equal(t, expected, actual)
}

func TestNew(t *testing.T) {
	locations := []string{
		"foo",
		"bar",
		"baz",
		"quux",
	}

	for _, name := range locations {
		t.Run(name, func(t *testing.T) {
			ctx := kcontext.NewKContext()
			ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
			ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

			storage.Register(name, 0, func(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
				return ptesting.NewMockBackend(storeConfig), nil
			})

			location := name + ":///test/location"

			store, err := storage.New(ctx, map[string]string{"location": location})
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if loc := store.Origin(); loc != location {
				t.Errorf("expected location to be '%s', got %v", location, loc)
			}
		})
	}

	t.Run("unknown backend", func(t *testing.T) {
		ctx := kcontext.NewKContext()
		ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
		ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

		// storage.Register("unknown", func(location string) storage.Store { return ptesting.NewMockBackend(location) })
		_, err := storage.New(ctx, map[string]string{"location": "unknown://dummy"})
		if err.Error() != "backend 'unknown' does not exist" {
			t.Fatalf("Expected %s but got %v", "backend 'unknown' does not exist", err)
		}
	})

	t.Run("absolute fs path", func(t *testing.T) {
		ctx := kcontext.NewKContext()
		ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
		ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

		// storage.Register("unknown", func(location string) storage.Store { return ptesting.NewMockBackend(location) })
		store, err := storage.New(ctx, map[string]string{"location": "dummy"})
		require.Nil(t, store)
		require.ErrorContains(t, err, "backend 'fs' does not exist")
	})
}
