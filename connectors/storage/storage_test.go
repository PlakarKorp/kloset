package storage_test

import (
	"context"
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
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
