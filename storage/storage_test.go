package storage_test

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/PlakarKorp/kloset/storage"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestNewStore(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.SetLogger(logging.NewLogger(os.Stdout, os.Stderr))
	ctx.MaxConcurrency = runtime.NumCPU()*8 + 1

	store, err := storage.New(ctx, map[string]string{"location": "mock:///test/location"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if store.Location() != "mock:///test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.Location())
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

	if store.Location() != "mock:///test/location" {
		t.Errorf("expected location to be '/test/location', got %v", store.Location())
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

	storage.Register(func(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
		return &ptesting.MockBackend{}, nil
	}, 0, "test")

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

			storage.Register(func(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
				return ptesting.NewMockBackend(storeConfig), nil
			}, 0, name)

			location := name + ":///test/location"

			store, err := storage.New(ctx, map[string]string{"location": location})
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if store.Location() != location {
				t.Errorf("expected location to be '%s', got %v", location, store.Location())
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
