package state

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

// newTestStore constructs the in-memory fake state store registered by init().
func newTestStore(t *testing.T) storage.Store {
	t.Helper()
	ctx := kcontext.NewKContext()
	t.Cleanup(func() { ctx.Close() })
	st, err := storage.New(ctx, map[string]string{"location": "fake+state://test"})
	require.NoError(t, err)
	return st
}

// TestStoreSimpleAccessors covers Type, Root, Origin, Size, Mode, Flags, Ping,
// Create — all of which are trivial implementations on the fake state store.
func TestStoreSimpleAccessors(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	require.NoError(t, s.Create(ctx, nil))
	require.NoError(t, s.Ping(ctx))
	require.Equal(t, "fakestatebackend", s.Type())
	require.Equal(t, "", s.Root())
	require.Equal(t, "fake", s.Origin())

	mode, err := s.Mode(ctx)
	require.NoError(t, err)
	require.NotZero(t, mode)

	size, err := s.Size(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)

	flags := s.Flags()
	require.Equal(t, uint32(0), uint32(flags))

	require.NoError(t, s.Close(ctx))
}

// TestStoreOpen exercises the Open method which returns ErrUnsupported.
func TestStoreOpen(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Open(context.Background())
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

// TestStorePutGetListState round-trips a state through Put / Get / List.
func TestStorePutGetListState(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	mac := objects.MAC{0xAA}
	payload := []byte("state payload")

	n, err := s.Put(ctx, storage.StorageResourceState, 0, mac, bytes.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), n)

	rc, err := s.Get(ctx, storage.StorageResourceState, 0, mac, nil)
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, payload, got)

	var macs []objects.MAC
	err = nil
	for m, e := range s.List(ctx, storage.StorageResourceState, 0) {
		if e != nil {
			err = e
			break
		}
		macs = append(macs, m.MAC)
	}

	require.NoError(t, err)
	require.Contains(t, macs, mac)
}

// TestStorePutUnsupported exercises the unsupported-resource branch of Put.
func TestStorePutUnsupported(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Put(context.Background(), storage.StorageResourcePackfile, 0, objects.MAC{}, bytes.NewReader(nil))
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

// TestStoreGetUnsupported exercises the unsupported-resource branch of Get.
func TestStoreGetUnsupported(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get(context.Background(), storage.StorageResourcePackfile, 0, objects.MAC{}, nil)
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

// TestStoreListUnsupported exercises the unsupported-resource branch of List.
func TestStoreListUnsupported(t *testing.T) {
	s := newTestStore(t)

	for _, err := range s.List(context.Background(), 0, integration.StorageFlag(integration.StorageResourcePackfile)) {
		require.ErrorIs(t, err, errors.ErrUnsupported)
		break
	}
}

// TestStoreDeleteUnsupported exercises the unsupported-resource branch of
// Delete. (The state branch panics by design, so we don't test it.)
func TestStoreDeleteUnsupported(t *testing.T) {
	s := newTestStore(t)
	err := s.Delete(context.Background(), storage.StorageResourcePackfile, 0, objects.MAC{})
	require.ErrorIs(t, err, errors.ErrUnsupported)
}

// errReader returns an error on every Read.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read fail") }

// TestStorePutStateReadError exercises the io.ReadAll error branch of Put.
func TestStorePutStateReadError(t *testing.T) {
	s := newTestStore(t)
	ctx := kcontext.NewKContext()
	t.Cleanup(func() { ctx.Close() })

	_, err := s.Put(ctx, storage.StorageResourceState, 0, objects.MAC{0xEE}, errReader{})
	require.Error(t, err)
}

// TestStoreDeleteStatePanics confirms the documented panic on state deletion.
func TestStoreDeleteStatePanics(t *testing.T) {
	s := newTestStore(t)
	require.Panics(t, func() {
		_ = s.Delete(context.Background(), storage.StorageResourceState, 0, objects.MAC{})
	})
}
