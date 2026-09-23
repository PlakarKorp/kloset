package repository

import (
	"context"
	"io"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/throttle"
)

// Wraps a real Store implementation and adds throttling to all i/o
type ThrottledStore struct {
	s  storage.Store
	th *throttle.Throttler
}

func NewThrottledStore(s storage.Store, th *throttle.Throttler) *ThrottledStore {
	return &ThrottledStore{s, th}
}

func (ts *ThrottledStore) Create(ctx context.Context, data []byte) error {
	return ts.s.Create(ctx, data)
}

func (ts *ThrottledStore) Open(ctx context.Context) ([]byte, error) {
	return ts.s.Open(ctx)
}

func (ts *ThrottledStore) Ping(ctx context.Context) error {
	return ts.s.Ping(ctx)
}

func (ts *ThrottledStore) Origin() string {
	return ts.s.Origin()
}

func (ts *ThrottledStore) Type() string {
	return ts.s.Type()
}

func (ts *ThrottledStore) Root() string {
	return ts.s.Root()
}

func (ts *ThrottledStore) Flags() location.Flags {
	return ts.s.Flags()
}

func (ts *ThrottledStore) Mode(ctx context.Context) (storage.Mode, error) {
	return ts.s.Mode(ctx)
}

func (ts *ThrottledStore) Size(ctx context.Context) (int64, error) {
	return ts.s.Size(ctx)
}

func (ts *ThrottledStore) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	return ts.s.List(ctx, res)
}

func (ts *ThrottledStore) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	if res == storage.StorageResourceLock {
		return ts.s.Put(ctx, res, mac, rd)
	}

	throttledRd := ts.th.WriteFromReader(ctx, rd)
	return ts.s.Put(ctx, res, mac, throttledRd)
}

func (ts *ThrottledStore) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	if res == storage.StorageResourceLock {
		return ts.s.Get(ctx, res, mac, rg)
	}

	rc, err := ts.s.Get(ctx, res, mac, rg)
	if err != nil {
		return nil, err
	}

	return ts.th.ReadCloser(ctx, rc), nil
}

func (ts *ThrottledStore) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	return ts.s.Delete(ctx, res, mac)
}

func (ts *ThrottledStore) Close(ctx context.Context) error {
	return ts.s.Close(ctx)
}
