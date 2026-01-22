package state

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
)

func init() {
	storage.Register("fake+state", 0, func(ctx context.Context, name string, conf map[string]string) (storage.Store, error) {
		return &store{
			states: make(map[objects.MAC][]byte),
		}, nil
	})
}

type store struct {
	states map[objects.MAC][]byte
}

func (s *store) Create(ctx context.Context, config []byte) error {
	return nil
}

func (s *store) Open(ctx context.Context) ([]byte, error) {
	return nil, unsupported
}

func (s *store) Ping(ctx context.Context) error {
	return nil
}

func (s *store) Type() string {
	return "fakestatebackend"
}

func (s *store) Root() string {
	return ""
}

func (s *store) Mode() storage.Mode {
	return storage.ModeRead | storage.ModeWrite
}

func (s *store) Flags() location.Flags {
	return 0
}

func (s *store) Origin() string {
	return "fake"
}

func (s *store) Size(ctx context.Context) (int64, error) {
	return 0, nil
}

func (s *store) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	switch res {
	case storage.StorageResourceStatefile:
		var all []objects.MAC
		for k := range s.states {
			all = append(all, k)
		}
		return all, nil
	}

	return nil, errors.ErrUnsupported
}

func (s *store) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	switch res {
	case storage.StorageResourceStatefile:
		data, err := io.ReadAll(rd)
		if err != nil {
			return 0, err
		}

		s.states[mac] = data
		return int64(len(data)), nil
	}

	return -1, errors.ErrUnsupported
}

func (s *store) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	switch res {
	case storage.StorageResourceStatefile:
		return io.NopCloser(bytes.NewReader(s.states[mac])), nil
	}

	return nil, errors.ErrUnsupported
}

func (s *store) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	switch res {
	case storage.StorageResourceStatefile:
		panic("!!!")
	}

	return errors.ErrUnsupported
}

func (s *store) Close(ctx context.Context) error {
	return nil
}
