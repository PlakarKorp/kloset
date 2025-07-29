package state

import (
	"bytes"
	"context"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/storage"
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

func (s *store) Location() string {
	return "fake"
}

func (s *store) Mode() storage.Mode {
	return storage.ModeWrite
}

func (s *store) Size(ctx context.Context) int64 {
	return 0
}

func (s *store) GetStates(ctx context.Context) ([]objects.MAC, error) {
	var all []objects.MAC
	for k := range s.states {
		all = append(all, k)
	}
	return all, nil
}

func (s *store) PutState(ctx context.Context, mac objects.MAC, rd io.Reader) (int64, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}

	s.states[mac] = data
	return int64(len(data)), nil
}

func (s *store) GetState(ctx context.Context, mac objects.MAC) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.states[mac])), nil
}

func (s *store) DeleteState(ctx context.Context, mac objects.MAC) error {
	panic("!!!")
}

func (s *store) GetPackfiles(ctx context.Context) ([]objects.MAC, error) {
	return nil, unsupported
}

func (s *store) PutPackfile(ctx context.Context, mac objects.MAC, rd io.Reader) (int64, error) {
	return 0, unsupported
}

func (s *store) GetPackfile(ctx context.Context, mac objects.MAC) (io.ReadCloser, error) {
	return nil, unsupported
}

func (s *store) GetPackfileBlob(ctx context.Context, mac objects.MAC, offset uint64, length uint32) (io.ReadCloser, error) {
	return nil, unsupported
}

func (s *store) DeletePackfile(ctx context.Context, mac objects.MAC) error {
	return unsupported
}

// pretend to be lockless

func (s *store) GetLocks(ctx context.Context) ([]objects.MAC, error) {
	return nil, nil
}

func (s *store) PutLock(ctx context.Context, lockID objects.MAC, rd io.Reader) (int64, error) {
	return 0, nil
}

func (s *store) GetLock(ctx context.Context, lockID objects.MAC) (io.ReadCloser, error) {
	return nil, nil
}

func (s *store) DeleteLock(ctx context.Context, lockID objects.MAC) error {
	return nil
}

func (s *store) Close(ctx context.Context) error {
	return nil
}
