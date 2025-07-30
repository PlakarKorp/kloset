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

func (s *store) Size() int64 {
	return 0
}

func (s *store) GetStates() ([]objects.MAC, error) {
	var all []objects.MAC
	for k := range s.states {
		all = append(all, k)
	}
	return all, nil
}

func (s *store) PutState(mac objects.MAC, rd io.Reader) (int64, error) {
	data, err := io.ReadAll(rd)
	if err != nil {
		return 0, err
	}

	s.states[mac] = data
	return int64(len(data)), nil
}

func (s *store) GetState(mac objects.MAC) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.states[mac])), nil
}

func (s *store) DeleteState(mac objects.MAC) error {
	panic("!!!")
}

func (s *store) GetPackfiles() ([]objects.MAC, error) {
	return nil, unsupported
}

func (s *store) PutPackfile(mac objects.MAC, rd io.Reader) (int64, error) {
	return 0, unsupported
}

func (s *store) GetPackfile(mac objects.MAC) (io.ReadCloser, error) {
	return nil, unsupported
}

func (s *store) GetPackfileBlob(mac objects.MAC, offset uint64, length uint32) (io.ReadCloser, error) {
	return nil, unsupported
}

func (s *store) DeletePackfile(mac objects.MAC) error {
	return unsupported
}

// pretend to be lockless

func (s *store) GetLocks() ([]objects.MAC, error) {
	return nil, nil
}

func (s *store) PutLock(lockID objects.MAC, rd io.Reader) (int64, error) {
	return 0, nil
}

func (s *store) GetLock(lockID objects.MAC) (io.ReadCloser, error) {
	return nil, nil
}

func (s *store) DeleteLock(lockID objects.MAC) error {
	return nil
}

func (s *store) Close() error {
	return nil
}
