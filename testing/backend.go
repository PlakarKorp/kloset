package testing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/storage"
)

func init() {
	storage.Register("mock", 0, func(ctx context.Context, proto string, storeConfig map[string]string) (storage.Store, error) {
		return &MockBackend{location: storeConfig["location"], locks: make(map[objects.MAC][]byte), stateMACs: make(map[objects.MAC][]byte), packfileMACs: make(map[objects.MAC][]byte)}, nil
	})
}

type mockedBackendBehavior struct {
	statesMACs    []objects.MAC
	header        any
	packfilesMACs []objects.MAC
	packfile      string
}

var behaviors = map[string]mockedBackendBehavior{
	"default": {
		statesMACs:    nil,
		header:        "blob data",
		packfilesMACs: nil,
		packfile:      `{"test": "data"}`,
	},
	"oneState": {
		statesMACs:    []objects.MAC{{0x01}, {0x02}, {0x03}, {0x04}},
		header:        header.Header{Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Identifier: [32]byte{0x1}, Sources: []header.Source{{VFS: header.VFS{Root: objects.MAC{0x01}}}}},
		packfilesMACs: []objects.MAC{{0x04}, {0x05}, {0x06}},
	},
	"oneSnapshot": {
		statesMACs:    []objects.MAC{{0x01}, {0x02}, {0x03}},
		header:        header.Header{Timestamp: time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC), Identifier: [32]byte{0x1}},
		packfilesMACs: []objects.MAC{{0x01}, {0x04}, {0x05}, {0x06}},
	},
	"brokenState": {
		statesMACs:    nil,
		header:        nil,
		packfilesMACs: nil,
	},
	"brokenGetState": {
		statesMACs:    nil,
		header:        nil,
		packfilesMACs: nil,
	},
	"nopackfile": {
		statesMACs:    []objects.MAC{{0x01}, {0x02}, {0x03}},
		header:        nil,
		packfilesMACs: nil,
	},
}

// MockBackend implements the Backend interface for testing purposes
type MockBackend struct {
	configuration []byte
	location      string
	locks         map[objects.MAC][]byte
	stateMACs     map[objects.MAC][]byte
	packfileMACs  map[objects.MAC][]byte

	packfileMutex sync.Mutex
	// used to trigger different behaviors during tests
	behavior string
}

func NewMockBackend(storeConfig map[string]string) *MockBackend {
	return &MockBackend{location: storeConfig["location"], locks: make(map[objects.MAC][]byte), stateMACs: make(map[objects.MAC][]byte), packfileMACs: make(map[objects.MAC][]byte)}
}

func (mb *MockBackend) Create(ctx context.Context, configuration []byte) error {
	if strings.Contains(mb.location, "musterror") {
		return errors.New("creating error")
	}
	mb.configuration = configuration

	mb.behavior = "default"

	u, err := url.Parse(mb.location)
	if err != nil {
		return err
	}
	m, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return err
	}
	if m.Get("behavior") != "" {
		mb.behavior = m.Get("behavior")
	}
	return nil
}

func (mb *MockBackend) Open(ctx context.Context) ([]byte, error) {
	if strings.Contains(mb.location, "musterror") {
		return nil, errors.New("opening error")
	}
	return mb.configuration, nil
}

func (mb *MockBackend) Location(ctx context.Context) (string, error) {
	return mb.location, nil
}

func (mb *MockBackend) Mode(ctx context.Context) (storage.Mode, error) {
	return storage.ModeRead | storage.ModeWrite, nil
}

func (mb *MockBackend) Size(ctx context.Context) (int64, error) {
	return 0, nil
}

func (mb *MockBackend) GetStates(ctx context.Context) ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for MAC := range mb.stateMACs {
		ret = append(ret, MAC)
	}
	return ret, nil
}

func (mb *MockBackend) PutState(ctx context.Context, MAC objects.MAC, rd io.Reader) (int64, error) {
	var buffer bytes.Buffer
	io.Copy(&buffer, rd)
	mb.stateMACs[MAC] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) GetState(ctx context.Context, MAC objects.MAC) (io.ReadCloser, error) {
	var buffer bytes.Buffer
	buffer.Write(mb.stateMACs[MAC])
	return io.NopCloser(&buffer), nil
}

func (mb *MockBackend) DeleteState(ctx context.Context, MAC objects.MAC) error {
	delete(mb.stateMACs, MAC)
	return nil
}

func (mb *MockBackend) GetPackfiles(ctx context.Context) ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for MAC := range mb.packfileMACs {
		ret = append(ret, MAC)
	}
	return ret, nil
}

func (mb *MockBackend) PutPackfile(ctx context.Context, MAC objects.MAC, rd io.Reader) (int64, error) {
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()

	var buffer bytes.Buffer
	io.Copy(&buffer, rd)
	mb.packfileMACs[MAC] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) GetPackfile(ctx context.Context, MAC objects.MAC) (io.ReadCloser, error) {
	buffer := bytes.NewReader(mb.packfileMACs[MAC])
	return io.NopCloser(buffer), nil
}

func (mb *MockBackend) GetPackfileBlob(ctx context.Context, MAC objects.MAC, offset uint64, length uint32) (io.ReadCloser, error) {
	buffer := bytes.NewReader(mb.packfileMACs[MAC])
	return io.NopCloser(io.NewSectionReader(buffer, int64(offset), int64(length))), nil
}

func (mb *MockBackend) DeletePackfile(ctx context.Context, MAC objects.MAC) error {
	delete(mb.packfileMACs, MAC)
	return nil
}

func (mb *MockBackend) Close(ctx context.Context) error {
	return nil
}

/* Locks */
func (mb *MockBackend) GetLocks(ctx context.Context) ([]objects.MAC, error) {
	locks := make([]objects.MAC, 0)
	for lock := range mb.locks {
		locks = append(locks, lock)
	}
	return locks, nil
}

func (mb *MockBackend) PutLock(ctx context.Context, lockID objects.MAC, rd io.Reader) (int64, error) {
	var buffer bytes.Buffer
	io.Copy(&buffer, rd)
	mb.locks[lockID] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) GetLock(ctx context.Context, lockID objects.MAC) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(mb.locks[lockID])), nil
}

func (mb *MockBackend) DeleteLock(ctx context.Context, lockID objects.MAC) error {
	delete(mb.locks, lockID)
	return nil
}
