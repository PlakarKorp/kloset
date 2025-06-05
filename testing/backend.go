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
	storage.Register(func(ctx context.Context, opts *storage.StoreOptions, proto string, storeConfig map[string]string) (storage.Store, error) {
		return &MockBackend{location: storeConfig["location"], locks: make(map[objects.MAC][]byte), stateMACs: make(map[objects.MAC][]byte), packfileMACs: make(map[objects.MAC][]byte)}, nil
	}, "mock")
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

func (mb *MockBackend) Location() string {
	return mb.location
}

func (mb *MockBackend) Mode() storage.Mode {
	return storage.ModeRead | storage.ModeWrite
}

func (mb *MockBackend) Size() int64 {
	return 0
}

func (mb *MockBackend) GetStates() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for MAC := range mb.stateMACs {
		ret = append(ret, MAC)
	}
	return ret, nil
}

func (mb *MockBackend) PutState(MAC objects.MAC, rd io.Reader) (int64, error) {
	var buffer bytes.Buffer
	io.Copy(&buffer, rd)
	mb.stateMACs[MAC] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) GetState(MAC objects.MAC) (io.Reader, error) {
	var buffer bytes.Buffer
	buffer.Write(mb.stateMACs[MAC])
	return &buffer, nil
}

func (mb *MockBackend) DeleteState(MAC objects.MAC) error {
	delete(mb.stateMACs, MAC)
	return nil
}

func (mb *MockBackend) GetPackfiles() ([]objects.MAC, error) {
	ret := make([]objects.MAC, 0)
	for MAC := range mb.packfileMACs {
		ret = append(ret, MAC)
	}
	return ret, nil
}

func (mb *MockBackend) PutPackfile(MAC objects.MAC, rd io.Reader) (int64, error) {
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()

	var buffer bytes.Buffer
	io.Copy(&buffer, rd)
	mb.packfileMACs[MAC] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) GetPackfile(MAC objects.MAC) (io.Reader, error) {
	buffer := bytes.NewReader(mb.packfileMACs[MAC])
	return buffer, nil
}

func (mb *MockBackend) GetPackfileBlob(MAC objects.MAC, offset uint64, length uint32) (io.Reader, error) {
	buffer := bytes.NewReader(mb.packfileMACs[MAC])
	return io.NewSectionReader(buffer, int64(offset), int64(length)), nil
}

func (mb *MockBackend) DeletePackfile(MAC objects.MAC) error {
	delete(mb.packfileMACs, MAC)
	return nil
}

func (mb *MockBackend) Close() error {
	return nil
}

/* Locks */
func (mb *MockBackend) GetLocks() ([]objects.MAC, error) {
	locks := make([]objects.MAC, 0)
	for lock := range mb.locks {
		locks = append(locks, lock)
	}
	return locks, nil
}

func (mb *MockBackend) PutLock(lockID objects.MAC, rd io.Reader) (int64, error) {
	var buffer bytes.Buffer
	io.Copy(&buffer, rd)
	mb.locks[lockID] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) GetLock(lockID objects.MAC) (io.Reader, error) {
	return bytes.NewReader(mb.locks[lockID]), nil
}

func (mb *MockBackend) DeleteLock(lockID objects.MAC) error {
	delete(mb.locks, lockID)
	return nil
}
