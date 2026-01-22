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

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/header"
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

func (mb *MockBackend) Ping(ctx context.Context) error {
	return nil
}

func (mb *MockBackend) Type() string {
	return "mockbackend"
}

func (mb *MockBackend) Root() string {
	return mb.location
}

func (mb *MockBackend) Origin() string {
	return mb.location
}

func (mb *MockBackend) Mode() storage.Mode {
	return storage.ModeRead | storage.ModeWrite
}

func (mb *MockBackend) Flags() location.Flags {
	return 0
}

func (mb *MockBackend) Size(ctx context.Context) (int64, error) {
	return 0, nil
}

func (mb *MockBackend) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	switch res {
	case storage.StorageResourcePackfile:
		ret := make([]objects.MAC, 0)
		for MAC := range mb.packfileMACs {
			ret = append(ret, MAC)
		}
		return ret, nil
	case storage.StorageResourceStatefile:
		ret := make([]objects.MAC, 0)
		for MAC := range mb.stateMACs {
			ret = append(ret, MAC)
		}
		return ret, nil
	case storage.StorageResourceLockfile:
		locks := make([]objects.MAC, 0)
		for lock := range mb.locks {
			locks = append(locks, lock)
		}
		return locks, nil
	}

	return nil, errors.ErrUnsupported
}

func (mb *MockBackend) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	switch res {
	case storage.StorageResourcePackfile:
		mb.packfileMutex.Lock()
		defer mb.packfileMutex.Unlock()

		var buffer bytes.Buffer
		io.Copy(&buffer, rd)
		mb.packfileMACs[mac] = buffer.Bytes()
		return int64(buffer.Len()), nil
	case storage.StorageResourceStatefile:
		var buffer bytes.Buffer
		io.Copy(&buffer, rd)
		mb.stateMACs[mac] = buffer.Bytes()
		return int64(buffer.Len()), nil
	case storage.StorageResourceLockfile:
		var buffer bytes.Buffer
		io.Copy(&buffer, rd)
		mb.locks[mac] = buffer.Bytes()
		return int64(buffer.Len()), nil
	}

	return -1, errors.ErrUnsupported
}

func (mb *MockBackend) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	switch res {
	case storage.StorageResourcePackfile:
		if rg == nil {
			buffer := bytes.NewReader(mb.packfileMACs[mac])
			return io.NopCloser(buffer), nil
		} else {
			buffer := bytes.NewReader(mb.packfileMACs[mac])
			return io.NopCloser(io.NewSectionReader(buffer, int64(rg.Offset), int64(rg.Length))), nil
		}

	case storage.StorageResourceStatefile:
		var buffer bytes.Buffer
		buffer.Write(mb.stateMACs[mac])
		return io.NopCloser(&buffer), nil
	case storage.StorageResourceLockfile:
		return io.NopCloser(bytes.NewReader(mb.locks[mac])), nil
	}

	return nil, errors.ErrUnsupported
}

func (mb *MockBackend) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	switch res {
	case storage.StorageResourcePackfile:
		delete(mb.packfileMACs, mac)
	case storage.StorageResourceStatefile:
		delete(mb.stateMACs, mac)
	case storage.StorageResourceLockfile:
		delete(mb.locks, mac)
	default:
		return errors.ErrUnsupported
	}

	return nil
}

func (mb *MockBackend) Close(ctx context.Context) error {
	return nil
}
