package testing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"os"
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
		return NewMockBackend(storeConfig), nil
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
	configuration   []byte
	location        string
	locks           map[objects.MAC][]byte
	stateMACs       map[objects.MAC][]byte
	packfileMACs    map[objects.MAC][]byte
	eccPackfileMACs map[objects.MAC][]byte
	eccStateMACs    map[objects.MAC][]byte

	packfileMutex sync.Mutex
	// used to trigger different behaviors during tests
	behavior string
}

func NewMockBackend(storeConfig map[string]string) *MockBackend {
	return &MockBackend{
		location:        storeConfig["location"],
		locks:           make(map[objects.MAC][]byte),
		stateMACs:       make(map[objects.MAC][]byte),
		packfileMACs:    make(map[objects.MAC][]byte),
		eccPackfileMACs: make(map[objects.MAC][]byte),
		eccStateMACs:    make(map[objects.MAC][]byte),
	}
}

// resourceMap returns the in-memory map backing a given storage resource, or
// nil if the resource is not backed by a simple MAC->bytes map.
func (mb *MockBackend) resourceMap(res storage.StorageResource) map[objects.MAC][]byte {
	switch res {
	case storage.StorageResourcePackfile:
		return mb.packfileMACs
	case storage.StorageResourceState:
		return mb.stateMACs
	case storage.StorageResourceLock:
		return mb.locks
	case storage.StorageResourceECCPackfile:
		return mb.eccPackfileMACs
	case storage.StorageResourceECCState:
		return mb.eccStateMACs
	}
	return nil
}

// Corrupt flips the byte at the given offset of a stored object, simulating
// on-disk bit rot. It is a test helper only.
func (mb *MockBackend) Corrupt(res storage.StorageResource, mac objects.MAC, offset int) error {
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()
	m := mb.resourceMap(res)
	if m == nil {
		return errors.ErrUnsupported
	}
	buf, ok := m[mac]
	if !ok || offset < 0 || offset >= len(buf) {
		return errors.New("mock: cannot corrupt: missing object or offset out of range")
	}
	buf[offset] ^= 0xFF
	return nil
}

// CorruptRange flips count bytes starting at offset of a stored object.
func (mb *MockBackend) CorruptRange(res storage.StorageResource, mac objects.MAC, offset, count int) error {
	for i := 0; i < count; i++ {
		if err := mb.Corrupt(res, mac, offset+i); err != nil {
			return err
		}
	}
	return nil
}

// Has reports whether an object exists for the given resource and MAC.
func (mb *MockBackend) Has(res storage.StorageResource, mac objects.MAC) bool {
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()
	m := mb.resourceMap(res)
	if m == nil {
		return false
	}
	_, ok := m[mac]
	return ok
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

func (mb *MockBackend) Mode(context.Context) (storage.Mode, error) {
	return storage.ModeRead | storage.ModeWrite, nil
}

func (mb *MockBackend) Flags() location.Flags {
	return 0
}

func (mb *MockBackend) Size(ctx context.Context) (int64, error) {
	return 0, nil
}

func (mb *MockBackend) List(ctx context.Context, res storage.StorageResource) ([]objects.MAC, error) {
	m := mb.resourceMap(res)
	if m == nil {
		return nil, errors.ErrUnsupported
	}
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()
	ret := make([]objects.MAC, 0, len(m))
	for MAC := range m {
		ret = append(ret, MAC)
	}
	return ret, nil
}

func (mb *MockBackend) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	m := mb.resourceMap(res)
	if m == nil {
		return -1, errors.ErrUnsupported
	}
	var buffer bytes.Buffer
	// NB: copy errors are intentionally ignored to mirror the historical mock
	// behavior (some writers, e.g. ptar pipes, close early in tests).
	io.Copy(&buffer, rd)
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()
	m[mac] = buffer.Bytes()
	return int64(buffer.Len()), nil
}

func (mb *MockBackend) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	m := mb.resourceMap(res)
	if m == nil {
		return nil, errors.ErrUnsupported
	}
	mb.packfileMutex.Lock()
	data, ok := m[mac]
	mb.packfileMutex.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	if rg == nil {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
	return io.NopCloser(io.NewSectionReader(bytes.NewReader(data), int64(rg.Offset), int64(rg.Length))), nil
}

func (mb *MockBackend) Delete(ctx context.Context, res storage.StorageResource, mac objects.MAC) error {
	m := mb.resourceMap(res)
	if m == nil {
		return errors.ErrUnsupported
	}
	mb.packfileMutex.Lock()
	defer mb.packfileMutex.Unlock()
	delete(m, mac)
	return nil
}

func (mb *MockBackend) Close(ctx context.Context) error {
	return nil
}
