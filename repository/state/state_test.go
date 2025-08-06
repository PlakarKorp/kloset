package state

import (
	"bytes"
	"fmt"
	"io"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// mockStateCache implements caching.StateCache for testing
type mockStateCache struct {
	states         map[objects.MAC][]byte
	deltas         map[string][]byte // key: "type:blob:packfile"
	deleteds       map[string][]byte // key: "type:blob"
	packfiles      map[objects.MAC][]byte
	configurations map[string][]byte
}

func newMockStateCache() *mockStateCache {
	return &mockStateCache{
		states:         make(map[objects.MAC][]byte),
		deltas:         make(map[string][]byte),
		deleteds:       make(map[string][]byte),
		packfiles:      make(map[objects.MAC][]byte),
		configurations: make(map[string][]byte),
	}
}

func (m *mockStateCache) PutState(stateID objects.MAC, data []byte) error {
	m.states[stateID] = data
	return nil
}

func (m *mockStateCache) HasState(stateID objects.MAC) (bool, error) {
	_, exists := m.states[stateID]
	return exists, nil
}

func (m *mockStateCache) GetState(stateID objects.MAC) ([]byte, error) {
	return m.states[stateID], nil
}

func (m *mockStateCache) DelState(stateID objects.MAC) error {
	delete(m.states, stateID)
	return nil
}

func (m *mockStateCache) GetStates() (map[objects.MAC][]byte, error) {
	return m.states, nil
}

func (m *mockStateCache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	key := fmt.Sprintf("%d:%x:%x", blobType, blobCsum, packfile)
	m.deltas[key] = data
	return nil
}

func (m *mockStateCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for key, data := range m.deltas {
			parts := strings.Split(key, ":")
			if len(parts) == 3 {
				// For testing purposes, we'll just yield all deltas
				// since the MAC parsing is complex and not essential for these tests
				var mac objects.MAC
				if !yield(mac, data) {
					return
				}
			}
		}
	}
}

func (m *mockStateCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for key, data := range m.deltas {
			parts := strings.Split(key, ":")
			if len(parts) == 3 {
				if parts[0] == fmt.Sprintf("%d", blobType) {
					var mac objects.MAC
					if !yield(mac, data) {
						return
					}
				}
			}
		}
	}
}

func (m *mockStateCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for key, data := range m.deltas {
			parts := strings.Split(key, ":")
			if len(parts) == 3 {
				var mac objects.MAC
				if !yield(mac, data) {
					return
				}
			}
		}
	}
}

func (m *mockStateCache) DelDelta(blobType resources.Type, blobCsum objects.MAC, packfileMAC objects.MAC) error {
	key := fmt.Sprintf("%d:%x:%x", blobType, blobCsum, packfileMAC)
	delete(m.deltas, key)
	return nil
}

func (m *mockStateCache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	key := fmt.Sprintf("%d:%x", blobType, blobCsum)
	m.deleteds[key] = data
	return nil
}

func (m *mockStateCache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	key := fmt.Sprintf("%d:%x", blobType, blobCsum)
	_, exists := m.deleteds[key]
	return exists, nil
}

func (m *mockStateCache) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	key := fmt.Sprintf("%d:%x", blobType, blobCsum)
	delete(m.deleteds, key)
	return nil
}

func (m *mockStateCache) GetDeletedsByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for key, data := range m.deleteds {
			parts := strings.Split(key, ":")
			if len(parts) == 2 {
				if parts[0] == fmt.Sprintf("%d", blobType) {
					var mac objects.MAC
					copy(mac[:], []byte(parts[1]))
					if !yield(mac, data) {
						return
					}
				}
			}
		}
	}
}

func (m *mockStateCache) GetDeleteds() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for key, data := range m.deleteds {
			parts := strings.Split(key, ":")
			if len(parts) == 2 {
				var mac objects.MAC
				copy(mac[:], []byte(parts[1]))
				if !yield(mac, data) {
					return
				}
			}
		}
	}
}

func (m *mockStateCache) PutPackfile(packfile objects.MAC, data []byte) error {
	m.packfiles[packfile] = data
	return nil
}

func (m *mockStateCache) DelPackfile(packfile objects.MAC) error {
	delete(m.packfiles, packfile)
	return nil
}

func (m *mockStateCache) HasPackfile(packfile objects.MAC) (bool, error) {
	_, exists := m.packfiles[packfile]
	return exists, nil
}

func (m *mockStateCache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for mac, data := range m.packfiles {
			if !yield(mac, data) {
				return
			}
		}
	}
}

func (m *mockStateCache) PutConfiguration(key string, data []byte) error {
	m.configurations[key] = data
	return nil
}

func (m *mockStateCache) GetConfiguration(key string) ([]byte, error) {
	return m.configurations[key], nil
}

func (m *mockStateCache) GetConfigurations() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, data := range m.configurations {
			if !yield(data) {
				return
			}
		}
	}
}

func TestNewLocalState(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	require.NotNil(t, state)
	require.Equal(t, versioning.FromString(VERSION), state.Metadata.Version)
	require.NotZero(t, state.Metadata.Timestamp)
	require.NotNil(t, state.configuration)
	require.Equal(t, cache, state.cache)
}

func TestFromStream(t *testing.T) {
	cache := newMockStateCache()

	// Create a test state with some data
	originalState := NewLocalState(cache)
	originalState.Metadata.Serial = uuid.New()

	// Add some test data
	deltaEntry := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{1, 2, 3, 4},
		Location: Location{
			Packfile: objects.MAC{5, 6, 7, 8},
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x1234,
	}
	originalState.PutDelta(deltaEntry)

	// Serialize to stream
	var buf bytes.Buffer
	err := originalState.SerializeToStream(&buf)
	require.NoError(t, err)

	// Deserialize from stream
	deserializedState, err := FromStream(&buf, cache)
	require.NoError(t, err)
	require.NotNil(t, deserializedState)

	// Verify metadata
	require.Equal(t, originalState.Metadata.Version, deserializedState.Metadata.Version)
	require.Equal(t, originalState.Metadata.Serial, deserializedState.Metadata.Serial)
}

func TestDerive(t *testing.T) {
	cache1 := newMockStateCache()
	cache2 := newMockStateCache()

	originalState := NewLocalState(cache1)
	originalState.Metadata.Serial = uuid.New()

	derivedState := originalState.Derive(cache2)

	require.NotNil(t, derivedState)
	require.Equal(t, originalState.Metadata.Serial, derivedState.Metadata.Serial)
	require.Equal(t, cache2, derivedState.cache)
	// The caches should be different instances
	require.NotEqual(t, fmt.Sprintf("%p", originalState.cache), fmt.Sprintf("%p", derivedState.cache))
}

func TestUpdateSerialOr(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Test with no existing states
	testSerial := uuid.New()
	err := state.UpdateSerialOr(testSerial)
	require.NoError(t, err)
	require.Equal(t, testSerial, state.Metadata.Serial)

	// Test with existing states
	existingSerial := uuid.New()
	existingMetadata := Metadata{
		Version:   versioning.FromString(VERSION),
		Timestamp: time.Now().Add(-time.Hour), // Older timestamp
		Serial:    existingSerial,
	}
	existingData, err := existingMetadata.ToBytes()
	require.NoError(t, err)

	cache.PutState(objects.MAC{1}, existingData)

	newSerial := uuid.New()
	err = state.UpdateSerialOr(newSerial)
	require.NoError(t, err)
	require.Equal(t, existingSerial, state.Metadata.Serial) // Should use existing serial
}

func TestMergeState(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Test merging non-existent state
	stateID := objects.MAC{1, 2, 3, 4}

	// Create test data for merging
	testMetadata := Metadata{
		Version:   versioning.FromString(VERSION),
		Timestamp: time.Now(),
		Serial:    uuid.New(),
	}

	// Add a delta entry to the stream
	deltaEntry := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{5, 6, 7, 8},
		Location: Location{
			Packfile: objects.MAC{9, 10, 11, 12},
			Offset:   2000,
			Length:   1000,
		},
		Flags: 0x5678,
	}

	// Serialize the test state
	testState := NewLocalState(cache)
	testState.Metadata = testMetadata
	testState.PutDelta(deltaEntry)

	var buf bytes.Buffer
	err := testState.SerializeToStream(&buf)
	require.NoError(t, err)

	// Merge the state
	err = state.MergeState(stateID, &buf)
	require.NoError(t, err)

	// Verify the state was merged
	hasState, err := state.HasState(stateID)
	require.NoError(t, err)
	require.True(t, hasState)
}

func TestPutState(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)
	state.Metadata.Serial = uuid.New()

	stateID := objects.MAC{1, 2, 3, 4}
	err := state.PutState(stateID)
	require.NoError(t, err)

	// Verify state was stored
	hasState, err := state.HasState(stateID)
	require.NoError(t, err)
	require.True(t, hasState)
}

func TestSerializeToStream(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)
	state.Metadata.Serial = uuid.New()

	// Add test data
	deltaEntry := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{1, 2, 3, 4},
		Location: Location{
			Packfile: objects.MAC{5, 6, 7, 8},
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x1234,
	}
	state.PutDelta(deltaEntry)

	deletedEntry := &DeletedEntry{
		Type: resources.RT_OBJECT,
		Blob: objects.MAC{9, 10, 11, 12},
		When: time.Now(),
	}
	state.DeleteResource(deletedEntry.Type, deletedEntry.Blob)

	packfileEntry := &PackfileEntry{
		Packfile:  objects.MAC{13, 14, 15, 16},
		StateID:   objects.MAC{17, 18, 19, 20},
		Timestamp: time.Now(),
	}
	state.PutPackfile(packfileEntry.StateID, packfileEntry.Packfile)

	configEntry := &ConfigurationEntry{
		Key:       "test_key",
		Value:     []byte("test_value"),
		CreatedAt: time.Now(),
	}
	state.SetConfiguration(configEntry.Key, configEntry.Value)

	// Serialize
	var buf bytes.Buffer
	err := state.SerializeToStream(&buf)
	require.NoError(t, err)

	// Verify serialized data is not empty
	require.Greater(t, buf.Len(), 0)
}

func TestDeltaEntrySerialization(t *testing.T) {
	original := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		Location: Location{
			Packfile: objects.MAC{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64},
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x12345678,
	}

	// Serialize
	data := original.ToBytes()
	require.Len(t, data, DeltaEntrySerializedSize)

	// Deserialize
	deserialized, err := DeltaEntryFromBytes(data)
	require.NoError(t, err)

	// Verify
	require.Equal(t, original.Type, deserialized.Type)
	require.Equal(t, original.Version, deserialized.Version)
	require.Equal(t, original.Blob, deserialized.Blob)
	require.Equal(t, original.Location, deserialized.Location)
	require.Equal(t, original.Flags, deserialized.Flags)
}

func TestPackfileEntrySerialization(t *testing.T) {
	original := &PackfileEntry{
		Packfile:  objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		StateID:   objects.MAC{33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64},
		Timestamp: time.Now().Truncate(time.Nanosecond),
	}

	// Serialize
	data := original.ToBytes()
	require.Len(t, data, PackfileEntrySerializedSize)

	// Deserialize
	deserialized, err := PackfileEntryFromBytes(data)
	require.NoError(t, err)

	// Verify
	require.Equal(t, original.Packfile, deserialized.Packfile)
	require.Equal(t, original.StateID, deserialized.StateID)
	require.Equal(t, original.Timestamp, deserialized.Timestamp)
}

func TestDeletedEntrySerialization(t *testing.T) {
	original := &DeletedEntry{
		Type: resources.RT_OBJECT,
		Blob: objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		When: time.Now().Truncate(time.Nanosecond),
	}

	// Serialize
	data := original.ToBytes()
	require.Len(t, data, DeletedEntrySerializedSize)

	// Deserialize
	deserialized, err := DeletedEntryFromBytes(data)
	require.NoError(t, err)

	// Verify
	require.Equal(t, original.Type, deserialized.Type)
	require.Equal(t, original.Blob, deserialized.Blob)
	require.Equal(t, original.When, deserialized.When)
}

func TestConfigurationEntrySerialization(t *testing.T) {
	original := &ConfigurationEntry{
		Key:       "test_config_key",
		Value:     []byte("test configuration value"),
		CreatedAt: time.Now().Truncate(time.Nanosecond),
	}

	// Serialize
	data := original.ToBytes()

	// Deserialize
	deserialized, err := ConfigurationEntryFromBytes(data)
	require.NoError(t, err)

	// Verify
	require.Equal(t, original.Key, deserialized.Key)
	require.Equal(t, original.Value, deserialized.Value)
	require.Equal(t, original.CreatedAt, deserialized.CreatedAt)
}

func TestBlobExists(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Test with non-existent blob
	exists := state.BlobExists(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.False(t, exists)

	// Test with existing blob and packfile
	deltaEntry := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{1, 2, 3, 4},
		Location: Location{
			Packfile: objects.MAC{5, 6, 7, 8},
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x1234,
	}
	state.PutDelta(deltaEntry)
	cache.PutPackfile(deltaEntry.Location.Packfile, []byte("packfile data"))

	// Since our mock returns all deltas regardless of MAC, this should work
	exists = state.BlobExists(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.True(t, exists)

	// Test with deleted packfile
	state.DeleteResource(resources.RT_PACKFILE, deltaEntry.Location.Packfile)
	exists = state.BlobExists(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.False(t, exists)
}

func TestGetSubpartForBlob(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Test with non-existent blob
	location, exists, err := state.GetSubpartForBlob(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.NoError(t, err)
	require.False(t, exists)

	// Test with existing blob
	expectedLocation := Location{
		Packfile: objects.MAC{5, 6, 7, 8},
		Offset:   1000,
		Length:   500,
	}

	deltaEntry := &DeltaEntry{
		Type:     resources.RT_SNAPSHOT,
		Version:  versioning.FromString("1.0.0"),
		Blob:     objects.MAC{1, 2, 3, 4},
		Location: expectedLocation,
		Flags:    0x1234,
	}
	state.PutDelta(deltaEntry)
	cache.PutPackfile(deltaEntry.Location.Packfile, []byte("packfile data"))

	// Since our mock returns all deltas regardless of MAC, this should work
	location, exists, err = state.GetSubpartForBlob(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, expectedLocation, location)
}

func TestListPackfiles(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Add some packfiles
	packfile1 := objects.MAC{1, 2, 3, 4}
	packfile2 := objects.MAC{5, 6, 7, 8}
	cache.PutPackfile(packfile1, []byte("data1"))
	cache.PutPackfile(packfile2, []byte("data2"))

	// List packfiles
	var found []objects.MAC
	for packfile := range state.ListPackfiles() {
		found = append(found, packfile)
	}

	require.Len(t, found, 2)
	require.Contains(t, found, packfile1)
	require.Contains(t, found, packfile2)
}

func TestListSnapshots(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Add snapshot delta entries
	snapshot1 := objects.MAC{1, 2, 3, 4}
	snapshot2 := objects.MAC{5, 6, 7, 8}
	packfile := objects.MAC{9, 10, 11, 12}

	delta1 := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    snapshot1,
		Location: Location{
			Packfile: packfile,
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x1234,
	}
	delta2 := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    snapshot2,
		Location: Location{
			Packfile: packfile,
			Offset:   1500,
			Length:   500,
		},
		Flags: 0x5678,
	}

	state.PutDelta(delta1)
	state.PutDelta(delta2)
	cache.PutPackfile(packfile, []byte("packfile data"))

	// List snapshots
	var found []objects.MAC
	for snapshot := range state.ListSnapshots() {
		found = append(found, snapshot)
	}

	require.Len(t, found, 2)
	require.Contains(t, found, snapshot1)
	require.Contains(t, found, snapshot2)
}

func TestListObjectsOfType(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Add objects of different types
	object1 := objects.MAC{1, 2, 3, 4}
	object2 := objects.MAC{5, 6, 7, 8}
	packfile := objects.MAC{9, 10, 11, 12}

	delta1 := &DeltaEntry{
		Type:    resources.RT_OBJECT,
		Version: versioning.FromString("1.0.0"),
		Blob:    object1,
		Location: Location{
			Packfile: packfile,
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x1234,
	}
	delta2 := &DeltaEntry{
		Type:    resources.RT_OBJECT,
		Version: versioning.FromString("1.0.0"),
		Blob:    object2,
		Location: Location{
			Packfile: packfile,
			Offset:   1500,
			Length:   500,
		},
		Flags: 0x5678,
	}

	state.PutDelta(delta1)
	state.PutDelta(delta2)
	cache.PutPackfile(packfile, []byte("packfile data"))

	// List objects of type RT_OBJECT
	var found []DeltaEntry
	for delta, err := range state.ListObjectsOfType(resources.RT_OBJECT) {
		require.NoError(t, err)
		found = append(found, delta)
	}

	require.Len(t, found, 2)
	require.Contains(t, found, *delta1)
	require.Contains(t, found, *delta2)
}

func TestListOrphanDeltas(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Add delta with missing packfile (orphan)
	orphanDelta := &DeltaEntry{
		Type:    resources.RT_OBJECT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{1, 2, 3, 4},
		Location: Location{
			Packfile: objects.MAC{5, 6, 7, 8}, // This packfile doesn't exist
			Offset:   1000,
			Length:   500,
		},
		Flags: 0x1234,
	}
	state.PutDelta(orphanDelta)

	// List orphan deltas
	var found []DeltaEntry
	for delta, err := range state.ListOrphanDeltas() {
		require.NoError(t, err)
		found = append(found, delta)
	}

	require.Len(t, found, 1)
	require.Equal(t, *orphanDelta, found[0])
}

func TestDeleteResource(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	resource := objects.MAC{1, 2, 3, 4}
	err := state.DeleteResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)

	// Verify resource is marked as deleted
	hasDeleted, err := state.HasDeletedResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestHasDeletedResource(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	resource := objects.MAC{1, 2, 3, 4}

	// Test non-existent deleted resource
	hasDeleted, err := state.HasDeletedResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.False(t, hasDeleted)

	// Delete the resource
	state.DeleteResource(resources.RT_OBJECT, resource)

	// Test existing deleted resource
	hasDeleted, err = state.HasDeletedResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestSetConfiguration(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	key := "test_key"
	value := []byte("test_value")

	err := state.SetConfiguration(key, value)
	require.NoError(t, err)

	// Verify configuration was set
	configData, err := cache.GetConfiguration(key)
	require.NoError(t, err)
	require.NotNil(t, configData)

	// Deserialize and verify
	config, err := ConfigurationEntryFromBytes(configData)
	require.NoError(t, err)
	require.Equal(t, key, config.Key)
	require.Equal(t, value, config.Value)
}

func TestListDeletedResources(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Delete some resources
	resource1 := objects.MAC{1, 2, 3, 4}
	resource2 := objects.MAC{5, 6, 7, 8}

	state.DeleteResource(resources.RT_OBJECT, resource1)
	state.DeleteResource(resources.RT_OBJECT, resource2)

	// List deleted resources
	var found []DeletedEntry
	for deleted, err := range state.ListDeletedResources(resources.RT_OBJECT) {
		require.NoError(t, err)
		found = append(found, deleted)
	}

	require.Len(t, found, 2)
	require.Equal(t, resources.RT_OBJECT, found[0].Type)
	require.Equal(t, resources.RT_OBJECT, found[1].Type)
	require.Contains(t, []objects.MAC{found[0].Blob, found[1].Blob}, resource1)
	require.Contains(t, []objects.MAC{found[0].Blob, found[1].Blob}, resource2)
}

func TestDelDeletedResource(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	resource := objects.MAC{1, 2, 3, 4}

	// Delete resource
	state.DeleteResource(resources.RT_OBJECT, resource)

	// Verify it's deleted
	hasDeleted, err := state.HasDeletedResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.True(t, hasDeleted)

	// Remove deleted resource
	err = state.DelDeletedResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)

	// Verify it's no longer marked as deleted
	hasDeleted, err = state.HasDeletedResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.False(t, hasDeleted)
}

func TestMetadataSerialization(t *testing.T) {
	original := &Metadata{
		Version:   versioning.FromString("1.0.0"),
		Timestamp: time.Now().Truncate(time.Nanosecond),
		Serial:    uuid.New(),
	}

	// Serialize
	data, err := original.ToBytes()
	require.NoError(t, err)
	require.NotNil(t, data)

	// Deserialize
	deserialized, err := MetadataFromBytes(data)
	require.NoError(t, err)
	require.NotNil(t, deserialized)

	// Verify
	require.Equal(t, original.Version, deserialized.Version)
	require.Equal(t, original.Timestamp, deserialized.Timestamp)
	require.Equal(t, original.Serial, deserialized.Serial)
}

func TestMetadataFromBytesError(t *testing.T) {
	// Test with invalid data
	_, err := MetadataFromBytes([]byte("invalid data"))
	require.Error(t, err)
}

func TestDeltaEntryFromBytesError(t *testing.T) {
	// Test with insufficient data - enough for type+version, but not for MAC
	_, err := DeltaEntryFromBytes([]byte{1, 0, 0, 0, 0}) // Too short for MAC
	require.Error(t, err)
}

func TestPackfileEntryFromBytesError(t *testing.T) {
	// Test with insufficient data
	_, err := PackfileEntryFromBytes([]byte{1, 2, 3}) // Too short
	require.Error(t, err)
}

func TestDeletedEntryFromBytesError(t *testing.T) {
	// Test with insufficient data
	_, err := DeletedEntryFromBytes([]byte{1, 2, 3}) // Too short
	require.Error(t, err)
}

func TestDeserializeFromStreamError(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Test with invalid stream
	invalidData := []byte{byte(ET_LOCATIONS), 0, 0, 0, 1} // Invalid length
	reader := bytes.NewReader(invalidData)

	err := state.deserializeFromStream(reader)
	require.Error(t, err)
}

func TestSerializeToStreamError(t *testing.T) {
	cache := newMockStateCache()
	state := NewLocalState(cache)

	// Create a writer that will fail
	failingWriter := &failingWriter{}

	err := state.SerializeToStream(failingWriter)
	require.Error(t, err)
}

// failingWriter is a writer that always fails
type failingWriter struct{}

func (fw *failingWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrShortWrite
}
