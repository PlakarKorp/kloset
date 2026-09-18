package state

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Test caches are the real backends in per-test temporary directories: the
// aggregate (LocalState) runs on an SQLState, the unitary delta state (State)
// runs on a ScanCache obtained through Derive, exactly as in production.

func newSQLState(t *testing.T) *caching.SQLState {
	t.Helper()

	cache, err := caching.NewSQLState(t.TempDir(), false)
	require.NoError(t, err)

	return cache
}

func newScanCache(t *testing.T) *caching.ScanCache {
	t.Helper()

	man := caching.NewManager(pebble.Constructor(t.TempDir()))
	t.Cleanup(func() { man.Close() })

	sc, err := man.Scan(objects.RandomMAC())
	require.NoError(t, err)
	t.Cleanup(func() { sc.Close() })

	return sc
}

func newAggregate(t *testing.T) (*LocalState, *caching.SQLState) {
	t.Helper()

	cache := newSQLState(t)
	ls, err := NewLocalState(cache)
	require.NoError(t, err)

	return ls, cache
}

func newDeltaState(t *testing.T) (*State, *caching.ScanCache) {
	t.Helper()

	ls, _ := newAggregate(t)
	ls.Metadata.Serial = uuid.New()

	sc := newScanCache(t)
	return ls.Derive(sc), sc
}

func TestNewLocalState(t *testing.T) {
	cache := newSQLState(t)
	state, err := NewLocalState(cache)

	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, versioning.FromString(VERSION), state.Metadata.Version)
	require.NotZero(t, state.Metadata.Timestamp)
	require.NotNil(t, state.configuration)
	require.Equal(t, cache, state.cache)
}

func TestDerive(t *testing.T) {
	originalState, _ := newAggregate(t)
	originalState.Metadata.Serial = uuid.New()

	scanCache := newScanCache(t)
	derivedState := originalState.Derive(scanCache)

	require.NotNil(t, derivedState)
	require.Equal(t, originalState.Metadata.Serial, derivedState.Metadata.Serial)
	require.Equal(t, scanCache, derivedState.cache)
}

func TestUpdateSerialOr(t *testing.T) {
	state, cache := newAggregate(t)

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
	state, _ := newAggregate(t)

	// Build a delta state stream to merge.
	src, _ := newDeltaState(t)

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
	require.NoError(t, src.PutDelta(deltaEntry))

	var buf bytes.Buffer
	err := src.SerializeToStream(&buf)
	require.NoError(t, err)

	// Merge the state
	stateID := objects.MAC{1, 2, 3, 4}
	err = state.MergeState(stateID, &buf, versioning.FromString("1.1.0"))
	require.NoError(t, err)

	// Verify the state was merged
	hasState, err := state.HasState(stateID)
	require.NoError(t, err)
	require.True(t, hasState)
}

func TestPutState(t *testing.T) {
	state, _ := newAggregate(t)
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
	state, scanCache := newDeltaState(t)

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

	deletedEntry := &ColouredEntry{
		Type: resources.RT_OBJECT,
		Blob: objects.MAC{9, 10, 11, 12},
		When: time.Now(),
	}
	state.ColourResource(deletedEntry.Type, deletedEntry.Blob)

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
	scanCache.PutConfiguration(configEntry.Key, configEntry.ToBytes())

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
	original := &ColouredEntry{
		Type: resources.RT_OBJECT,
		Blob: objects.MAC{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		When: time.Now().Truncate(time.Nanosecond),
	}

	// Serialize
	data := original.ToBytes()
	require.Len(t, data, ColouredEntrySerializedSize)

	// Deserialize
	deserialized, err := ColouredEntryFromBytes(data)
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
	state, cache := newAggregate(t)

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

	exists = state.BlobExists(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.True(t, exists)

	// Test with deleted packfile
	state.ColourResource(resources.RT_PACKFILE, deltaEntry.Location.Packfile)
	exists = state.BlobExists(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.False(t, exists)
}

func TestGetSubpartForBlob(t *testing.T) {
	state, cache := newAggregate(t)

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

	location, exists, err = state.GetSubpartForBlob(resources.RT_SNAPSHOT, objects.MAC{1, 2, 3, 4})
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, expectedLocation, location)
}

func TestListPackfiles(t *testing.T) {
	state, cache := newAggregate(t)

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
	state, cache := newAggregate(t)

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
	for snapshot, err := range state.ListSnapshots() {
		require.NoError(t, err)
		found = append(found, snapshot)
	}

	require.Len(t, found, 2)
	require.Contains(t, found, snapshot1)
	require.Contains(t, found, snapshot2)
}

func TestListObjectsOfType(t *testing.T) {
	state, cache := newAggregate(t)

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
	state, _ := newAggregate(t)

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
	state, _ := newAggregate(t)

	resource := objects.MAC{1, 2, 3, 4}
	err := state.ColourResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)

	// Verify resource is marked as deleted
	hasDeleted, err := state.HasColouredResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestHasDeletedResource(t *testing.T) {
	state, _ := newAggregate(t)

	resource := objects.MAC{1, 2, 3, 4}

	// Test non-existent deleted resource
	hasDeleted, err := state.HasColouredResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.False(t, hasDeleted)

	// Delete the resource
	state.ColourResource(resources.RT_OBJECT, resource)

	// Test existing deleted resource
	hasDeleted, err = state.HasColouredResource(resources.RT_OBJECT, resource)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestSetConfiguration(t *testing.T) {
	state, cache := newAggregate(t)

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
	state, _ := newAggregate(t)

	// Delete some resources
	resource1 := objects.MAC{1, 2, 3, 4}
	resource2 := objects.MAC{5, 6, 7, 8}

	state.ColourResource(resources.RT_OBJECT, resource1)
	state.ColourResource(resources.RT_OBJECT, resource2)

	// List deleted resources
	var found []ColouredEntry
	for deleted, err := range state.ListColouredResources(resources.RT_OBJECT) {
		require.NoError(t, err)
		found = append(found, deleted)
	}

	require.Len(t, found, 2)
	require.Equal(t, resources.RT_OBJECT, found[0].Type)
	require.Equal(t, resources.RT_OBJECT, found[1].Type)
	require.Contains(t, []objects.MAC{found[0].Blob, found[1].Blob}, resource1)
	require.Contains(t, []objects.MAC{found[0].Blob, found[1].Blob}, resource2)
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
	_, err := ColouredEntryFromBytes([]byte{1, 2, 3}) // Too short
	require.Error(t, err)
}

func TestDeserializeFromStreamError(t *testing.T) {
	state, _ := newAggregate(t)

	// Test with invalid stream
	invalidData := []byte{byte(ET_LOCATIONS), 0, 0, 0, 1} // Invalid length
	reader := bytes.NewReader(invalidData)

	err := state.deserializeFromStream(reader)
	require.Error(t, err)
}

func TestSerializeToStreamError(t *testing.T) {
	state, _ := newDeltaState(t)

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
