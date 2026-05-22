package state

import (
	"bytes"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newStateWithCache(t *testing.T) (*LocalState, *mockStateCache) {
	t.Helper()
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	return st, cache
}

// ---------------------------------------------------------------------------
// DeleteEntry ToBytes / FromBytes round-trip
// ---------------------------------------------------------------------------

func TestDeleteEntryToBytesRoundtrip(t *testing.T) {
	original := DeleteEntry{
		Type:     ET_LOCATIONS,
		BlobType: resources.RT_SNAPSHOT,
		Blob:     objects.MAC{1, 2, 3, 4, 5},
		Packfile: objects.MAC{6, 7, 8, 9, 10},
	}

	data := original.ToBytes()
	require.Len(t, data, DeleteEntrySerializedSize)

	got, err := DeleteEntryFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, original.Type, got.Type)
	require.Equal(t, original.BlobType, got.BlobType)
	require.Equal(t, original.Blob, got.Blob)
	require.Equal(t, original.Packfile, got.Packfile)
}

func TestDeleteEntryFromBytesShort(t *testing.T) {
	// Only 1 byte — too short to read the BlobType byte.
	_, err := DeleteEntryFromBytes([]byte{byte(ET_PACKFILE)})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// DelState
// ---------------------------------------------------------------------------

func TestDelState(t *testing.T) {
	st, cache := newStateWithCache(t)

	stateID := objects.MAC{0xDE, 0xAD}
	mt := Metadata{
		Version:   versioning.FromString(VERSION),
		Timestamp: time.Now(),
	}
	data, err := mt.ToBytes()
	require.NoError(t, err)
	require.NoError(t, cache.PutState(stateID, data))

	has, err := st.HasState(stateID)
	require.NoError(t, err)
	require.True(t, has)

	require.NoError(t, st.DelState(stateID))

	has, err = st.HasState(stateID)
	require.NoError(t, err)
	require.False(t, has)
}

// ---------------------------------------------------------------------------
// GetStates
// ---------------------------------------------------------------------------

func TestGetStates(t *testing.T) {
	st, cache := newStateWithCache(t)

	id1 := objects.MAC{0x01}
	id2 := objects.MAC{0x02}
	mt := Metadata{Version: versioning.FromString(VERSION), Timestamp: time.Now()}
	data, _ := mt.ToBytes()
	cache.PutState(id1, data)
	cache.PutState(id2, data)

	states, err := st.GetStates()
	require.NoError(t, err)
	require.Len(t, states, 2)
	_, ok1 := states[id1]
	_, ok2 := states[id2]
	require.True(t, ok1)
	require.True(t, ok2)
}

// ---------------------------------------------------------------------------
// NewBatch
// ---------------------------------------------------------------------------

func TestNewBatch(t *testing.T) {
	st, _ := newStateWithCache(t)
	batch := st.NewBatch()
	require.NotNil(t, batch)

	// Inserting into the batch should work without error.
	mac := objects.MAC{0xAA, 0xBB}
	pfMAC := objects.MAC{0xCC, 0xDD}
	de := &DeltaEntry{
		Type:    resources.RT_CHUNK,
		Version: versioning.FromString("1.0.0"),
		Blob:    mac,
		Location: Location{
			Packfile: pfMAC,
			Offset:   0,
			Length:   64,
		},
	}
	err := batch.PutDelta(de.Type, de.Blob, de.Location.Packfile, de.ToBytes())
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// ---------------------------------------------------------------------------
// DelDelta
// ---------------------------------------------------------------------------

func TestDelDelta(t *testing.T) {
	st, cache := newStateWithCache(t)

	blobMAC := objects.MAC{0x11}
	pfMAC := objects.MAC{0x22}
	de := &DeltaEntry{
		Type:    resources.RT_CHUNK,
		Version: versioning.FromString("1.0.0"),
		Blob:    blobMAC,
		Location: Location{Packfile: pfMAC, Offset: 0, Length: 32},
	}
	require.NoError(t, st.PutDelta(de))

	// After PutDelta the delta should be visible via the cache.
	require.NotEmpty(t, cache.deltas)

	// DelDelta records a delete entry (does not touch cache.deltas directly in
	// the current implementation — it writes a deleted marker to the cache).
	err := st.DelDelta(resources.RT_CHUNK, blobMAC, pfMAC)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// DelPackfile
// ---------------------------------------------------------------------------

func TestDelPackfile(t *testing.T) {
	st, cache := newStateWithCache(t)

	pfMAC := objects.MAC{0x33}
	// First, add the packfile.
	require.NoError(t, st.PutPackfile(objects.MAC{0x00}, pfMAC))
	has, err := cache.HasPackfile(pfMAC)
	require.NoError(t, err)
	require.True(t, has)

	// DelPackfile marks the packfile as deleted (writes a delete entry to the
	// "deleted" store inside the cache).
	require.NoError(t, st.DelPackfile(pfMAC))
	// The packfile itself is still present in the cache map (the delete entry
	// is separate). What matters is the operation does not error.
}

// ---------------------------------------------------------------------------
// ListPackfileEntries
// ---------------------------------------------------------------------------

func TestListPackfileEntries(t *testing.T) {
	st, _ := newStateWithCache(t)

	pf1 := objects.MAC{0xAA}
	pf2 := objects.MAC{0xBB}
	require.NoError(t, st.PutPackfile(objects.MAC{0x01}, pf1))
	require.NoError(t, st.PutPackfile(objects.MAC{0x02}, pf2))

	var entries []PackfileEntry
	for pe, err := range st.ListPackfileEntries() {
		require.NoError(t, err)
		entries = append(entries, pe)
	}
	require.Len(t, entries, 2)

	var pfs []objects.MAC
	for _, e := range entries {
		pfs = append(pfs, e.Packfile)
	}
	require.Contains(t, pfs, pf1)
	require.Contains(t, pfs, pf2)
}

// ---------------------------------------------------------------------------
// DelColouredResource
// ---------------------------------------------------------------------------

func TestDelColouredResource(t *testing.T) {
	st, _ := newStateWithCache(t)

	resMAC := objects.MAC{0x44}
	// First colour it.
	require.NoError(t, st.ColourResource(resources.RT_SNAPSHOT, resMAC))

	// Now request to delete the coloured resource — this records a delete marker.
	err := st.DelColouredResource(resources.RT_SNAPSHOT, resMAC)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// DeltaEntry ToBytes / FromBytes
// ---------------------------------------------------------------------------

func TestDeltaEntryToBytesRoundtrip(t *testing.T) {
	original := DeltaEntry{
		Type:    resources.RT_OBJECT,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{10, 20, 30},
		Location: Location{
			Packfile: objects.MAC{40, 50, 60},
			Offset:   9999,
			Length:   512,
		},
		Flags: 0xDEAD,
	}

	data := original.ToBytes()
	require.Len(t, data, DeltaEntrySerializedSize)

	got, err := DeltaEntryFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, original.Type, got.Type)
	require.Equal(t, original.Version, got.Version)
	require.Equal(t, original.Blob, got.Blob)
	require.Equal(t, original.Location, got.Location)
	require.Equal(t, original.Flags, got.Flags)
}

// ---------------------------------------------------------------------------
// ReadHeader
// ---------------------------------------------------------------------------

func TestReadHeaderValid(t *testing.T) {
	ver := versioning.FromString("1.1.0")

	parent := objects.MAC{0x11, 0x22, 0x33}
	buf := bytes.NewBuffer(parent[:])

	hdr, err := ReadHeader(buf, ver)
	require.NoError(t, err)
	require.NotNil(t, hdr)
	require.Equal(t, parent, hdr.Parent)
}

func TestReadHeaderShortBuffer(t *testing.T) {
	ver := versioning.FromString("1.1.0")
	// Empty buffer — should fail to read the 32-byte parent MAC.
	_, err := ReadHeader(bytes.NewBuffer([]byte{}), ver)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// SerializeToStream round-trip with delete entries
// ---------------------------------------------------------------------------

func TestSerializeWithDeleteEntries(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// Add a delta and a packfile, then delete both.
	blobMAC := objects.MAC{0xBE, 0xEF}
	pfMAC := objects.MAC{0xCA, 0xFE}
	de := &DeltaEntry{
		Type:    resources.RT_CHUNK,
		Version: versioning.FromString("1.0.0"),
		Blob:    blobMAC,
		Location: Location{Packfile: pfMAC, Offset: 0, Length: 16},
	}
	require.NoError(t, st.PutDelta(de))
	require.NoError(t, st.PutPackfile(objects.NilMac, pfMAC))
	require.NoError(t, st.DelDelta(resources.RT_CHUNK, blobMAC, pfMAC))
	require.NoError(t, st.DelPackfile(pfMAC))

	var buf bytes.Buffer
	require.NoError(t, st.SerializeToStream(&buf))
	require.Greater(t, buf.Len(), 0)

	// Deserialise back and make sure it does not error.
	cache2 := newMockStateCache()
	st2, err := FromStream(&buf, versioning.FromString("1.1.0"), cache2)
	require.NoError(t, err)
	require.NotNil(t, st2)
}

// ---------------------------------------------------------------------------
// mergeFromCache
// ---------------------------------------------------------------------------

func TestMergeStateFromCache(t *testing.T) {
	// Build a "source" state in its own cache.
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)

	blobMAC := objects.MAC{0x77}
	pfMAC := objects.MAC{0x88}
	de := &DeltaEntry{
		Type:    resources.RT_SNAPSHOT,
		Version: versioning.FromString("1.0.0"),
		Blob:    blobMAC,
		Location: Location{Packfile: pfMAC, Offset: 0, Length: 8},
	}
	require.NoError(t, src.PutDelta(de))
	require.NoError(t, src.PutPackfile(objects.NilMac, pfMAC))

	// Merge into a different state using MergeStateFromCache.
	dstCache := newMockStateCache()
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	stateID := objects.MAC{0x99}
	err = dst.MergeStateFromCache(stateID, srcCache)
	require.NoError(t, err)

	has, err := dst.HasState(stateID)
	require.NoError(t, err)
	require.True(t, has)
}

// ---------------------------------------------------------------------------
// deserializeFromStreamv100 (v1.0.0)
// ---------------------------------------------------------------------------

// buildV100Stream manually builds a minimal v1.0.0 stream.
// v1.0.0 format has NO header; it goes directly to <EntryType><EntryLength><Entry>...<ET_METADATA>...
func buildV100Stream(t *testing.T) *bytes.Buffer {
	t.Helper()

	writeUint32 := func(buf *bytes.Buffer, v uint32) {
		b := make([]byte, 4)
		b[0] = byte(v)
		b[1] = byte(v >> 8)
		b[2] = byte(v >> 16)
		b[3] = byte(v >> 24)
		buf.Write(b)
	}
	writeUint64 := func(buf *bytes.Buffer, v uint64) {
		b := make([]byte, 8)
		for i := range 8 {
			b[i] = byte(v >> (uint(i) * 8))
		}
		buf.Write(b)
	}

	buf := &bytes.Buffer{}

	// A single ET_LOCATIONS entry.
	de := &DeltaEntry{
		Type:    resources.RT_CHUNK,
		Version: versioning.FromString("1.0.0"),
		Blob:    objects.MAC{0x55},
		Location: Location{Packfile: objects.MAC{0x66}, Offset: 0, Length: 4},
	}
	buf.WriteByte(byte(ET_LOCATIONS))
	writeUint32(buf, DeltaEntrySerializedSize)
	buf.Write(de.ToBytes())

	// ET_METADATA terminator.
	buf.WriteByte(byte(ET_METADATA))
	// version (4 bytes)
	writeUint32(buf, uint32(versioning.FromString(VERSION)))
	// timestamp (8 bytes)
	writeUint64(buf, uint64(time.Now().UnixNano()))
	// serial (16 bytes UUID)
	buf.Write(make([]byte, 16))

	return buf
}

func TestDeserializeFromStreamv100(t *testing.T) {
	stream := buildV100Stream(t)

	// Deserialise as version 1.0.0 (uses deserializeFromStreamv100 code path).
	cache := newMockStateCache()
	st, err := FromStream(stream, versioning.FromString("1.0.0"), cache)
	require.NoError(t, err)
	require.NotNil(t, st)
}
