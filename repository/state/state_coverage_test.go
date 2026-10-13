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

// TestSetConfigurationOverwriteNewer exercises the
// "oldCe.CreatedAt.Before(ce.CreatedAt)" branch of insertOrUpdateConfiguration:
// after seeding an older entry, a newer SetConfiguration call replaces it.
func TestSetConfigurationOverwriteNewer(t *testing.T) {
	st, cache := newAggregate(t)

	older := ConfigurationEntry{
		Key:       "k",
		Value:     []byte("older"),
		CreatedAt: time.Now().Add(-1 * time.Hour),
	}
	require.NoError(t, cache.PutConfiguration(older.Key, older.ToBytes()))

	// Now SetConfiguration writes a newer entry; the "newer" branch overwrites.
	require.NoError(t, st.SetConfiguration("k", []byte("newer")))

	got, err := cache.GetConfiguration("k")
	require.NoError(t, err)
	ce, err := ConfigurationEntryFromBytes(got)
	require.NoError(t, err)
	require.Equal(t, []byte("newer"), ce.Value)
}

// TestSetConfigurationKeepsOlder exercises the "no overwrite" branch:
// when the stored entry is newer than the incoming one, insertOrUpdate keeps
// the stored value.
func TestSetConfigurationKeepsOlder(t *testing.T) {
	st, cache := newAggregate(t)

	// Seed cache with a future-dated entry so subsequent SetConfiguration
	// won't replace it.
	future := ConfigurationEntry{
		Key:       "k",
		Value:     []byte("future"),
		CreatedAt: time.Now().Add(1 * time.Hour),
	}
	require.NoError(t, cache.PutConfiguration(future.Key, future.ToBytes()))

	require.NoError(t, st.SetConfiguration("k", []byte("now")))

	got, err := cache.GetConfiguration("k")
	require.NoError(t, err)
	ce, err := ConfigurationEntryFromBytes(got)
	require.NoError(t, err)
	require.Equal(t, []byte("future"), ce.Value)
}

// TestListSnapshotsWithMissingPackfile exercises ListSnapshots's continue
// branch when the packfile referenced by a delta is not in the cache.
func TestListSnapshotsWithMissingPackfile(t *testing.T) {
	st, cache := newAggregate(t)

	pfMissing := objects.MAC{0xAA}
	pfPresent := objects.MAC{0xBB}

	// Two snapshot delta entries — one referring to a packfile in the
	// cache, one to a packfile that isn't.
	deltaMissing := DeltaEntry{
		Type:     resources.RT_SNAPSHOT,
		Blob:     objects.MAC{0x01},
		Location: Location{Packfile: pfMissing},
	}
	deltaPresent := DeltaEntry{
		Type:     resources.RT_SNAPSHOT,
		Blob:     objects.MAC{0x02},
		Location: Location{Packfile: pfPresent},
	}
	require.NoError(t, cache.PutDelta(resources.RT_SNAPSHOT, deltaMissing.Blob, pfMissing, deltaMissing.ToBytes()))
	require.NoError(t, cache.PutDelta(resources.RT_SNAPSHOT, deltaPresent.Blob, pfPresent, deltaPresent.ToBytes()))

	require.NoError(t, cache.PutPackfile(pfPresent, []byte("pf-data")))

	// ListSnapshots must produce exactly the entry whose packfile is present
	// in the cache.
	var found []objects.MAC
	for snap, err := range st.ListSnapshots() {
		require.NoError(t, err)
		found = append(found, snap)
	}
	require.Equal(t, []objects.MAC{deltaPresent.Blob}, found)
}

// TestListSnapshotsSkipsColoured exercises the path where a snapshot has been
// coloured (marked deleted) — it should be skipped by ListSnapshots.
func TestListSnapshotsSkipsColoured(t *testing.T) {
	st, cache := newAggregate(t)

	pf := objects.MAC{0xCC}
	require.NoError(t, cache.PutPackfile(pf, []byte("pf")))

	snapID := objects.MAC{0xDD}
	delta := DeltaEntry{
		Type:     resources.RT_SNAPSHOT,
		Blob:     snapID,
		Location: Location{Packfile: pf},
	}
	require.NoError(t, cache.PutDelta(resources.RT_SNAPSHOT, snapID, pf, delta.ToBytes()))

	// Mark the snapshot as coloured (deleted); the aggregate receives coloured
	// entries via merge, so seed the cache directly.
	colourInCache(t, cache, resources.RT_SNAPSHOT, snapID)

	count := 0
	for _, err := range st.ListSnapshots() {
		require.NoError(t, err)
		count++
	}
	require.Zero(t, count)
}

// TestListOrphanDeltasWithMixedState seeds both present and missing-packfile
// deltas; ListOrphanDeltas should only emit those whose packfile is absent.
func TestListOrphanDeltasWithMixedState(t *testing.T) {
	st, cache := newAggregate(t)

	pfPresent := objects.MAC{0xA1}
	require.NoError(t, cache.PutPackfile(pfPresent, []byte("pf")))

	orphan := DeltaEntry{
		Type:     resources.RT_CHUNK,
		Blob:     objects.MAC{0x9F},
		Location: Location{Packfile: objects.MAC{0xFA}},
	}
	healthy := DeltaEntry{
		Type:     resources.RT_CHUNK,
		Blob:     objects.MAC{0x8E},
		Location: Location{Packfile: pfPresent},
	}
	require.NoError(t, cache.PutDelta(resources.RT_CHUNK, orphan.Blob, orphan.Location.Packfile, orphan.ToBytes()))
	require.NoError(t, cache.PutDelta(resources.RT_CHUNK, healthy.Blob, pfPresent, healthy.ToBytes()))

	var found []DeltaEntry
	for de, err := range st.ListOrphanDeltas() {
		require.NoError(t, err)
		found = append(found, de)
	}
	require.Equal(t, []DeltaEntry{orphan}, found)
}

// TestSerializeDeserializeAllEntryTypes drives a complete round-trip of every
// entry type the on-stream format supports, so both State.SerializeToStream
// and LocalState.deserializeFromStream cover the per-entry branches (DELETE,
// LOCATIONS, COLOURED, PACKFILE, CONFIGURATION, METADATA).
func TestSerializeDeserializeAllEntryTypes(t *testing.T) {
	src, srcCache := newDeltaState(t)

	// LOCATIONS / delta
	require.NoError(t, src.PutDelta(&DeltaEntry{
		Type:     resources.RT_SNAPSHOT,
		Version:  versioning.FromString("1.0.0"),
		Blob:     objects.MAC{0xD1},
		Location: Location{Packfile: objects.MAC{0xB1}, Offset: 10, Length: 5},
	}))

	// COLOURED
	require.NoError(t, src.ColourResource(resources.RT_OBJECT, objects.MAC{0xC1}))

	// PACKFILE
	require.NoError(t, src.PutPackfile(objects.MAC{0xB1}, objects.MAC{0xB2}))

	// DELETE (delta deletion)
	require.NoError(t, src.DelDelta(resources.RT_CHUNK, objects.MAC{0xDE}, objects.MAC{0xAD}))

	// CONFIGURATION
	ce := ConfigurationEntry{Key: "ck", Value: []byte("cv"), CreatedAt: time.Now()}
	require.NoError(t, srcCache.PutConfiguration(ce.Key, ce.ToBytes()))

	var buf bytes.Buffer
	require.NoError(t, src.SerializeToStream(&buf))

	dst, _ := newAggregate(t)
	require.NoError(t, dst.deserializeFromStream(&buf))
	require.Equal(t, src.Metadata.Serial, dst.Metadata.Serial)
}

// TestDeserializeFromStreamShortHeader feeds too few bytes so reading the
// state header returns an error.
func TestDeserializeFromStreamShortHeader(t *testing.T) {
	st, _ := newAggregate(t)
	require.Error(t, st.deserializeFromStream(bytes.NewReader(make([]byte, 5))))
}

// TestDeserializeFromStreamBadEntryLength constructs a header plus an entry
// type byte plus a wrong length, hitting the "wrong length" branch.
func TestDeserializeFromStreamBadEntryLength(t *testing.T) {
	buf := &bytes.Buffer{}
	// Header: 32 bytes of Parent MAC.
	buf.Write(make([]byte, 32))
	// Entry type ET_LOCATIONS.
	buf.WriteByte(byte(ET_LOCATIONS))
	// Wrong length (0).
	buf.Write([]byte{0, 0, 0, 0})

	st, _ := newAggregate(t)
	require.Error(t, st.deserializeFromStream(buf))
}

// TestDeserializeFromStreamUnknownEntryType writes an unrecognised entry
// type so the deserialize loop skips it via the default branch.
func TestDeserializeFromStreamUnknownEntryType(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.Write(make([]byte, 32)) // header parent
	// Unknown entry type 0x7F, length 3, then 3 payload bytes.
	buf.WriteByte(0x7F)
	buf.Write([]byte{3, 0, 0, 0})
	buf.Write([]byte{0xAA, 0xBB, 0xCC})
	// Terminating ET_METADATA + version + timestamp + serial.
	buf.WriteByte(byte(ET_METADATA))
	buf.Write([]byte{0, 0, 0, 0}) // version
	buf.Write(make([]byte, 8))    // timestamp
	buf.Write(make([]byte, 16))   // serial

	st, _ := newAggregate(t)
	require.NoError(t, st.deserializeFromStream(buf))
}

// TestDeserializeFromStreamV100AllEntryTypes feeds a hand-rolled v1.0.0
// state stream (no header MAC, no DELETE entries) so deserializeFromStreamv100
// is driven through each case branch.
func TestDeserializeFromStreamV100AllEntryTypes(t *testing.T) {
	buf := &bytes.Buffer{}

	// LOCATIONS entry
	delta := DeltaEntry{
		Type:     resources.RT_OBJECT,
		Version:  versioning.FromString("1.0.0"),
		Blob:     objects.MAC{0x01},
		Location: Location{Packfile: objects.MAC{0x10}, Offset: 1, Length: 2},
	}
	buf.WriteByte(byte(ET_LOCATIONS))
	writeUint32LE(buf, DeltaEntrySerializedSize)
	buf.Write(delta.ToBytes())

	// COLOURED entry
	coloured := ColouredEntry{
		Type: resources.RT_OBJECT,
		Blob: objects.MAC{0x02},
		When: time.Now(),
	}
	buf.WriteByte(byte(ET_COLOURED))
	writeUint32LE(buf, ColouredEntrySerializedSize)
	buf.Write(coloured.ToBytes())

	// PACKFILE entry
	pe := PackfileEntry{
		Packfile:  objects.MAC{0x03},
		StateID:   objects.MAC{0x30},
		Timestamp: time.Now(),
	}
	buf.WriteByte(byte(ET_PACKFILE))
	writeUint32LE(buf, PackfileEntrySerializedSize)
	buf.Write(pe.ToBytes())

	// CONFIGURATION entry
	ce := ConfigurationEntry{Key: "vk", Value: []byte("vv"), CreatedAt: time.Now()}
	ceBytes := ce.ToBytes()
	buf.WriteByte(byte(ET_CONFIGURATION))
	writeUint32LE(buf, uint32(len(ceBytes)))
	buf.Write(ceBytes)

	// Unknown entry (default branch — skip via CopyN)
	buf.WriteByte(0x7F)
	writeUint32LE(buf, 3)
	buf.Write([]byte{1, 2, 3})

	// METADATA terminator + fields
	buf.WriteByte(byte(ET_METADATA))
	writeUint32LE(buf, 0)         // version
	writeUint64LE(buf, uint64(0)) // timestamp
	buf.Write(make([]byte, 16))   // serial

	st, _ := newAggregate(t)
	require.NoError(t, st.deserializeFromStreamv100(buf))
}

func writeUint32LE(buf *bytes.Buffer, v uint32) {
	tmp := [4]byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)}
	buf.Write(tmp[:])
}

func writeUint64LE(buf *bytes.Buffer, v uint64) {
	tmp := [8]byte{
		byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56),
	}
	buf.Write(tmp[:])
}

// TestMergeFromCachePopulated exercises mergeFromCache with all four cache
// categories populated, so each loop body in the merge function runs.
func TestMergeFromCachePopulated(t *testing.T) {
	src, srcCache := newDeltaState(t)

	require.NoError(t, src.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Blob: objects.MAC{0x01},
		Location: Location{Packfile: objects.MAC{0x10}},
	}))
	require.NoError(t, src.ColourResource(resources.RT_OBJECT, objects.MAC{0x02}))
	require.NoError(t, src.PutPackfile(objects.MAC{0x03}, objects.MAC{0x30}))
	ce := ConfigurationEntry{Key: "mfk", Value: []byte("mfv"), CreatedAt: time.Now()}
	require.NoError(t, srcCache.PutConfiguration(ce.Key, ce.ToBytes()))

	dst, _ := newAggregate(t)

	require.NoError(t, dst.mergeFromCache(srcCache))
}

// TestMergeStateFromCacheAlreadyHas exercises the early-return branch of
// MergeStateFromCache when HasState already returns true.
func TestMergeStateFromCacheAlreadyHas(t *testing.T) {
	st, cache := newAggregate(t)

	stateID := objects.MAC{0xCA, 0xFE}
	mt := Metadata{Version: versioning.FromString(VERSION), Timestamp: time.Now()}
	mtBytes, err := mt.ToBytes()
	require.NoError(t, err)
	require.NoError(t, cache.PutState(stateID, mtBytes))

	src := newScanCache(t)
	require.NoError(t, st.MergeStateFromCache(stateID, src))
}

// TestSerializeToStreamWithAllEntryTypes follows up by triggering each
// State.SerializeToStream branch (DELETE / LOCATIONS / COLOURED / PACKFILE /
// CONFIGURATION) with a non-empty cache.
func TestSerializeToStreamWithAllEntryTypes(t *testing.T) {
	st, cache := newDeltaState(t)

	require.NoError(t, st.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Blob: objects.MAC{0x01},
		Location: Location{Packfile: objects.MAC{0x10}},
	}))
	require.NoError(t, st.ColourResource(resources.RT_OBJECT, objects.MAC{0x02}))
	require.NoError(t, st.PutPackfile(objects.MAC{0x03}, objects.MAC{0x30}))
	require.NoError(t, st.DelDelta(resources.RT_OBJECT, objects.MAC{0x04}, objects.MAC{0x40}))
	ce := ConfigurationEntry{Key: "sk", Value: []byte("sv"), CreatedAt: time.Now()}
	require.NoError(t, cache.PutConfiguration(ce.Key, ce.ToBytes()))

	var buf bytes.Buffer
	require.NoError(t, st.SerializeToStream(&buf))
	require.NotZero(t, buf.Len())

	// And read it back too, so deserializeFromStream gets all branches.
	dst, _ := newAggregate(t)
	require.NoError(t, dst.deserializeFromStream(&buf))
}
