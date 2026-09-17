// Golden-fixture tests for the state wire format and its merge semantics.
//
// These exist as insurance for the LocalState/DeltaState decoupling refactor:
// the files under testdata/ are serialized state streams produced by the
// current implementation, and the tests below assert both directions against
// them:
//
//   - serialize: rebuilding the same logical content must produce
//     byte-identical streams (TestGoldenSerialize);
//   - parse/merge: ingesting the frozen bytes into a fresh aggregate must
//     produce exactly the same queryable content (TestGoldenMerge*).
//
// The package is an external test package (state_test) on purpose: it only
// goes through public API and the real caches (SQLState for the aggregate,
// ScanCache for delta construction), never the mock, so it should survive the
// refactor with minimal churn. The derived state's concrete type is never
// named; only `Derive` and the state.FromStream call below are expected to
// need a mechanical rename.
//
// Fixtures intentionally contain no ET_CONFIGURATION entry in the streams that
// get merged into the sqlite aggregate: SQLState.GetConfiguration returns
// sql.ErrNoRows for a missing key and PutConfiguration is a plain INSERT, so
// merging configuration entries into the aggregate fails today. The
// configuration wire format is still pinned through full-v110.state, which is
// byte-compared and loaded into a ScanCache only.
//
// Regenerate with:
//
//	go test ./repository/state -run TestGoldenSerialize -update
//
// Only regenerate on purpose: a diff in testdata/ means the wire format
// changed.
package state_test

import (
	"bytes"
	"encoding/binary"
	"flag"
	"iter"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "regenerate golden state fixtures")

func filledMAC(b byte) objects.MAC {
	var m objects.MAC
	for i := range m {
		m[i] = b
	}
	return m
}

// Fixed identities so the fixtures are fully deterministic.
var (
	gState1 = filledMAC(0xA1) // backup
	gState2 = filledMAC(0xA2) // rm
	gState3 = filledMAC(0xA3) // maintenance

	gSnap1       = filledMAC(0x51)
	gSnap2       = filledMAC(0x52)
	gChunk1      = filledMAC(0xC1)
	gChunk2      = filledMAC(0xC2)
	gChunkOrphan = filledMAC(0xC0)
	gObj1        = filledMAC(0x0B)

	gPack1       = filledMAC(0xB1)
	gPack2       = filledMAC(0xB2)
	gPack3       = filledMAC(0xB3)
	gPackMissing = filledMAC(0xBF)

	// full-v110.state only
	gStateFull = filledMAC(0xA9)
	gSnapX     = filledMAC(0x5F)
	gObjF      = filledMAC(0x0F)
	gPackF     = filledMAC(0xBE)
	gChunkY    = filledMAC(0xCF)
	gPackZ     = filledMAC(0xBD)

	gSerial = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

	gT0 = time.Unix(0, 1700000000000000000).UTC()
	gT1 = time.Unix(0, 1700000001000000000).UTC()
	gT2 = time.Unix(0, 1700000002000000000).UTC()
	gT3 = time.Unix(0, 1700000003000000000).UTC()

	gBlobVersion = versioning.FromString("1.0.0")
	gV110        = versioning.FromString("1.1.0")
	gV100        = versioning.FromString("1.0.0")
)

func newScanCache(t *testing.T) *caching.ScanCache {
	t.Helper()

	man := caching.NewManager(pebble.Constructor(t.TempDir()))
	t.Cleanup(func() { man.Close() })

	sc, err := man.Scan(objects.RandomMAC())
	require.NoError(t, err)
	t.Cleanup(func() { sc.Close() })

	return sc
}

func newLocalState(t *testing.T) (*state.LocalState, *caching.SQLState) {
	t.Helper()

	cache, err := caching.NewSQLState(t.TempDir(), false)
	require.NoError(t, err)

	ls, err := state.NewLocalState(cache)
	require.NoError(t, err)

	return ls, cache
}

// listObjectsOfType reproduces the aggregate's removed ListObjectsOfType on
// top of the public cache API: deltas of a type whose packfile is present.
// The HasPackfile filtering is part of what the golden fixtures pin
// (gChunkOrphan must stay excluded).
func listObjectsOfType(t *testing.T, cache *caching.SQLState, typ resources.Type) []state.DeltaEntry {
	t.Helper()

	var out []state.DeltaEntry
	for _, buf := range cache.GetDeltasByType(typ) {
		de, err := state.DeltaEntryFromBytes(buf)
		require.NoError(t, err)

		ok, err := cache.HasPackfile(de.Location.Packfile)
		require.NoError(t, err)
		if !ok {
			continue
		}

		out = append(out, de)
	}
	return out
}

// Backup-like state: delta entries plus their packfiles. gChunkOrphan
// deliberately points to a packfile absent from the state to pin the
// HasPackfile filtering in the List* methods.
func backupDeltas() []state.DeltaEntry {
	return []state.DeltaEntry{
		{Type: resources.RT_SNAPSHOT, Version: gBlobVersion, Blob: gSnap1, Location: state.Location{Packfile: gPack1, Offset: 0, Length: 100}},
		{Type: resources.RT_SNAPSHOT, Version: gBlobVersion, Blob: gSnap2, Location: state.Location{Packfile: gPack2, Offset: 0, Length: 120}},
		{Type: resources.RT_CHUNK, Version: gBlobVersion, Blob: gChunk1, Location: state.Location{Packfile: gPack1, Offset: 100, Length: 50}, Flags: 7},
		{Type: resources.RT_CHUNK, Version: gBlobVersion, Blob: gChunk2, Location: state.Location{Packfile: gPack2, Offset: 120, Length: 60}},
		{Type: resources.RT_OBJECT, Version: gBlobVersion, Blob: gObj1, Location: state.Location{Packfile: gPack3, Offset: 0, Length: 70}},
		{Type: resources.RT_CHUNK, Version: gBlobVersion, Blob: gChunkOrphan, Location: state.Location{Packfile: gPackMissing, Offset: 0, Length: 10}},
	}
}

func buildBackupState(t *testing.T, version versioning.Version) []byte {
	t.Helper()

	sc := newScanCache(t)
	base, _ := newLocalState(t)
	ds := base.Derive(sc)
	ds.Metadata = state.Metadata{
		Parent:    objects.NilMac,
		Version:   version,
		Timestamp: gT0,
		Serial:    gSerial,
	}

	for _, de := range backupDeltas() {
		require.NoError(t, ds.PutDelta(&de))
	}

	for _, pf := range []objects.MAC{gPack1, gPack2, gPack3} {
		pe := state.PackfileEntry{Packfile: pf, StateID: gState1, Timestamp: gT0}
		require.NoError(t, sc.PutPackfile(pe.Packfile, pe.ToBytes()))
	}

	var buf bytes.Buffer
	require.NoError(t, ds.SerializeToStream(&buf))
	return buf.Bytes()
}

// rm-like state: colours a snapshot and a packfile, no other entries.
func buildRmState(t *testing.T) []byte {
	t.Helper()

	sc := newScanCache(t)
	base, _ := newLocalState(t)
	ds := base.Derive(sc)
	ds.Metadata = state.Metadata{
		Parent:    gState1,
		Version:   gV110,
		Timestamp: gT1,
		Serial:    gSerial,
	}

	for _, ce := range []state.ColouredEntry{
		{Type: resources.RT_SNAPSHOT, Blob: gSnap2, When: gT1},
		{Type: resources.RT_PACKFILE, Blob: gPack2, When: gT1},
	} {
		require.NoError(t, sc.PutColoured(ce.Type, ce.Blob, ce.ToBytes()))
	}

	var buf bytes.Buffer
	require.NoError(t, ds.SerializeToStream(&buf))
	return buf.Bytes()
}

// maintenance-like state: only ET_DELETE tombstones, one of each kind.
func buildMaintenanceState(t *testing.T) []byte {
	t.Helper()

	sc := newScanCache(t)
	base, _ := newLocalState(t)
	ds := base.Derive(sc)
	ds.Metadata = state.Metadata{
		Parent:    gState2,
		Version:   gV110,
		Timestamp: gT2,
		Serial:    gSerial,
	}

	require.NoError(t, ds.DelDelta(resources.RT_CHUNK, gChunk2, gPack2))
	require.NoError(t, ds.DelPackfile(gPack2))
	require.NoError(t, ds.DelColouredResource(resources.RT_PACKFILE, gPack2))

	var buf bytes.Buffer
	require.NoError(t, ds.SerializeToStream(&buf))
	return buf.Bytes()
}

// Kitchen-sink state carrying every entry type, including configuration.
// Byte-compared and loaded into a ScanCache only (see the package comment for
// why it is never merged into the sqlite aggregate).
func buildFullState(t *testing.T) []byte {
	t.Helper()

	sc := newScanCache(t)
	base, _ := newLocalState(t)
	ds := base.Derive(sc)
	ds.Metadata = state.Metadata{
		Parent:    gState3,
		Version:   gV110,
		Timestamp: gT3,
		Serial:    gSerial,
	}

	require.NoError(t, ds.PutDelta(&state.DeltaEntry{
		Type: resources.RT_OBJECT, Version: gBlobVersion, Blob: gObjF,
		Location: state.Location{Packfile: gPackF, Offset: 0, Length: 40},
	}))

	pe := state.PackfileEntry{Packfile: gPackF, StateID: gStateFull, Timestamp: gT3}
	require.NoError(t, sc.PutPackfile(pe.Packfile, pe.ToBytes()))

	ce := state.ColouredEntry{Type: resources.RT_SNAPSHOT, Blob: gSnapX, When: gT3}
	require.NoError(t, sc.PutColoured(ce.Type, ce.Blob, ce.ToBytes()))

	require.NoError(t, ds.DelDelta(resources.RT_CHUNK, gChunkY, gPackZ))

	cfg := state.ConfigurationEntry{Key: "fixture-key", Value: []byte("fixture-value"), CreatedAt: gT3}
	require.NoError(t, sc.PutConfiguration(cfg.Key, cfg.ToBytes()))

	var buf bytes.Buffer
	require.NoError(t, ds.SerializeToStream(&buf))
	return buf.Bytes()
}

// The v1.0.0 stream layout is the v1.1.0 one without the leading 32-byte
// parent header (and without ET_DELETE entries, which the backup fixture does
// not contain).
func buildBackupStateV100(t *testing.T) []byte {
	t.Helper()
	return buildBackupState(t, gV100)[len(objects.MAC{}):]
}

// The backup stream with an entry of an unknown type spliced in right after
// the header: parsers must skip it and produce identical results.
func buildBackupStateUnknownEntry(t *testing.T) []byte {
	t.Helper()

	full := buildBackupState(t, gV110)
	hdrLen := len(objects.MAC{})

	garbage := []byte("garbage")
	unknown := []byte{42}
	unknown = binary.LittleEndian.AppendUint32(unknown, uint32(len(garbage)))
	unknown = append(unknown, garbage...)

	spliced := make([]byte, 0, len(full)+len(unknown))
	spliced = append(spliced, full[:hdrLen]...)
	spliced = append(spliced, unknown...)
	spliced = append(spliced, full[hdrLen:]...)
	return spliced
}

func fixturePath(name string) string {
	return filepath.Join("testdata", name)
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()

	data, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatalf("missing golden fixture %s (regenerate with `go test -run TestGoldenSerialize -update`): %v", name, err)
	}
	return data
}

func TestGoldenSerialize(t *testing.T) {
	fixtures := []struct {
		name  string
		build func(*testing.T) []byte
	}{
		{"backup-v110.state", func(t *testing.T) []byte { return buildBackupState(t, gV110) }},
		{"rm-v110.state", buildRmState},
		{"maintenance-v110.state", buildMaintenanceState},
		{"full-v110.state", buildFullState},
		{"backup-v100.state", buildBackupStateV100},
		{"backup-unknown-entry-v110.state", buildBackupStateUnknownEntry},
	}

	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0755))
	}

	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			got := fixture.build(t)

			if *update {
				require.NoError(t, os.WriteFile(fixturePath(fixture.name), got, 0644))
				return
			}

			require.Equal(t, readFixture(t, fixture.name), got,
				"serialized stream differs from golden fixture %s", fixture.name)
		})
	}
}

func collect2[V any](t *testing.T, it iter.Seq2[V, error]) []V {
	t.Helper()

	var out []V
	for v, err := range it {
		require.NoError(t, err)
		out = append(out, v)
	}
	return out
}

func requireMetadata(t *testing.T, got state.Metadata, parent objects.MAC, version versioning.Version, timestamp time.Time, serial uuid.UUID) {
	t.Helper()

	require.Equal(t, parent, got.Parent)
	require.Equal(t, version, got.Version)
	require.Equal(t, timestamp.UnixNano(), got.Timestamp.UnixNano())
	require.Equal(t, serial, got.Serial)
}

// assertBackupMerged checks the aggregate content after ingesting the backup
// fixture (regardless of the stream version it came from).
func assertBackupMerged(t *testing.T, ls *state.LocalState, cache *caching.SQLState) {
	t.Helper()

	has, err := cache.HasState(gState1)
	require.NoError(t, err)
	require.True(t, has)

	pfes := collect2(t, ls.ListPackfileEntries())
	require.Len(t, pfes, 3)
	seenPacks := make(map[objects.MAC]bool)
	for _, pe := range pfes {
		require.Equal(t, gState1, pe.StateID)
		require.Equal(t, gT0.UnixNano(), pe.Timestamp.UnixNano())
		seenPacks[pe.Packfile] = true
	}
	require.Equal(t, map[objects.MAC]bool{gPack1: true, gPack2: true, gPack3: true}, seenPacks)

	chunks := listObjectsOfType(t, cache, resources.RT_CHUNK)
	require.ElementsMatch(t,
		[]objects.MAC{gChunk1, gChunk2},
		[]objects.MAC{chunks[0].Blob, chunks[1].Blob})
	for _, de := range chunks {
		if de.Blob == gChunk1 {
			require.Equal(t, state.Location{Packfile: gPack1, Offset: 100, Length: 50}, de.Location)
			require.Equal(t, uint32(7), de.Flags)
			require.Equal(t, gBlobVersion, de.Version)
		}
	}

	snapshots := collect2(t, ls.ListSnapshots())
	require.ElementsMatch(t, []objects.MAC{gSnap1, gSnap2}, snapshots)

	orphans := collect2(t, ls.ListOrphanDeltas())
	require.Len(t, orphans, 1)
	require.Equal(t, gChunkOrphan, orphans[0].Blob)

	require.True(t, ls.BlobExists(resources.RT_CHUNK, gChunk1))
	require.False(t, ls.BlobExists(resources.RT_CHUNK, gChunkOrphan))
	require.False(t, ls.BlobExists(resources.RT_CHUNK, filledMAC(0xEE)))

	loc, exists, err := ls.GetSubpartForBlob(resources.RT_CHUNK, gChunk1)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, state.Location{Packfile: gPack1, Offset: 100, Length: 50}, loc)
}

func TestGoldenMergeBackup(t *testing.T) {
	data := readFixture(t, "backup-v110.state")

	ls, cache := newLocalState(t)
	require.NoError(t, ls.MergeState(gState1, bytes.NewReader(data), gV110))

	// Merging a state adopts the stream's metadata as the aggregate's.
	requireMetadata(t, ls.Metadata, objects.NilMac, gV110, gT0, gSerial)

	// The published state's stored metadata matches the stream's too.
	states, err := ls.GetStates()
	require.NoError(t, err)
	require.Len(t, states, 1)
	mt, err := state.MetadataFromBytes(states[gState1])
	require.NoError(t, err)
	requireMetadata(t, *mt, objects.NilMac, gV110, gT0, gSerial)

	assertBackupMerged(t, ls, cache)

	// Re-merging an already-known state is a no-op, not an error.
	require.NoError(t, ls.MergeState(gState1, bytes.NewReader(data), gV110))
	states, err = ls.GetStates()
	require.NoError(t, err)
	require.Len(t, states, 1)
	assertBackupMerged(t, ls, cache)
}

func TestGoldenMergeBackupV100(t *testing.T) {
	data := readFixture(t, "backup-v100.state")

	ls, cache := newLocalState(t)
	require.NoError(t, ls.MergeState(gState1, bytes.NewReader(data), gV100))

	// v1.0.0 streams carry no parent header: the aggregate's parent is
	// left untouched (NilMac on a fresh state).
	requireMetadata(t, ls.Metadata, objects.NilMac, gV100, gT0, gSerial)

	assertBackupMerged(t, ls, cache)
}

func TestGoldenMergeBackupUnknownEntry(t *testing.T) {
	data := readFixture(t, "backup-unknown-entry-v110.state")

	ls, cache := newLocalState(t)
	require.NoError(t, ls.MergeState(gState1, bytes.NewReader(data), gV110))

	// The unknown entry is skipped; everything else merges as usual.
	requireMetadata(t, ls.Metadata, objects.NilMac, gV110, gT0, gSerial)
	assertBackupMerged(t, ls, cache)
}

// TestGoldenMergeLifecycle replays a full backup -> rm -> maintenance chain
// and pins the aggregate's view after each stage, including the ET_DELETE
// application and the packfile-deletion cascade over its deltas.
func TestGoldenMergeLifecycle(t *testing.T) {
	ls, cache := newLocalState(t)

	require.NoError(t, ls.MergeState(gState1, bytes.NewReader(readFixture(t, "backup-v110.state")), gV110))
	require.NoError(t, ls.MergeState(gState2, bytes.NewReader(readFixture(t, "rm-v110.state")), gV110))

	// After rm: snap2 and pack2 are coloured but still present.
	requireMetadata(t, ls.Metadata, gState1, gV110, gT1, gSerial)

	snapshots := collect2(t, ls.ListSnapshots())
	require.Equal(t, []objects.MAC{gSnap1}, snapshots)

	coloured, err := ls.HasColouredResource(resources.RT_PACKFILE, gPack2)
	require.NoError(t, err)
	require.True(t, coloured)

	colouredSnaps := collect2(t, ls.ListColouredResources(resources.RT_SNAPSHOT))
	require.Len(t, colouredSnaps, 1)
	require.Equal(t, gSnap2, colouredSnaps[0].Blob)
	require.Equal(t, gT1.UnixNano(), colouredSnaps[0].When.UnixNano())

	// A blob whose only packfile is coloured no longer "exists".
	require.False(t, ls.BlobExists(resources.RT_CHUNK, gChunk2))
	require.True(t, ls.BlobExists(resources.RT_CHUNK, gChunk1))
	_, exists, err := ls.GetSubpartForBlob(resources.RT_CHUNK, gChunk2)
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, ls.MergeState(gState3, bytes.NewReader(readFixture(t, "maintenance-v110.state")), gV110))

	// After maintenance: pack2 and its deltas are gone, its colour is lifted.
	requireMetadata(t, ls.Metadata, gState2, gV110, gT2, gSerial)

	coloured, err = ls.HasColouredResource(resources.RT_PACKFILE, gPack2)
	require.NoError(t, err)
	require.False(t, coloured)

	// snap2 itself stays coloured; only the packfile was uncoloured.
	coloured, err = ls.HasColouredResource(resources.RT_SNAPSHOT, gSnap2)
	require.NoError(t, err)
	require.True(t, coloured)

	var packs []objects.MAC
	for pf := range ls.ListPackfiles() {
		packs = append(packs, pf)
	}
	require.ElementsMatch(t, []objects.MAC{gPack1, gPack3}, packs)

	// Deleting a packfile cascades over its remaining deltas: snap2's delta
	// lived in pack2 and must be gone entirely (not even orphaned).
	chunks := listObjectsOfType(t, cache, resources.RT_CHUNK)
	require.Len(t, chunks, 1)
	require.Equal(t, gChunk1, chunks[0].Blob)

	_, exists, err = ls.GetSubpartForBlob(resources.RT_SNAPSHOT, gSnap2)
	require.NoError(t, err)
	require.False(t, exists)

	orphans := collect2(t, ls.ListOrphanDeltas())
	require.Len(t, orphans, 1)
	require.Equal(t, gChunkOrphan, orphans[0].Blob)

	// All three states are published with their own stream metadata.
	states, err := ls.GetStates()
	require.NoError(t, err)
	require.Len(t, states, 3)
	for stateID, want := range map[objects.MAC]struct {
		parent    objects.MAC
		timestamp time.Time
	}{
		gState1: {objects.NilMac, gT0},
		gState2: {gState1, gT1},
		gState3: {gState2, gT2},
	} {
		mt, err := state.MetadataFromBytes(states[stateID])
		require.NoError(t, err)
		requireMetadata(t, *mt, want.parent, gV110, want.timestamp, gSerial)
	}

	// UpdateSerialOr keeps the serial of the latest published state and
	// ignores the fallback when states exist.
	require.NoError(t, ls.UpdateSerialOr(uuid.New()))
	require.Equal(t, gSerial, ls.Metadata.Serial)
}

func TestGoldenReadHeader(t *testing.T) {
	data := readFixture(t, "rm-v110.state")

	hdr, err := state.ReadHeader(bytes.NewReader(data), gV110)
	require.NoError(t, err)
	require.Equal(t, gState1, hdr.Parent)
}

// TestGoldenLoadFull loads the kitchen-sink fixture into a scratch ScanCache
// (the diag path) and pins what a loaded unitary state exposes, including the
// configuration entry the aggregate cannot ingest today.
func TestGoldenLoadFull(t *testing.T) {
	// TODO: re-enable when FromStream comes back on the State type (it is
	// commented out in state.go pending the loading-path rework).
	t.Skip("state.FromStream is pending rework")

	/*
			data := readFixture(t, "full-v110.state")

			sc := newScanCache(t)
			st, err := state.FromStream(bytes.NewReader(data), gV110, sc)
			require.NoError(t, err)

		requireMetadata(t, st.Metadata, gState3, gV110, gT3, gSerial)

		objs := collect2(t, st.ListObjectsOfType(resources.RT_OBJECT))
		require.Len(t, objs, 1)
		require.Equal(t, gObjF, objs[0].Blob)
		require.Equal(t, state.Location{Packfile: gPackF, Offset: 0, Length: 40}, objs[0].Location)

		colouredSnaps := collect2(t, st.ListColouredResources(resources.RT_SNAPSHOT))
		require.Len(t, colouredSnaps, 1)
		require.Equal(t, gSnapX, colouredSnaps[0].Blob)
		require.Equal(t, gT3.UnixNano(), colouredSnaps[0].When.UnixNano())

		require.True(t, st.BlobExists(resources.RT_OBJECT, gObjF))

		raw, err := sc.GetConfiguration("fixture-key")
		require.NoError(t, err)
		cfg, err := state.ConfigurationEntryFromBytes(raw)
		require.NoError(t, err)
			require.Equal(t, "fixture-key", cfg.Key)
			require.Equal(t, []byte("fixture-value"), cfg.Value)
			require.Equal(t, gT3.UnixNano(), cfg.CreatedAt.UnixNano())
	*/
}
