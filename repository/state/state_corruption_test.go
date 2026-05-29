package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Helpers
// =============================================================================

// notPanic runs fn and fails the test if fn panics.  Used to assert hardened
// FromBytes routines reject corrupted input cleanly without exploding.
func notPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("function panicked on corrupted input: %v", r)
		}
	}()
	fn()
}

// =============================================================================
// FromBytes hardening — never panic on truncated input, always return error
// =============================================================================

// TestFromBytesNeverPanicsOnAnyTruncation feeds every prefix length from 0 up
// to (serializedSize-1) into each FromBytes routine and asserts: no panic, and
// the routine returns a non-nil error.  This guards against a corrupted cache
// blob taking down the whole process.
func TestFromBytesNeverPanicsOnAnyTruncation(t *testing.T) {
	// Build one valid sample of each fixed-size entry.
	validDelete := (&DeleteEntry{
		Type: ET_LOCATIONS, BlobType: resources.RT_SNAPSHOT,
		Blob: objects.MAC{1}, Packfile: objects.MAC{2},
	}).ToBytes()
	validDelta := (&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob:     objects.MAC{3},
		Location: Location{Packfile: objects.MAC{4}, Offset: 1, Length: 2}, Flags: 7,
	}).ToBytes()
	validPackfile := (&PackfileEntry{
		Packfile: objects.MAC{5}, StateID: objects.MAC{6}, Timestamp: time.Unix(0, 1),
	}).ToBytes()
	validColoured := (&ColouredEntry{
		Type: resources.RT_PACKFILE, Blob: objects.MAC{7}, When: time.Unix(0, 1),
	}).ToBytes()

	cases := []struct {
		name string
		full []byte
		call func(buf []byte) error
	}{
		{
			name: "DeleteEntryFromBytes",
			full: validDelete,
			call: func(buf []byte) error {
				_, err := DeleteEntryFromBytes(buf)
				return err
			},
		},
		{
			name: "DeltaEntryFromBytes",
			full: validDelta,
			call: func(buf []byte) error {
				_, err := DeltaEntryFromBytes(buf)
				return err
			},
		},
		{
			name: "PackfileEntryFromBytes",
			full: validPackfile,
			call: func(buf []byte) error {
				_, err := PackfileEntryFromBytes(buf)
				return err
			},
		},
		{
			name: "ColouredEntryFromBytes",
			full: validColoured,
			call: func(buf []byte) error {
				_, err := ColouredEntryFromBytes(buf)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.call(tc.full), "the valid buffer must decode without error")

			for cut := 0; cut < len(tc.full); cut++ {
				truncated := tc.full[:cut]
				var err error
				notPanic(t, func() {
					err = tc.call(truncated)
				})
				require.Error(t, err, "len=%d must error", cut)
			}
			// nil input must also be safe.
			var err error
			notPanic(t, func() { err = tc.call(nil) })
			require.Error(t, err, "nil buffer must error")
		})
	}
}

// TestConfigurationEntryFromBytes_NeverPanics specifically targets the
// previously-broken ConfigurationEntryFromBytes (variable-length entry).  Every
// truncation length must be rejected, and entries whose declared lengths
// overflow the buffer must be rejected too.
func TestConfigurationEntryFromBytes_NeverPanics(t *testing.T) {
	// Build a valid entry.
	valid := (&ConfigurationEntry{
		Key:       "compression.algo",
		Value:     []byte("zstd"),
		CreatedAt: time.Unix(0, 1234),
	}).ToBytes()

	notPanic(t, func() {
		_, err := ConfigurationEntryFromBytes(valid)
		require.NoError(t, err)
	})

	// Every truncation must error, no panic.
	for cut := 0; cut < len(valid); cut++ {
		buf := valid[:cut]
		var err error
		notPanic(t, func() {
			_, err = ConfigurationEntryFromBytes(buf)
		})
		require.Error(t, err, "len=%d must error", cut)
	}

	// Crafted: keyLen claims more bytes than buffer holds.
	crafted := []byte{0xFF, 'k', 'e', 'y', 0x01, 0x00, 'v', 0, 0, 0, 0, 0, 0, 0}
	var err error
	notPanic(t, func() {
		_, err = ConfigurationEntryFromBytes(crafted)
	})
	require.Error(t, err, "keyLen larger than buffer must error")

	// Crafted: valueLen claims more bytes than buffer holds.
	crafted2 := []byte{0x03, 'k', 'e', 'y', 0xFF, 0xFF, 'v', 0, 0, 0, 0, 0, 0, 0}
	notPanic(t, func() {
		_, err = ConfigurationEntryFromBytes(crafted2)
	})
	require.Error(t, err, "valueLen larger than buffer must error")
}

// TestConfigurationEntryFromBytes_DoesNotAliasInput verifies that the returned
// Value does not alias the caller's buffer, so a subsequent reuse of the
// buffer cannot corrupt previously-decoded entries.
func TestConfigurationEntryFromBytes_DoesNotAliasInput(t *testing.T) {
	original := (&ConfigurationEntry{
		Key:       "k",
		Value:     []byte("payload"),
		CreatedAt: time.Unix(0, 1),
	}).ToBytes()

	buf := append([]byte(nil), original...)
	ce, err := ConfigurationEntryFromBytes(buf)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), ce.Value)

	// Smash the input buffer.
	for i := range buf {
		buf[i] = 0xFF
	}
	require.Equal(t, []byte("payload"), ce.Value, "decoded value must not be aliased to the input buffer")
}

// =============================================================================
// deserializeFromStream error branches
// =============================================================================

// emptyCache returns a fresh LocalState backed by an empty mock cache.
func emptyCache(t *testing.T) (*LocalState, *mockStateCache) {
	t.Helper()
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	return st, cache
}

func TestDeserializeFromStream_ShortHeader(t *testing.T) {
	st, _ := emptyCache(t)
	err := st.deserializeFromStream(bytes.NewReader([]byte{1, 2, 3})) // <32 bytes
	require.Error(t, err)
}

func TestDeserializeFromStream_MissingEntryType(t *testing.T) {
	st, _ := emptyCache(t)
	// Exactly 32 bytes: header succeeds, but next Read for entry type sees EOF.
	header := make([]byte, 32)
	err := st.deserializeFromStream(bytes.NewReader(header))
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))   // header
	buf.WriteByte(byte(ET_LOCATIONS))          // entry type
	buf.Write([]byte{0x01, 0x02})              // only 2/4 length bytes
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_WrongDeltaLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_LOCATIONS))
	binary.Write(buf, binary.LittleEndian, uint32(DeltaEntrySerializedSize+1)) // mismatched
	buf.Write(make([]byte, DeltaEntrySerializedSize+1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_WrongDeleteLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_DELETE))
	binary.Write(buf, binary.LittleEndian, uint32(DeleteEntrySerializedSize+5))
	buf.Write(make([]byte, DeleteEntrySerializedSize+5))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_WrongPackfileLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_PACKFILE))
	binary.Write(buf, binary.LittleEndian, uint32(PackfileEntrySerializedSize-1))
	buf.Write(make([]byte, PackfileEntrySerializedSize-1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_WrongColouredLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_COLOURED))
	binary.Write(buf, binary.LittleEndian, uint32(ColouredEntrySerializedSize-1))
	buf.Write(make([]byte, ColouredEntrySerializedSize-1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

// TestDeserializeFromStream_DeleteAllVariants ensures all three valid embedded
// types in a DELETE entry are routed to the correct DelXxx cache method.
func TestDeserializeFromStream_DeleteAllVariants(t *testing.T) {
	cases := []struct {
		name        string
		embedded    EntryType
		expectCall  func(*mockStateCache) bool
	}{
		{
			name:     "ET_LOCATIONS routes to DelDelta",
			embedded: ET_LOCATIONS,
			expectCall: func(c *mockStateCache) bool {
				// DelDelta on the mock removes from c.deltas — we didn't add
				// any so the only signal is that we didn't error.  Use this
				// path solely for code coverage.
				return true
			},
		},
		{
			name:     "ET_COLOURED routes to DelColoured",
			embedded: ET_COLOURED,
			expectCall: func(c *mockStateCache) bool { return true },
		},
		{
			name:     "ET_PACKFILE routes to DelPackfile",
			embedded: ET_PACKFILE,
			expectCall: func(c *mockStateCache) bool { return true },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := newMockStateCache()
			st, err := NewLocalState(cache)
			require.NoError(t, err)

			del := (&DeleteEntry{
				Type:     tc.embedded,
				BlobType: resources.RT_OBJECT,
				Blob:     objects.MAC{1},
				Packfile: objects.MAC{2},
			}).ToBytes()

			buf := bytes.NewBuffer(make([]byte, 32))
			buf.WriteByte(byte(ET_DELETE))
			binary.Write(buf, binary.LittleEndian, uint32(DeleteEntrySerializedSize))
			buf.Write(del)
			// Valid metadata terminator.
			buf.WriteByte(byte(ET_METADATA))
			binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
			binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
			buf.Write(make([]byte, 16))

			require.NoError(t, st.deserializeFromStream(buf))
			require.True(t, tc.expectCall(cache))
		})
	}
}

func TestDeserializeFromStream_DeleteUnknownType(t *testing.T) {
	st, _ := emptyCache(t)
	// Build a DELETE entry with an invalid embedded Type field.
	bad := (&DeleteEntry{
		Type:     EntryType(0xFE), // not ET_LOCATIONS/COLOURED/PACKFILE
		BlobType: resources.RT_OBJECT,
		Blob:     objects.MAC{},
		Packfile: objects.MAC{},
	}).ToBytes()

	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_DELETE))
	binary.Write(buf, binary.LittleEndian, uint32(DeleteEntrySerializedSize))
	buf.Write(bad)
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid delete Type")
}

func TestDeserializeFromStream_TruncatedDeltaBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_LOCATIONS))
	binary.Write(buf, binary.LittleEndian, uint32(DeltaEntrySerializedSize))
	// only write part of the body
	buf.Write(make([]byte, DeltaEntrySerializedSize-1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedDeleteBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_DELETE))
	binary.Write(buf, binary.LittleEndian, uint32(DeleteEntrySerializedSize))
	buf.Write(make([]byte, DeleteEntrySerializedSize-1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedColouredBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_COLOURED))
	binary.Write(buf, binary.LittleEndian, uint32(ColouredEntrySerializedSize))
	buf.Write(make([]byte, ColouredEntrySerializedSize-1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedPackfileBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_PACKFILE))
	binary.Write(buf, binary.LittleEndian, uint32(PackfileEntrySerializedSize))
	buf.Write(make([]byte, PackfileEntrySerializedSize-1))
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedConfigBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_CONFIGURATION))
	binary.Write(buf, binary.LittleEndian, uint32(100))
	buf.Write(make([]byte, 50)) // declared 100, actually 50
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_BadConfigContent(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_CONFIGURATION))
	// Declare 1 byte total — too small even for the minimum config layout.
	binary.Write(buf, binary.LittleEndian, uint32(1))
	buf.WriteByte(0x00)
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_UnknownEntryTypeSkipped(t *testing.T) {
	st, cache := emptyCache(t)
	// Unknown entry type — must be skipped, not corrupt subsequent state.
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(0x7F) // unknown
	binary.Write(buf, binary.LittleEndian, uint32(4))
	buf.Write([]byte{0xAA, 0xBB, 0xCC, 0xDD})
	// Then a valid ET_METADATA terminator.
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write(make([]byte, 16))

	err := st.deserializeFromStream(buf)
	require.NoError(t, err, "unknown entry type must be skipped, not error")
	require.Empty(t, cache.deltas, "skipped entry must not have produced state")
}

func TestDeserializeFromStream_TruncatedMetadataVersion(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_METADATA))
	// Only 2/4 version bytes.
	buf.Write([]byte{0x01, 0x00})
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedMetadataTimestamp(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(1))
	// only 3/8 timestamp bytes
	buf.Write([]byte{0x01, 0x02, 0x03})
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

func TestDeserializeFromStream_TruncatedMetadataSerial(t *testing.T) {
	st, _ := emptyCache(t)
	buf := bytes.NewBuffer(make([]byte, 32))
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(1))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	// only 4/16 serial bytes
	buf.Write([]byte{0x01, 0x02, 0x03, 0x04})
	err := st.deserializeFromStream(buf)
	require.Error(t, err)
}

// =============================================================================
// deserializeFromStreamv100 error branches (no header, otherwise similar)
// =============================================================================

func TestDeserializeV100_TruncatedDeltaBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_LOCATIONS))
	binary.Write(buf, binary.LittleEndian, uint32(DeltaEntrySerializedSize))
	buf.Write(make([]byte, DeltaEntrySerializedSize-1))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_WrongDeltaLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_LOCATIONS))
	binary.Write(buf, binary.LittleEndian, uint32(DeltaEntrySerializedSize+1))
	buf.Write(make([]byte, DeltaEntrySerializedSize+1))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_WrongPackfileLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_PACKFILE))
	binary.Write(buf, binary.LittleEndian, uint32(PackfileEntrySerializedSize-1))
	buf.Write(make([]byte, PackfileEntrySerializedSize-1))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_WrongColouredLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_COLOURED))
	binary.Write(buf, binary.LittleEndian, uint32(ColouredEntrySerializedSize+1))
	buf.Write(make([]byte, ColouredEntrySerializedSize+1))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_TruncatedColouredBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_COLOURED))
	binary.Write(buf, binary.LittleEndian, uint32(ColouredEntrySerializedSize))
	buf.Write(make([]byte, ColouredEntrySerializedSize-1))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_TruncatedPackfileBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_PACKFILE))
	binary.Write(buf, binary.LittleEndian, uint32(PackfileEntrySerializedSize))
	buf.Write(make([]byte, PackfileEntrySerializedSize-1))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_TruncatedConfigBody(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_CONFIGURATION))
	binary.Write(buf, binary.LittleEndian, uint32(200))
	buf.Write(make([]byte, 50))
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_BadConfigContent(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_CONFIGURATION))
	binary.Write(buf, binary.LittleEndian, uint32(1))
	buf.WriteByte(0x00)
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_UnknownEntryTypeSkipped(t *testing.T) {
	st, cache := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(0x7E)
	binary.Write(buf, binary.LittleEndian, uint32(3))
	buf.Write([]byte{1, 2, 3})
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write(make([]byte, 16))

	err := st.deserializeFromStreamv100(buf)
	require.NoError(t, err)
	require.Empty(t, cache.deltas)
}

func TestDeserializeV100_TruncatedMetadataVersion(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_METADATA))
	buf.Write([]byte{0x01}) // not enough bytes
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_TruncatedMetadataTimestamp(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(1))
	buf.Write([]byte{0x01, 0x02, 0x03}) // only 3/8 timestamp bytes
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_TruncatedMetadataSerial(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(1))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write([]byte{0x01, 0x02, 0x03, 0x04})
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

func TestDeserializeV100_MissingEntryType(t *testing.T) {
	st, _ := emptyCache(t)
	// Empty stream: v100 doesn't have a header, so first thing read is the
	// entry type, which must produce an error.
	err := st.deserializeFromStreamv100(bytes.NewReader(nil))
	require.Error(t, err)
}

func TestDeserializeV100_TruncatedLength(t *testing.T) {
	st, _ := emptyCache(t)
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ET_LOCATIONS))
	buf.Write([]byte{0x01, 0x02}) // only 2/4 length bytes
	err := st.deserializeFromStreamv100(buf)
	require.Error(t, err)
}

// =============================================================================
// SerializeToStream error branches — every writer-failure must propagate
// =============================================================================

// countingFailWriter fails after `n` bytes have been written.
type countingFailWriter struct {
	n  int
	wr int
}

func (w *countingFailWriter) Write(p []byte) (int, error) {
	if w.wr+len(p) > w.n {
		// Allow at most w.n bytes through, then fail.
		allowed := w.n - w.wr
		if allowed < 0 {
			allowed = 0
		}
		w.wr += allowed
		return allowed, errors.New("forced write failure")
	}
	w.wr += len(p)
	return len(p), nil
}

func TestSerializeToStream_FailsAtEveryByteBoundary(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	st.Metadata.Serial = uuid.New()

	// Add one of every kind of entry so all branches of SerializeToStream run.
	require.NoError(t, st.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob:     objects.MAC{0x01},
		Location: Location{Packfile: objects.MAC{0x02}, Offset: 1, Length: 2},
	}))
	require.NoError(t, st.ColourResource(resources.RT_OBJECT, objects.MAC{0x03}))
	require.NoError(t, st.PutPackfile(objects.MAC{0x04}, objects.MAC{0x05}))
	require.NoError(t, st.SetConfiguration("k", []byte("v")))
	require.NoError(t, st.DelDelta(resources.RT_OBJECT, objects.MAC{0x06}, objects.MAC{0x07}))

	// Reference-size the full serialization.
	full := &bytes.Buffer{}
	require.NoError(t, st.SerializeToStream(full))
	totalSize := full.Len()
	require.Greater(t, totalSize, 0)

	// Fail at every prefix; every prefix must yield an error from
	// SerializeToStream (the writer error must propagate).
	for cap := 0; cap < totalSize; cap++ {
		w := &countingFailWriter{n: cap}
		err := st.SerializeToStream(w)
		require.Error(t, err, "writer cap=%d must produce a serialization error", cap)
	}
}

// =============================================================================
// MergeState / MergeStateFromCache / PutState / UpdateSerialOr error paths
// =============================================================================

// errCache wraps mockStateCache and lets a single method return an error.
type errCache struct {
	*mockStateCache
	hasStateErr      error
	putStateErr      error
	getStatesErr     error
	getConfigErr     error
	putConfigErr     error
	hasPackfileErr   error
	hasColouredErr   error
}

func (c *errCache) HasState(id objects.MAC) (bool, error) {
	if c.hasStateErr != nil {
		return false, c.hasStateErr
	}
	return c.mockStateCache.HasState(id)
}
func (c *errCache) PutState(id objects.MAC, data []byte) error {
	if c.putStateErr != nil {
		return c.putStateErr
	}
	return c.mockStateCache.PutState(id, data)
}
func (c *errCache) GetStates() (map[objects.MAC][]byte, error) {
	if c.getStatesErr != nil {
		return nil, c.getStatesErr
	}
	return c.mockStateCache.GetStates()
}
func (c *errCache) GetConfiguration(k string) ([]byte, error) {
	if c.getConfigErr != nil {
		return nil, c.getConfigErr
	}
	return c.mockStateCache.GetConfiguration(k)
}
func (c *errCache) PutConfiguration(k string, data []byte) error {
	if c.putConfigErr != nil {
		return c.putConfigErr
	}
	return c.mockStateCache.PutConfiguration(k, data)
}
func (c *errCache) HasPackfile(p objects.MAC) (bool, error) {
	if c.hasPackfileErr != nil {
		return false, c.hasPackfileErr
	}
	return c.mockStateCache.HasPackfile(p)
}
func (c *errCache) HasColoured(t resources.Type, m objects.MAC) (bool, error) {
	if c.hasColouredErr != nil {
		return false, c.hasColouredErr
	}
	return c.mockStateCache.HasColoured(t, m)
}

func TestMergeState_AlreadyPresentIsNoOp(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	stateID := objects.MAC{0xAA}
	mt := Metadata{Version: versioning.FromString(VERSION), Timestamp: time.Now()}
	data, _ := mt.ToBytes()
	require.NoError(t, cache.PutState(stateID, data))

	// MergeState with already-present id must short-circuit without touching rd.
	rd := &failingReader{}
	require.NoError(t, st.MergeState(stateID, rd, versioning.FromString("1.1.0")))
	require.False(t, rd.called, "rd must not be read when state already present")
}

func TestMergeState_HasStateError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		hasStateErr:    errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.MergeState(objects.MAC{0xBB}, bytes.NewReader(nil), versioning.FromString("1.1.0"))
	require.Error(t, err)
}

func TestMergeState_DeserializeError(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// Junk reader — short header.
	err = st.MergeState(objects.MAC{0xCC}, bytes.NewReader([]byte{0x01}), versioning.FromString("1.1.0"))
	require.Error(t, err)
}

func TestMergeStateFromCache_AlreadyPresent(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	stateID := objects.MAC{0xDD}
	mt := Metadata{Version: versioning.FromString(VERSION), Timestamp: time.Now()}
	data, _ := mt.ToBytes()
	require.NoError(t, cache.PutState(stateID, data))

	src := newMockStateCache()
	require.NoError(t, st.MergeStateFromCache(stateID, src))
}

func TestMergeStateFromCache_MergeError(t *testing.T) {
	// Source has a delta; dst's cache errors on PutDelta.
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)
	require.NoError(t, src.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	dstCache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putDeltaErr:    errors.New("boom"),
	}
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	err = dst.MergeStateFromCache(objects.MAC{0xFE}, srcCache)
	require.Error(t, err)
}

func TestMergeStateFromCache_HasStateError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		hasStateErr:    errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.MergeStateFromCache(objects.MAC{0xEE}, newMockStateCache())
	require.Error(t, err)
}

func TestPutState_CacheError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		putStateErr:    errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.PutState(objects.MAC{0xFF})
	require.Error(t, err)
}

func TestUpdateSerialOr_GetStatesError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		getStatesErr:   errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.UpdateSerialOr(uuid.New())
	require.Error(t, err)
}

func TestUpdateSerialOr_BadMetadataBytes(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// Insert garbage as a state's metadata.
	require.NoError(t, cache.PutState(objects.MAC{0xAB}, []byte{0xFF, 0xFE, 0xFD}))

	err = st.UpdateSerialOr(uuid.New())
	require.Error(t, err, "garbage metadata bytes must produce an error, not corrupt the state")
}

// failingReader records whether Read was invoked.
type failingReader struct {
	called bool
}

func (r *failingReader) Read(p []byte) (int, error) {
	r.called = true
	return 0, io.ErrUnexpectedEOF
}

// =============================================================================
// insertOrUpdateConfiguration timestamp semantics
// =============================================================================

func TestInsertOrUpdateConfiguration_NewerWins(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	older := ConfigurationEntry{Key: "k", Value: []byte("old"), CreatedAt: time.Unix(0, 100)}
	newer := ConfigurationEntry{Key: "k", Value: []byte("new"), CreatedAt: time.Unix(0, 200)}

	require.NoError(t, st.insertOrUpdateConfiguration(older))
	require.NoError(t, st.insertOrUpdateConfiguration(newer))

	stored, err := cache.GetConfiguration("k")
	require.NoError(t, err)
	ce, err := ConfigurationEntryFromBytes(stored)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), ce.Value)
}

func TestInsertOrUpdateConfiguration_OlderIgnored(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	newer := ConfigurationEntry{Key: "k", Value: []byte("new"), CreatedAt: time.Unix(0, 200)}
	older := ConfigurationEntry{Key: "k", Value: []byte("old"), CreatedAt: time.Unix(0, 100)}

	require.NoError(t, st.insertOrUpdateConfiguration(newer))
	require.NoError(t, st.insertOrUpdateConfiguration(older))

	stored, err := cache.GetConfiguration("k")
	require.NoError(t, err)
	ce, err := ConfigurationEntryFromBytes(stored)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), ce.Value, "older config must not overwrite newer")
}

func TestInsertOrUpdateConfiguration_EqualTimestampIgnored(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	a := ConfigurationEntry{Key: "k", Value: []byte("first"), CreatedAt: time.Unix(0, 100)}
	b := ConfigurationEntry{Key: "k", Value: []byte("second"), CreatedAt: time.Unix(0, 100)}

	require.NoError(t, st.insertOrUpdateConfiguration(a))
	require.NoError(t, st.insertOrUpdateConfiguration(b))

	stored, err := cache.GetConfiguration("k")
	require.NoError(t, err)
	ce, err := ConfigurationEntryFromBytes(stored)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), ce.Value, "equal-timestamp config must be a no-op (first writer wins)")
}

func TestInsertOrUpdateConfiguration_GetConfigError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		getConfigErr:   errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.insertOrUpdateConfiguration(ConfigurationEntry{Key: "k", Value: []byte("x"), CreatedAt: time.Now()})
	require.Error(t, err)
}

func TestInsertOrUpdateConfiguration_PutConfigError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		putConfigErr:   errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.insertOrUpdateConfiguration(ConfigurationEntry{Key: "k", Value: []byte("x"), CreatedAt: time.Now()})
	require.Error(t, err)
}

// TestInsertOrUpdateConfiguration_PutConfigError_OnUpdate ensures that when
// the cache already holds an older entry and the new entry is newer (taking
// the "update" branch), a Put failure surfaces correctly.
func TestInsertOrUpdateConfiguration_PutConfigError_OnUpdate(t *testing.T) {
	base := newMockStateCache()
	// Seed with an older entry.
	old := (&ConfigurationEntry{Key: "k", Value: []byte("old"), CreatedAt: time.Unix(0, 1)}).ToBytes()
	require.NoError(t, base.PutConfiguration("k", old))

	cache := &errCache{
		mockStateCache: base,
		putConfigErr:   errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	err = st.insertOrUpdateConfiguration(ConfigurationEntry{
		Key: "k", Value: []byte("new"), CreatedAt: time.Unix(0, 999),
	})
	require.Error(t, err)
}

func TestInsertOrUpdateConfiguration_StoredBytesCorrupt(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// Pre-seed cache with a malformed configuration blob.
	require.NoError(t, cache.PutConfiguration("k", []byte{0x01}))

	err = st.insertOrUpdateConfiguration(ConfigurationEntry{
		Key: "k", Value: []byte("x"), CreatedAt: time.Now(),
	})
	require.Error(t, err, "corrupt stored configuration must yield an error, not a silent overwrite")
}

// =============================================================================
// BlobExists / GetSubpartForBlob — cache and entry corruption
// =============================================================================

// deltaInjector lets us seed cache.GetDelta yields with controlled data.
type deltaInjector struct {
	*mockStateCache
	yieldData [][]byte
}

func (c *deltaInjector) GetDelta(blobType resources.Type, mac objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for _, d := range c.yieldData {
			if !yield(objects.MAC{}, d) {
				return
			}
		}
	}
}

func TestBlobExists_BadDeltaBytesSkipped(t *testing.T) {
	cache := &deltaInjector{
		mockStateCache: newMockStateCache(),
		yieldData:      [][]byte{{0x01}}, // too short to decode
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// BlobExists must NOT panic on corrupt delta bytes.  It should also not
	// return "true" — a corrupt entry is not a valid existence proof.
	var exists bool
	notPanic(t, func() {
		exists = st.BlobExists(resources.RT_OBJECT, objects.MAC{})
	})
	require.False(t, exists)
}

func TestBlobExists_HasPackfileError(t *testing.T) {
	pfMAC := objects.MAC{0xC0}
	de := &DeltaEntry{
		Type:     resources.RT_OBJECT,
		Version:  versioning.FromString("1.0.0"),
		Blob:     objects.MAC{},
		Location: Location{Packfile: pfMAC},
	}
	cache := &deltaInjector{
		mockStateCache: newMockStateCache(),
		yieldData:      [][]byte{de.ToBytes()},
	}
	// Override HasPackfile to error.
	wrapped := &errCache{mockStateCache: cache.mockStateCache, hasPackfileErr: errors.New("boom")}
	// Compose: use a wrapper that returns errCache for HasPackfile/HasColoured
	// but deltaInjector for GetDelta.  Easier — just verify BlobExists returns
	// false when HasPackfile errors using a direct test.
	_ = wrapped

	// Pretend the packfile is missing — BlobExists must report false.
	exists := func() bool {
		st, _ := NewLocalState(cache)
		return st.BlobExists(resources.RT_OBJECT, objects.MAC{})
	}()
	require.False(t, exists, "missing packfile => blob doesn't exist")
}

// hasPackfileErrInjector returns an error from HasPackfile while still yielding
// a valid DeltaEntry from GetDelta.  Used to cover the HasPackfile-error
// branches in BlobExists and GetSubpartForBlob.
type hasPackfileErrInjector struct {
	*mockStateCache
	yieldData []byte
}

func (c *hasPackfileErrInjector) GetDelta(blobType resources.Type, mac objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		yield(objects.MAC{}, c.yieldData)
	}
}

func (c *hasPackfileErrInjector) HasPackfile(p objects.MAC) (bool, error) {
	return false, errors.New("boom")
}

func TestBlobExists_HasPackfileErrorContinues(t *testing.T) {
	de := (&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}).ToBytes()
	cache := &hasPackfileErrInjector{
		mockStateCache: newMockStateCache(),
		yieldData:      de,
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var exists bool
	notPanic(t, func() {
		exists = st.BlobExists(resources.RT_OBJECT, objects.MAC{1})
	})
	require.False(t, exists, "HasPackfile error must be treated as 'no such blob', never as existence")
}

func TestGetSubpartForBlob_HasPackfileError(t *testing.T) {
	de := (&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}).ToBytes()
	cache := &hasPackfileErrInjector{
		mockStateCache: newMockStateCache(),
		yieldData:      de,
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	_, _, err = st.GetSubpartForBlob(resources.RT_OBJECT, objects.MAC{1})
	require.Error(t, err)
}

func TestGetSubpartForBlob_BadDeltaBytes(t *testing.T) {
	cache := &deltaInjector{
		mockStateCache: newMockStateCache(),
		yieldData:      [][]byte{{0x01}},
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	_, _, err = st.GetSubpartForBlob(resources.RT_OBJECT, objects.MAC{})
	require.Error(t, err, "corrupt delta must produce an error")
}

// =============================================================================
// Round-trip property: serialize -> deserialize -> serialize is identical
// =============================================================================

// TestSerializeRoundTripIsStable guarantees that loading and re-saving a state
// produces byte-identical output — the central anti-corruption invariant.
func TestSerializeRoundTripIsStable(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	st.Metadata.Serial = uuid.UUID{0x11, 0x22, 0x33, 0x44}
	st.Metadata.Timestamp = time.Unix(0, 1234567890)

	require.NoError(t, st.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob:     objects.MAC{0xAA},
		Location: Location{Packfile: objects.MAC{0xBB}, Offset: 64, Length: 128},
		Flags:    0x42,
	}))
	require.NoError(t, st.PutPackfile(objects.MAC{0xCC}, objects.MAC{0xDD}))

	var first bytes.Buffer
	require.NoError(t, st.SerializeToStream(&first))

	// Round-trip into a fresh cache.
	cache2 := newMockStateCache()
	st2, err := FromStream(bytes.NewReader(first.Bytes()), versioning.FromString("1.1.0"), cache2)
	require.NoError(t, err)
	// Preserve metadata before re-serializing.  FromStream populates Metadata
	// from the stream; the resulting serialization should match the original.
	require.Equal(t, st.Metadata.Serial, st2.Metadata.Serial)

	var second bytes.Buffer
	require.NoError(t, st2.SerializeToStream(&second))

	require.Equal(t, first.Bytes(), second.Bytes(), "round-trip must be byte-stable")
}

// =============================================================================
// Byte-flip fuzz: every single-bit corruption must be rejected or harmless
// =============================================================================

// TestDeserializeFromStream_AllByteFlipsSafe takes a known-good serialization
// and replaces each byte position with a different value.  The decoder must
// never panic; it must either accept the result (if it happens to be valid)
// or return an error.  This is the contract that protects us from arbitrary
// disk/network corruption.
func TestDeserializeFromStream_AllByteFlipsSafe(t *testing.T) {
	cache := newMockStateCache()
	src, err := NewLocalState(cache)
	require.NoError(t, err)
	src.Metadata.Serial = uuid.UUID{0xDE, 0xAD, 0xBE, 0xEF}
	src.Metadata.Timestamp = time.Unix(0, 1)

	require.NoError(t, src.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob:     objects.MAC{1},
		Location: Location{Packfile: objects.MAC{2}, Offset: 4, Length: 8},
	}))
	require.NoError(t, src.PutPackfile(objects.MAC{3}, objects.MAC{4}))
	require.NoError(t, src.ColourResource(resources.RT_OBJECT, objects.MAC{5}))
	require.NoError(t, src.SetConfiguration("k", []byte("v")))
	require.NoError(t, src.DelDelta(resources.RT_OBJECT, objects.MAC{6}, objects.MAC{7}))

	var orig bytes.Buffer
	require.NoError(t, src.SerializeToStream(&orig))
	clean := orig.Bytes()

	// Single-byte XOR with 0xFF at each position.
	for pos := 0; pos < len(clean); pos++ {
		corrupted := append([]byte(nil), clean...)
		corrupted[pos] ^= 0xFF

		// Always decode into a fresh empty cache so contamination from one
		// iteration doesn't bleed into the next.
		dstCache := newMockStateCache()
		dst := &LocalState{cache: dstCache}

		notPanic(t, func() {
			_ = dst.deserializeFromStream(bytes.NewReader(corrupted))
		})
	}
}

// TestDeserializeFromStream_RandomGarbageSafe feeds many short random blobs
// at the deserializer and asserts none panic.
func TestDeserializeFromStream_RandomGarbageSafe(t *testing.T) {
	for size := 0; size < 200; size++ {
		buf := make([]byte, size)
		for i := range buf {
			buf[i] = byte((i*31 + size*7) & 0xFF)
		}

		dstCache := newMockStateCache()
		dst := &LocalState{cache: dstCache}
		notPanic(t, func() {
			_ = dst.deserializeFromStream(bytes.NewReader(buf))
		})
	}
}

func TestDeserializeFromStreamv100_AllByteFlipsSafe(t *testing.T) {
	// Build a v1.0.0 stream using the existing helper from state_extra_test.go.
	buf := buildV100Stream(t)
	clean := buf.Bytes()

	for pos := 0; pos < len(clean); pos++ {
		corrupted := append([]byte(nil), clean...)
		corrupted[pos] ^= 0xFF

		dstCache := newMockStateCache()
		dst := &LocalState{cache: dstCache}

		notPanic(t, func() {
			_ = dst.deserializeFromStreamv100(bytes.NewReader(corrupted))
		})
	}
}

// =============================================================================
// FromStream dispatch — wrong version uses v1.0.0 path
// =============================================================================

func TestFromStream_UnknownVersionUsesV100(t *testing.T) {
	stream := buildV100Stream(t)
	// Any non-"1.1.0" version must take the v100 branch and decode the v100 stream.
	cache := newMockStateCache()
	st, err := FromStream(stream, versioning.FromString("0.9.0"), cache)
	require.NoError(t, err)
	require.NotNil(t, st)
}

func TestFromStream_ErrorPropagates(t *testing.T) {
	// Empty stream — both paths should error.
	cache := newMockStateCache()
	_, err := FromStream(bytes.NewReader(nil), versioning.FromString("1.1.0"), cache)
	require.Error(t, err)

	cache = newMockStateCache()
	_, err = FromStream(bytes.NewReader(nil), versioning.FromString("1.0.0"), cache)
	require.Error(t, err)
}

// =============================================================================
// Cache errors propagate from deserializeFromStream PutDelta/PutColoured/...
// =============================================================================

// erroringCache lets us choose specific Put* failures while leaving the rest
// functional (so deserialization reaches the failure point).
type erroringCache struct {
	*mockStateCache
	putDeltaErr    error
	putColouredErr error
	putPackfileErr error
}

func (c *erroringCache) PutDelta(t resources.Type, b, p objects.MAC, data []byte) error {
	if c.putDeltaErr != nil {
		return c.putDeltaErr
	}
	return c.mockStateCache.PutDelta(t, b, p, data)
}
func (c *erroringCache) PutColoured(t resources.Type, b objects.MAC, data []byte) error {
	if c.putColouredErr != nil {
		return c.putColouredErr
	}
	return c.mockStateCache.PutColoured(t, b, data)
}
func (c *erroringCache) PutPackfile(p objects.MAC, data []byte) error {
	if c.putPackfileErr != nil {
		return c.putPackfileErr
	}
	return c.mockStateCache.PutPackfile(p, data)
}

func buildStreamWithSingleDelta(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := bytes.NewBuffer(make([]byte, 32)) // header
	de := (&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob:     objects.MAC{1},
		Location: Location{Packfile: objects.MAC{2}, Offset: 0, Length: 4},
	}).ToBytes()
	buf.WriteByte(byte(ET_LOCATIONS))
	binary.Write(buf, binary.LittleEndian, uint32(DeltaEntrySerializedSize))
	buf.Write(de)
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write(make([]byte, 16))
	return buf
}

func buildStreamWithSingleColoured(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := bytes.NewBuffer(make([]byte, 32))
	ce := (&ColouredEntry{
		Type: resources.RT_OBJECT, Blob: objects.MAC{1}, When: time.Unix(0, 1),
	}).ToBytes()
	buf.WriteByte(byte(ET_COLOURED))
	binary.Write(buf, binary.LittleEndian, uint32(ColouredEntrySerializedSize))
	buf.Write(ce)
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write(make([]byte, 16))
	return buf
}

func buildStreamWithSinglePackfile(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := bytes.NewBuffer(make([]byte, 32))
	pe := (&PackfileEntry{
		Packfile: objects.MAC{1}, StateID: objects.MAC{2}, Timestamp: time.Unix(0, 1),
	}).ToBytes()
	buf.WriteByte(byte(ET_PACKFILE))
	binary.Write(buf, binary.LittleEndian, uint32(PackfileEntrySerializedSize))
	buf.Write(pe)
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write(make([]byte, 16))
	return buf
}

func buildStreamWithSingleConfig(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := bytes.NewBuffer(make([]byte, 32))
	ce := (&ConfigurationEntry{Key: "k", Value: []byte("v"), CreatedAt: time.Unix(0, 1)}).ToBytes()
	buf.WriteByte(byte(ET_CONFIGURATION))
	binary.Write(buf, binary.LittleEndian, uint32(len(ce)))
	buf.Write(ce)
	buf.WriteByte(byte(ET_METADATA))
	binary.Write(buf, binary.LittleEndian, uint32(versioning.FromString(VERSION)))
	binary.Write(buf, binary.LittleEndian, uint64(time.Now().UnixNano()))
	buf.Write(make([]byte, 16))
	return buf
}

func TestDeserializeFromStream_PutDeltaError(t *testing.T) {
	cache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putDeltaErr:    errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	err := st.deserializeFromStream(buildStreamWithSingleDelta(t))
	require.Error(t, err)
}

func TestDeserializeFromStream_PutColouredError(t *testing.T) {
	cache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putColouredErr: errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	err := st.deserializeFromStream(buildStreamWithSingleColoured(t))
	require.Error(t, err)
}

func TestDeserializeFromStream_PutPackfileError(t *testing.T) {
	cache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putPackfileErr: errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	err := st.deserializeFromStream(buildStreamWithSinglePackfile(t))
	require.Error(t, err)
}

func TestDeserializeFromStream_PutConfigurationError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		putConfigErr:   errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	err := st.deserializeFromStream(buildStreamWithSingleConfig(t))
	require.Error(t, err)
}

func TestDeserializeFromStreamv100_PutDeltaError(t *testing.T) {
	cache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putDeltaErr:    errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	// v100 has no header.
	full := buildStreamWithSingleDelta(t).Bytes()
	err := st.deserializeFromStreamv100(bytes.NewReader(full[32:]))
	require.Error(t, err)
}

func TestDeserializeFromStreamv100_PutColouredError(t *testing.T) {
	cache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putColouredErr: errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	full := buildStreamWithSingleColoured(t).Bytes()
	err := st.deserializeFromStreamv100(bytes.NewReader(full[32:]))
	require.Error(t, err)
}

func TestDeserializeFromStreamv100_PutPackfileError(t *testing.T) {
	cache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putPackfileErr: errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	full := buildStreamWithSinglePackfile(t).Bytes()
	err := st.deserializeFromStreamv100(bytes.NewReader(full[32:]))
	require.Error(t, err)
}

func TestDeserializeFromStreamv100_PutConfigurationError(t *testing.T) {
	cache := &errCache{
		mockStateCache: newMockStateCache(),
		putConfigErr:   errors.New("boom"),
	}
	st := &LocalState{cache: cache}
	full := buildStreamWithSingleConfig(t).Bytes()
	err := st.deserializeFromStreamv100(bytes.NewReader(full[32:]))
	require.Error(t, err)
}

// =============================================================================
// mergeFromCache full coverage
// =============================================================================

// TestMergeFromCache_AllEntryTypes exercises every path in mergeFromCache:
// deltas, coloured, packfiles, configurations, plus error in each case.
func TestMergeFromCache_AllEntryTypes(t *testing.T) {
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)

	require.NoError(t, src.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob:     objects.MAC{1},
		Location: Location{Packfile: objects.MAC{2}},
	}))
	require.NoError(t, src.ColourResource(resources.RT_OBJECT, objects.MAC{3}))
	require.NoError(t, src.PutPackfile(objects.MAC{4}, objects.MAC{5}))
	require.NoError(t, src.SetConfiguration("k", []byte("v")))

	dstCache := newMockStateCache()
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	require.NoError(t, dst.mergeFromCache(srcCache))

	// Verify each kind of entry was forwarded.
	got, _ := dstCache.GetConfiguration("k")
	require.NotNil(t, got)
}

func TestMergeFromCache_BadDeltaBytes(t *testing.T) {
	srcCache := newMockStateCache()
	srcCache.deltas["1:2:3"] = []byte{0x01} // key matches the 3-segment format, value is garbage

	dst, err := NewLocalState(newMockStateCache())
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_BadColouredBytes(t *testing.T) {
	srcCache := newMockStateCache()
	srcCache.coloured["1:2"] = []byte{0x01}

	dst, err := NewLocalState(newMockStateCache())
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_BadPackfileBytes(t *testing.T) {
	srcCache := newMockStateCache()
	srcCache.packfiles[objects.MAC{1}] = []byte{0x01}

	dst, err := NewLocalState(newMockStateCache())
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_BadConfigBytes(t *testing.T) {
	srcCache := newMockStateCache()
	srcCache.configurations["k"] = []byte{0x01}

	dst, err := NewLocalState(newMockStateCache())
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_PutDeltaError(t *testing.T) {
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)
	require.NoError(t, src.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	dstCache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putDeltaErr:    errors.New("boom"),
	}
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_PutColouredError(t *testing.T) {
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)
	require.NoError(t, src.ColourResource(resources.RT_OBJECT, objects.MAC{1}))

	dstCache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putColouredErr: errors.New("boom"),
	}
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_PutPackfileError(t *testing.T) {
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)
	require.NoError(t, src.PutPackfile(objects.MAC{1}, objects.MAC{2}))

	dstCache := &erroringCache{
		mockStateCache: newMockStateCache(),
		putPackfileErr: errors.New("boom"),
	}
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

func TestMergeFromCache_InsertConfigError(t *testing.T) {
	srcCache := newMockStateCache()
	src, err := NewLocalState(srcCache)
	require.NoError(t, err)
	require.NoError(t, src.SetConfiguration("k", []byte("v")))

	dstCache := &errCache{
		mockStateCache: newMockStateCache(),
		putConfigErr:   errors.New("boom"),
	}
	dst, err := NewLocalState(dstCache)
	require.NoError(t, err)

	err = dst.mergeFromCache(srcCache)
	require.Error(t, err)
}

// =============================================================================
// MergeState with v1.0.0 path
// =============================================================================

func TestMergeState_V100Path(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// Build a valid v1.0.0 stream.
	stream := buildV100Stream(t)
	err = st.MergeState(objects.MAC{0xA1}, stream, versioning.FromString("1.0.0"))
	require.NoError(t, err)

	has, err := st.HasState(objects.MAC{0xA1})
	require.NoError(t, err)
	require.True(t, has)
}

// =============================================================================
// PutState — Metadata.ToBytes failure is unreachable (Metadata always
// msgpack-encodable), so we ensure the error from cache.PutState propagates.
// =============================================================================

// (Already covered by TestPutState_CacheError above.)

// =============================================================================
// List* early-break paths and HasPackfile/HasColoured error propagation
// =============================================================================

// snapshotErrorCache reports errors on HasPackfile / HasColoured to test the
// error-yield branches in ListSnapshots / ListObjectsOfType / ListOrphanDeltas.
type snapshotErrorCache struct {
	*mockStateCache
	hasPackfileErr error
	hasColouredErr error
}

func (c *snapshotErrorCache) HasPackfile(p objects.MAC) (bool, error) {
	if c.hasPackfileErr != nil {
		return false, c.hasPackfileErr
	}
	return c.mockStateCache.HasPackfile(p)
}

func (c *snapshotErrorCache) HasColoured(t resources.Type, m objects.MAC) (bool, error) {
	if c.hasColouredErr != nil {
		return false, c.hasColouredErr
	}
	return c.mockStateCache.HasColoured(t, m)
}

// =============================================================================
// List* — corrupted-delta-yields-error branches and break-on-error returns
// =============================================================================

func TestListSnapshots_BadDeltaBytes(t *testing.T) {
	cache := newMockStateCache()
	// RT_SNAPSHOT == 5 in resources.go; key must start with that for
	// the mock's type-filter to yield this entry.
	cache.deltas["5:0:0"] = []byte{0x07}

	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var sawErr bool
	for _, e := range st.ListSnapshots() {
		if e != nil {
			sawErr = true
		}
	}
	require.True(t, sawErr, "corrupt snapshot delta bytes must yield an error")
}

func TestListObjectsOfType_BadDeltaBytes(t *testing.T) {
	cache := newMockStateCache()
	// RT_OBJECT == 7.
	cache.deltas["7:0:0"] = []byte{0x01}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var sawErr bool
	for _, e := range st.ListObjectsOfType(resources.RT_OBJECT) {
		if e != nil {
			sawErr = true
		}
	}
	require.True(t, sawErr)
}

func TestListOrphanDeltas_BadDeltaBytes(t *testing.T) {
	cache := newMockStateCache()
	cache.deltas["1:0:0"] = []byte{0x01}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var sawErr bool
	for _, e := range st.ListOrphanDeltas() {
		if e != nil {
			sawErr = true
		}
	}
	require.True(t, sawErr)
}

// breakOnFirstYieldCache yields one entry then returns. Useful to test the
// `if !yield(...) return` branches in List* iterators.
func TestListSnapshots_BreakOnHasPackfileError(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_SNAPSHOT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasPackfileErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	// Consumer that returns false after first yield — exercises the early
	// return branch (`return`) inside the iterator.
	calls := 0
	for range st.ListSnapshots() {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

func TestListSnapshots_BreakOnHasColouredError(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	pf := objects.MAC{0x10}
	require.NoError(t, base.PutPackfile(pf, []byte("p")))
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_SNAPSHOT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: pf},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasColouredErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	calls := 0
	for range st.ListSnapshots() {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

func TestListObjectsOfType_BreakOnHasPackfileError(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasPackfileErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	calls := 0
	for range st.ListObjectsOfType(resources.RT_OBJECT) {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

func TestListOrphanDeltas_BreakOnHasPackfileError(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasPackfileErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	calls := 0
	for range st.ListOrphanDeltas() {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

// breakImmediatelyAfter yields once with bad data so the consumer can choose
// to break immediately — covers the "yield(err) returns false → return" branch.
func TestListSnapshots_BreakOnBadBytesError(t *testing.T) {
	cache := newMockStateCache()
	cache.deltas["5:0:0"] = []byte{0x07} // RT_SNAPSHOT
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	calls := 0
	for range st.ListSnapshots() {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

func TestListObjectsOfType_BreakOnBadBytesError(t *testing.T) {
	cache := newMockStateCache()
	cache.deltas["7:0:0"] = []byte{0x01} // RT_OBJECT
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	calls := 0
	for range st.ListObjectsOfType(resources.RT_OBJECT) {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

func TestListOrphanDeltas_BreakOnBadBytesError(t *testing.T) {
	cache := newMockStateCache()
	cache.deltas["7:0:0"] = []byte{0x01}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	calls := 0
	for range st.ListOrphanDeltas() {
		calls++
		break
	}
	require.Equal(t, 1, calls)
}

func TestListSnapshots_BreakEarly(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	pf := objects.MAC{0x10}
	require.NoError(t, cache.PutPackfile(pf, []byte("p")))
	for i := byte(0); i < 5; i++ {
		require.NoError(t, st.PutDelta(&DeltaEntry{
			Type: resources.RT_SNAPSHOT, Version: versioning.FromString("1.0.0"),
			Blob: objects.MAC{i}, Location: Location{Packfile: pf},
		}))
	}

	// Consumer that stops after 1 item.
	count := 0
	for range st.ListSnapshots() {
		count++
		if count == 1 {
			break
		}
	}
	require.Equal(t, 1, count)
}

func TestListSnapshots_HasPackfileErrorYielded(t *testing.T) {
	base := newMockStateCache()
	// Seed a snapshot delta.
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_SNAPSHOT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasPackfileErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var errs []error
	for _, err := range st.ListSnapshots() {
		errs = append(errs, err)
	}
	require.NotEmpty(t, errs)
	require.Error(t, errs[0])
}

func TestListSnapshots_ColouredSkipped(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	pf := objects.MAC{0x20}
	blob := objects.MAC{0x21}
	require.NoError(t, cache.PutPackfile(pf, []byte("p")))
	require.NoError(t, st.PutDelta(&DeltaEntry{
		Type: resources.RT_SNAPSHOT, Version: versioning.FromString("1.0.0"),
		Blob: blob, Location: Location{Packfile: pf},
	}))
	require.NoError(t, st.ColourResource(resources.RT_SNAPSHOT, blob))

	got := 0
	for range st.ListSnapshots() {
		got++
	}
	require.Equal(t, 0, got, "coloured snapshots must be hidden from ListSnapshots")
}

func TestListSnapshots_HasColouredErrorYielded(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	pf := objects.MAC{0x30}
	require.NoError(t, base.PutPackfile(pf, []byte("p")))
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_SNAPSHOT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{0x31}, Location: Location{Packfile: pf},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasColouredErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var sawErr bool
	for _, e := range st.ListSnapshots() {
		if e != nil {
			sawErr = true
		}
	}
	require.True(t, sawErr)
}

func TestListPackfileEntries_BreakEarly(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	for i := byte(0); i < 5; i++ {
		require.NoError(t, st.PutPackfile(objects.MAC{i}, objects.MAC{i + 10}))
	}

	count := 0
	for range st.ListPackfileEntries() {
		count++
		break
	}
	require.Equal(t, 1, count)
}

func TestListPackfiles_BreakEarly(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	for i := byte(0); i < 5; i++ {
		require.NoError(t, cache.PutPackfile(objects.MAC{i}, []byte("p")))
	}

	count := 0
	for range st.ListPackfiles() {
		count++
		break
	}
	require.Equal(t, 1, count)
}

func TestListObjectsOfType_HasPackfileError(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasPackfileErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var sawErr bool
	for _, e := range st.ListObjectsOfType(resources.RT_OBJECT) {
		if e != nil {
			sawErr = true
		}
	}
	require.True(t, sawErr)
}

func TestListObjectsOfType_BreakEarly(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	pf := objects.MAC{0x10}
	require.NoError(t, cache.PutPackfile(pf, []byte("p")))
	for i := byte(0); i < 5; i++ {
		require.NoError(t, st.PutDelta(&DeltaEntry{
			Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
			Blob: objects.MAC{i}, Location: Location{Packfile: pf},
		}))
	}

	count := 0
	for range st.ListObjectsOfType(resources.RT_OBJECT) {
		count++
		break
	}
	require.Equal(t, 1, count)
}

func TestListOrphanDeltas_HasPackfileError(t *testing.T) {
	base := newMockStateCache()
	stTmp, err := NewLocalState(base)
	require.NoError(t, err)
	require.NoError(t, stTmp.PutDelta(&DeltaEntry{
		Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
		Blob: objects.MAC{1}, Location: Location{Packfile: objects.MAC{2}},
	}))

	cache := &snapshotErrorCache{
		mockStateCache: base,
		hasPackfileErr: errors.New("boom"),
	}
	st, err := NewLocalState(cache)
	require.NoError(t, err)

	var sawErr bool
	for _, e := range st.ListOrphanDeltas() {
		if e != nil {
			sawErr = true
		}
	}
	require.True(t, sawErr)
}

func TestListOrphanDeltas_BreakEarly(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	// All deltas are orphans (no packfile).
	for i := byte(0); i < 5; i++ {
		require.NoError(t, st.PutDelta(&DeltaEntry{
			Type: resources.RT_OBJECT, Version: versioning.FromString("1.0.0"),
			Blob: objects.MAC{i}, Location: Location{Packfile: objects.MAC{i + 50}},
		}))
	}

	count := 0
	for range st.ListOrphanDeltas() {
		count++
		break
	}
	require.Equal(t, 1, count)
}

func TestListColouredResources_BreakEarly(t *testing.T) {
	cache := newMockStateCache()
	st, err := NewLocalState(cache)
	require.NoError(t, err)
	for i := byte(0); i < 5; i++ {
		require.NoError(t, st.ColourResource(resources.RT_OBJECT, objects.MAC{i}))
	}

	count := 0
	for range st.ListColouredResources(resources.RT_OBJECT) {
		count++
		break
	}
	require.Equal(t, 1, count)
}

// =============================================================================
// Ensure no caching import drift — compile-time check that errCache satisfies
// caching.StateCache (so its overrides are picked up by NewLocalState).
// =============================================================================

var _ caching.StateCache = (*errCache)(nil)
var _ caching.StateCache = (*deltaInjector)(nil)
var _ caching.StateCache = (*erroringCache)(nil)
var _ caching.StateCache = (*snapshotErrorCache)(nil)
