package header

import (
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

// TestNewSource ensures the zero-value constructor produces non-nil slices.
func TestNewSource(t *testing.T) {
	s := NewSource()
	require.NotNil(t, s.Context)
	require.NotNil(t, s.Indexes)
}

// TestGetSource verifies index-based source retrieval.
func TestGetSource(t *testing.T) {
	h := NewHeader("test", objects.MAC{})
	src := NewSource()
	src.Importer.Type = "fs"
	src.Importer.Directory = "/data"
	h.Sources = append(h.Sources, src)

	got := h.GetSource(0)
	require.NotNil(t, got)
	require.Equal(t, "fs", got.Importer.Type)
	require.Equal(t, "/data", got.Importer.Directory)
}

// TestHasTag checks that HasTag matches only contained tags.
func TestHasTag(t *testing.T) {
	h := NewHeader("tagged", objects.MAC{})
	h.Tags = []string{"alpha", "beta"}

	require.True(t, h.HasTag("alpha"))
	require.True(t, h.HasTag("beta"))
	require.False(t, h.HasTag("gamma"))
	// Empty string does not match unless explicitly in the tag list.
	require.False(t, h.HasTag(""))
}

// TestSerializeRoundTrip verifies Serialize → NewFromBytes is lossless.
func TestSerializeRoundTrip(t *testing.T) {
	mac := objects.MAC{1, 2, 3, 4}
	h := NewHeader("roundtrip", mac)
	h.Timestamp = time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	h.Tags = []string{"a", "b"}

	data, err := h.Serialize()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	h2, err := NewFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, h.Name, h2.Name)
	require.Equal(t, h.Tags, h2.Tags)
	require.True(t, h.Timestamp.Equal(h2.Timestamp))
}

// TestNewFromBytesError verifies that corrupt data is rejected.
func TestNewFromBytesError(t *testing.T) {
	_, err := NewFromBytes([]byte("not valid msgpack data at all !!"))
	require.Error(t, err)
}

// TestGetSourceInvalidIndex verifies GetSource panics on out-of-range indices.
func TestGetSourceInvalidIndex(t *testing.T) {
	h := NewHeader("test", objects.MAC{})
	require.Panics(t, func() { h.GetSource(-1) })
	require.Panics(t, func() { h.GetSource(0) }) // no sources yet
	h.Sources = append(h.Sources, NewSource())
	require.Panics(t, func() { h.GetSource(1) }) // one past the end
}

// TestSortHeadersEqualKeys exercises the comparator's terminal "return false"
// path, reached when two headers compare equal on every sort key.
func TestSortHeadersEqualKeys(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	mac := objects.MAC{9}
	a := NewHeader("a", mac)
	a.Timestamp = ts
	b := NewHeader("b", mac)
	b.Timestamp = ts

	headers := []Header{*a, *b}
	// Both share Identifier and Timestamp, so each comparison falls through to
	// the final `return false`.
	err := SortHeaders(headers, []string{"Identifier", "Timestamp"})
	require.NoError(t, err)
	require.Len(t, headers, 2)
}
