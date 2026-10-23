package packfile

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// validUnwrappedPackfile builds a one-blob packfile and returns it in the
// layout NewPackfileInMemoryFromBytes expects: blobs, index, footer.
func validUnwrappedPackfile(t *testing.T) []byte {
	t.Helper()

	p := newPackfileInMemory(sha256.New()).(*PackfileInMemory)
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{0x42}, []byte("malformed footer fixture"), 0))

	reader, _, err := p.Serialize(func(r io.Reader) (io.Reader, error) { return r, nil })
	require.NoError(t, err)
	serialized, err := io.ReadAll(reader)
	require.NoError(t, err)

	return unwrapSerializedPackfile(t, serialized, p.Footer)
}

// TestNewPackfileInMemoryFromBytesMalformedFooter feeds footers whose
// IndexOffset or Count do not match the bytes that precede them. Each must be
// rejected with an error before anything is allocated: a corrupt footer used
// to drive make([]byte, IndexOffset), which panics with "len out of range" or
// tries a multi-gigabyte allocation.
func TestNewPackfileInMemoryFromBytesMalformedFooter(t *testing.T) {
	valid := validUnwrappedPackfile(t)
	footerStart := len(valid) - FOOTER_SIZE
	// Raw footer layout: Timestamp(8) Count(4) IndexOffset(8) IndexMAC(32) Flags(4).
	countAt := footerStart + 8
	indexOffsetAt := footerStart + 12

	setCount := func(b []byte, v uint32) { binary.LittleEndian.PutUint32(b[countAt:], v) }
	setIndexOffset := func(b []byte, v uint64) { binary.LittleEndian.PutUint64(b[indexOffsetAt:], v) }

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{
			name: "index offset is max uint64",
			mutate: func(b []byte) []byte {
				setIndexOffset(b, ^uint64(0))
				return b
			},
		},
		{
			name: "index offset exceeds int range",
			mutate: func(b []byte) []byte {
				setIndexOffset(b, 1<<63)
				return b
			},
		},
		{
			name: "index offset asks for gigabytes",
			mutate: func(b []byte) []byte {
				setIndexOffset(b, 8<<30)
				return b
			},
		},
		{
			name: "index offset one past the footer",
			mutate: func(b []byte) []byte {
				setIndexOffset(b, uint64(footerStart)+1)
				return b
			},
		},
		{
			name: "index offset eats one index byte",
			mutate: func(b []byte) []byte {
				setIndexOffset(b, uint64(footerStart)-BLOB_RECORD_SIZE+1)
				return b
			},
		},
		{
			name: "index offset leaves a stray byte before the index",
			mutate: func(b []byte) []byte {
				setIndexOffset(b, uint64(footerStart)-BLOB_RECORD_SIZE-1)
				return b
			},
		},
		{
			name: "count claims one record too many",
			mutate: func(b []byte) []byte {
				setCount(b, 2)
				return b
			},
		},
		{
			name: "count claims no records",
			mutate: func(b []byte) []byte {
				setCount(b, 0)
				return b
			},
		},
		{
			name: "count is max uint32",
			mutate: func(b []byte) []byte {
				setCount(b, ^uint32(0))
				return b
			},
		},
		{
			name:   "one byte truncated",
			mutate: func(b []byte) []byte { return b[:len(b)-1] },
		},
		{
			name:   "one byte appended",
			mutate: func(b []byte) []byte { return append(b, 0) },
		},
		{
			name:   "footer only",
			mutate: func(b []byte) []byte { return b[footerStart:] },
		},
		{
			name:   "shorter than a footer",
			mutate: func(b []byte) []byte { return b[:FOOTER_SIZE-1] },
		},
		{
			name:   "empty",
			mutate: func([]byte) []byte { return nil },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			input := tc.mutate(append([]byte{}, valid...))
			require.NotPanics(t, func() {
				_, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), input)
				require.Error(t, err)
			})
		})
	}

	t.Run("untouched input still decodes", func(t *testing.T) {
		p, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), valid)
		require.NoError(t, err)
		require.Len(t, p.Index, 1)
	})
}
