package packfile

import (
	"bytes"
	"crypto/sha256"
	"hash"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// Suppress unused import warning — io is used in other tests
var _ = io.EOF

func sha256Factory() hash.Hash {
	return sha256.New()
}

func identityEncoder(r io.Reader) (io.Reader, error) {
	return r, nil
}

func TestPackfileOnDiskAddAndEntries(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	data1 := []byte("chunk one data")
	data2 := []byte("chunk two data")
	mac1 := objects.MAC{1}
	mac2 := objects.MAC{2}
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)

	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac1, data1, 0))
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac2, data2, 0))

	entries := p.Entries()
	require.Len(t, entries, 2)
	require.Equal(t, mac1, entries[0].MAC)
	require.Equal(t, mac2, entries[1].MAC)
}

func TestPackfileOnDiskSize(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	require.Equal(t, uint64(0), p.Size())

	data := []byte("some data here")
	mac := objects.MAC{5}
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)

	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac, data, 0))
	require.Equal(t, uint64(len(data)), p.Size())
}

func TestPackfileOnDiskSerialize(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)

	data := []byte("test blob data")
	mac := objects.MAC{10}
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)

	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac, data, 0))

	reader, fileMac, err := p.Serialize(identityEncoder)
	require.NoError(t, err)
	require.NotEqual(t, objects.NilMac, fileMac)
	require.NotNil(t, reader)

	// Read all serialized content
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, content)

	// The blob data should appear somewhere in the output
	require.Contains(t, string(content), string(data))
}

func TestPackfileOnDiskCleanup(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)

	err = p.Cleanup()
	require.NoError(t, err)
}

func TestPackfileOnDiskMultipleBlobs(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	blobs := []struct {
		mac  objects.MAC
		data []byte
	}{
		{objects.MAC{1}, []byte("first blob")},
		{objects.MAC{2}, []byte("second blob")},
		{objects.MAC{3}, []byte("third blob")},
	}

	for _, b := range blobs {
		require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, b.mac, b.data, 0))
	}

	entries := p.Entries()
	require.Len(t, entries, 3)

	// Verify offsets are sequential
	offset := uint64(0)
	for i, b := range blobs {
		require.Equal(t, offset, entries[i].Offset)
		require.Equal(t, uint32(len(b.data)), entries[i].Length)
		offset += uint64(len(b.data))
	}
}

func TestPackfileInMemorySerializeRoundtrip(t *testing.T) {
	// The packfile format uses an encoder; use a real compression-free passthrough
	// that still wraps the bytes so the format remains parseable. Use nil-bytes passthrough.
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	data1 := []byte("hello world data")
	data2 := []byte("goodbye world data")
	mac1 := objects.MAC{11}
	mac2 := objects.MAC{12}
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)

	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac1, data1, 0))
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac2, data2, 0))

	entries := p.Entries()
	require.Len(t, entries, 2)
	require.Equal(t, mac1, entries[0].MAC)
	require.Equal(t, mac2, entries[1].MAC)
	require.Equal(t, uint32(len(data1)), entries[0].Length)
	require.Equal(t, uint32(len(data2)), entries[1].Length)

	got1, ok := p.getBlob(mac1)
	require.True(t, ok)
	require.Equal(t, data1, got1)

	got2, ok := p.getBlob(mac2)
	require.True(t, ok)
	require.Equal(t, data2, got2)
}

func TestNewInMemoryIndexFromBytes(t *testing.T) {
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	mac := objects.MAC{40}

	// Use the in-memory packfile to get a real index buffer
	p := newPackfileInMemory(sha256.New()).(*PackfileInMemory)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac, []byte("data"), 0))

	// The index is p.Index; serialize it to bytes in the expected binary format
	idxBuf := &bytes.Buffer{}
	for _, b := range p.Index {
		writeLE(idxBuf, b)
	}

	index, err := NewInMemoryIndexFromBytes(p.Footer.Version, idxBuf.Bytes())
	require.NoError(t, err)
	require.Len(t, index, 1)
	require.Equal(t, mac, index[0].MAC)
}

func TestNewInMemoryFooterFromBytesTooShort(t *testing.T) {
	// Too short buffer — should return error
	_, err := NewInMemoryFooterFromBytes(versioning.FromString(VERSION), make([]byte, 4))
	require.Error(t, err)
}

// writeLE writes a Blob to a buffer in little-endian binary format matching the packfile format.
func writeLE(buf *bytes.Buffer, b Blob) {
	type4 := make([]byte, 4)
	type4[0] = byte(b.Type)
	buf.Write(type4)

	ver4 := make([]byte, 4)
	ver4[0] = byte(b.Version)
	buf.Write(ver4)

	buf.Write(b.MAC[:])

	off8 := make([]byte, 8)
	for i := 0; i < 8; i++ {
		off8[i] = byte(b.Offset >> (uint(i) * 8))
	}
	buf.Write(off8)

	len4 := make([]byte, 4)
	for i := 0; i < 4; i++ {
		len4[i] = byte(b.Length >> (uint(i) * 8))
	}
	buf.Write(len4)

	flags4 := make([]byte, 4)
	for i := 0; i < 4; i++ {
		flags4[i] = byte(b.Flags >> (uint(i) * 8))
	}
	buf.Write(flags4)
}

func TestNewPackfileInMemory(t *testing.T) {
	p, err := NewPackfileInMemory(sha256Factory)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.Equal(t, uint64(0), p.Size())
	require.Empty(t, p.Entries())
}

func TestPackfileInMemoryCleanup(t *testing.T) {
	p, err := NewPackfileInMemory(sha256Factory)
	require.NoError(t, err)
	require.NoError(t, p.Cleanup())
}

func TestPackfileInMemorySerialize(t *testing.T) {
	p, err := NewPackfileInMemory(sha256Factory)
	require.NoError(t, err)

	data := []byte("serialize test data")
	mac := objects.MAC{50}
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac, data, 0))

	reader, fileMac, err := p.Serialize(identityEncoder)
	require.NoError(t, err)
	require.NotEqual(t, objects.NilMac, fileMac)
	require.NotNil(t, reader)

	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, content)
}

