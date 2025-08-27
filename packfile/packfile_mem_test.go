package packfile

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestPackFile(t *testing.T) {
	hasher := hmac.New(sha256.New, []byte("testkey"))

	p := NewPackfileInMemory(hasher).(*PackfileInMemory)

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	mac1 := [32]byte{1} // Mock mac for chunk1
	mac2 := [32]byte{2} // Mock mac for chunk2

	// Test AddBlob
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac1, chunk1, 0)
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac2, chunk2, 0)

	// Test GetBlob
	retrievedChunk1, exists := p.getBlob(mac1)
	if !exists || !bytes.Equal(retrievedChunk1, chunk1) {
		t.Fatalf("Expected %s but got %s", chunk1, retrievedChunk1)
	}

	retrievedChunk2, exists := p.getBlob(mac2)
	if !exists || !bytes.Equal(retrievedChunk2, chunk2) {
		t.Fatalf("Expected %s but got %s", chunk2, retrievedChunk2)
	}

	// this blob should not exist
	_, exists = p.getBlob([32]byte{200})
	require.Equal(t, false, exists)

	// Check PackFile Metadata
	if p.Footer.Count != 2 {
		t.Fatalf("Expected Footer.Count to be 2 but got %d", p.Footer.Count)
	}
	if p.Footer.IndexOffset != uint64(len(p.Blobs)) {
		t.Fatalf("Expected Footer.Length to be %d but got %d", len(p.Blobs), p.Footer.IndexOffset)
	}
}

func _TestPackFileSerializeIndex(t *testing.T) {
	hasher := hmac.New(sha256.New, []byte("testkey"))

	p := NewPackfileInMemory(hasher).(*PackfileInMemory)

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	mac1 := objects.MAC{1} // Mock mac for chunk1
	mac2 := objects.MAC{2} // Mock mac for chunk2

	// Test AddBlob
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac1, chunk1, 0)
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac2, chunk2, 0)

	// Test packfile Size
	require.Equal(t, p.Size(), uint32(44), "Expected 2 blobs but got %q", p.Size())

	// Test Serialize and NewFromBytes
	serialized, err := p.SerializeIndex()
	require.NoError(t, err, "Failed to serialize PackFile index")

	p2, err := NewInMemoryIndexFromBytes(versioning.GetCurrentVersion(resources.RT_PACKFILE), serialized)
	require.NoError(t, err, "Failed to create PackFile index from bytes")

	require.Equal(t, len(p2), 2, "Expected 2 blobs but got %d", len(p2))

	// Test that both chunks are equal after serialization and deserialization
	blob1, blob2 := p2[0], p2[1]

	require.Equal(t, blob1.Type, resources.RT_CHUNK)
	require.Equal(t, blob2.Type, resources.RT_CHUNK)

	require.Equal(t, blob1.Version, versioning.GetCurrentVersion(resources.RT_CHUNK))
	require.Equal(t, blob2.Version, versioning.GetCurrentVersion(resources.RT_CHUNK))

	require.Equal(t, blob1.Length, uint32(len(chunk1)))
	require.Equal(t, blob2.Length, uint32(len(chunk2)))

	require.Equal(t, blob1.MAC, mac1, "Expected %q but got %q", mac1, blob1.MAC)
	require.Equal(t, blob2.MAC, mac2, "Expected %q but got %q", mac1, blob2.MAC)
}

func _TestPackFileSerializeFooter(t *testing.T) {
	hasher := hmac.New(sha256.New, []byte("testkey"))
	p := NewPackfileInMemory(hasher).(*PackfileInMemory)

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	mac1 := [32]byte{1} // Mock mac for chunk1
	mac2 := [32]byte{2} // Mock mac for chunk2

	// Test AddBlob
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac1, chunk1, 0)
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac2, chunk2, 0)

	// Test Serialize and NewFromBytes
	serialized, err := p.SerializeFooter()
	require.NoError(t, err, "Failed to serialize PackFile footer")

	p2, err := NewInMemoryFooterFromBytes(versioning.GetCurrentVersion(resources.RT_PACKFILE), serialized)
	require.NoError(t, err, "Failed to create PackFile footer from bytes")

	require.Equal(t, p2.Count, uint32(2), "Expected 2 blobs but got %d", uint32(p2.Count))

	require.Equal(t, p2.IndexOffset, uint64(len(chunk1)+len(chunk2)), "Expected IndexOffset to be %d but got %d", len(chunk1)+len(chunk2), p2.IndexOffset)
}

func _TestPackFileSerializeData(t *testing.T) {
	hasher := hmac.New(sha256.New, []byte("testkey"))
	p := NewPackfileInMemory(hasher).(*PackfileInMemory)

	// Define some sample chunks
	chunk1 := []byte("This is chunk number 1")
	chunk2 := []byte("This is chunk number 2")
	mac1 := [32]byte{1} // Mock mac for chunk1
	mac2 := [32]byte{2} // Mock mac for chunk2

	// Test AddBlob
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac1, chunk1, 0)
	p.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac2, chunk2, 0)

	// Test SerializeData
	serialized, err := p.SerializeData()
	require.NoError(t, err, "Failed to serialize PackFile data")

	// Check that the serialized data is correct
	expected := append(chunk1, chunk2...)
	require.Equal(t, expected, serialized, "Serialized data does not match expected data")
}

func TestDefaultConfiguration(t *testing.T) {
	c := NewDefaultConfiguration()

	require.Equal(t, c.MinSize, uint64(0))
	require.Equal(t, c.AvgSize, uint64(0))
	require.Equal(t, c.MaxSize, uint64(20971520))
}
