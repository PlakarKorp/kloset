package packfile

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"testing"

	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestPackFile(t *testing.T) {
	hasher := hmac.New(sha256.New, []byte("testkey"))

	p := newPackfileInMemory(hasher).(*PackfileInMemory)

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

func TestDefaultConfiguration(t *testing.T) {
	c := NewDefaultConfiguration()

	require.Equal(t, c.MinSize, uint64(0))
	require.Equal(t, c.AvgSize, uint64(0))
	require.Equal(t, c.MaxSize, uint64(20971520))
}
