package packfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// buildValidFooterBytes constructs a minimal valid footer byte slice that
// NewInMemoryFooterFromBytes can parse: Timestamp(8) + Count(4) + IndexOffset(8) + IndexMAC(32) + Flags(4) = 56 bytes.
func buildValidFooterBytes() []byte {
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, int64(123456789))  // Timestamp
	_ = binary.Write(buf, binary.LittleEndian, uint32(3))         // Count
	_ = binary.Write(buf, binary.LittleEndian, uint64(512))       // IndexOffset
	mac := objects.MAC{1, 2, 3, 4}
	_ = binary.Write(buf, binary.LittleEndian, mac)               // IndexMAC
	_ = binary.Write(buf, binary.LittleEndian, uint32(0))         // Flags
	return buf.Bytes()
}

// TestNewInMemoryFooterFromBytesValid verifies a correctly sized buffer parses
// back into the expected field values.
func TestNewInMemoryFooterFromBytesValid(t *testing.T) {
	ver := versioning.FromString(VERSION)
	raw := buildValidFooterBytes()
	require.Equal(t, FOOTER_SIZE, len(raw))

	footer, err := NewInMemoryFooterFromBytes(ver, raw)
	require.NoError(t, err)
	require.Equal(t, ver, footer.Version)
	require.Equal(t, int64(123456789), footer.Timestamp)
	require.Equal(t, uint32(3), footer.Count)
	require.Equal(t, uint64(512), footer.IndexOffset)
	require.Equal(t, objects.MAC{1, 2, 3, 4}, footer.IndexMAC)
	require.Equal(t, uint32(0), footer.Flags)
}

// TestNewInMemoryFooterFromBytesShort confirms an error is returned when fewer
// than FOOTER_SIZE bytes are provided.
func TestNewInMemoryFooterFromBytesShort(t *testing.T) {
	ver := versioning.FromString(VERSION)
	// 7 bytes — too short to read even the Timestamp (8 bytes).
	_, err := NewInMemoryFooterFromBytes(ver, make([]byte, 7))
	require.Error(t, err)
}

// TestNewInMemoryFooterFromBytesMissingFields exercises mid-buffer truncation:
// provide enough bytes for Timestamp but not for Count.
func TestNewInMemoryFooterFromBytesMissingCount(t *testing.T) {
	ver := versioning.FromString(VERSION)
	// Only 8 bytes (Timestamp ok, Count missing).
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, int64(99))
	_, err := NewInMemoryFooterFromBytes(ver, buf.Bytes())
	require.Error(t, err)
}

// unwrapSerializedPackfile takes the output of PackfileInMemory.Serialize (with
// an identity encoder) and reconstructs the flat byte slice that
// NewPackfileInMemoryFromBytes expects: blobs | raw_index | raw_footer.
//
// The Serialize output layout is:
//   blobs | encoded_index | encoded_footer | uint32(encoded_footer_len)
//
// With an identity encoder "encoded" == "raw", so we just strip the trailing 4 bytes
// that hold the footer length, then strip the footer bytes, then strip the index bytes.
// We then reassemble as: blobs | raw_index | raw_footer.
func unwrapSerializedPackfile(t *testing.T, raw []byte, footer PackfileInMemoryFooter) []byte {
	t.Helper()

	// The last 4 bytes are the encoded footer length.
	footerLen := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	raw = raw[:len(raw)-4]

	// The last footerLen bytes are the encoded footer.
	footerBuf := make([]byte, footerLen)
	copy(footerBuf, raw[len(raw)-int(footerLen):])
	raw = raw[:len(raw)-int(footerLen)]

	// What remains is: blobs (IndexOffset bytes) | index.
	// IndexOffset is set by AddBlob to reflect the total size of blob data.
	blobData := raw[:footer.IndexOffset]
	indexBuf := raw[footer.IndexOffset:]

	result := make([]byte, 0, len(blobData)+len(indexBuf)+len(footerBuf))
	result = append(result, blobData...)
	result = append(result, indexBuf...)
	result = append(result, footerBuf...)
	return result
}

// TestNewPackfileInMemoryFromBytesRoundtrip builds a PackfileInMemory, serialises
// it with an identity encoder, unwraps the output into the format that
// NewPackfileInMemoryFromBytes understands, and asserts the blobs survive intact.
func TestNewPackfileInMemoryFromBytesRoundtrip(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	blob1 := []byte("roundtrip blob one")
	blob2 := []byte("roundtrip blob two")
	mac1 := objects.MAC{0xAA}
	mac2 := objects.MAC{0xBB}
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)

	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac1, blob1, 0))
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, mac2, blob2, 0))

	// Capture footer before Serialize resets the hasher.
	footer := p.Footer

	// Serialise with identity encoder (no compression / encryption).
	reader, fileMac, err := p.Serialize(func(r io.Reader) (io.Reader, error) { return r, nil })
	require.NoError(t, err)
	require.NotEqual(t, objects.NilMac, fileMac)

	serialized, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NotEmpty(t, serialized)

	// Unwrap into the format expected by NewPackfileInMemoryFromBytes.
	// After Serialize the footer.IndexMAC is set; refresh from p.Footer.
	footer = p.Footer
	unwrapped := unwrapSerializedPackfile(t, serialized, footer)

	// Deserialise.
	hasher2 := sha256.New()
	p2, err := NewPackfileInMemoryFromBytes(hasher2, versioning.FromString(VERSION), unwrapped)
	require.NoError(t, err)
	require.NotNil(t, p2)

	// Verify blob count and data.
	require.Equal(t, uint32(2), p2.Footer.Count)
	require.Len(t, p2.Index, 2)

	got1, ok := p2.getBlob(mac1)
	require.True(t, ok, "mac1 should be found after round-trip")
	require.Equal(t, blob1, got1)

	got2, ok := p2.getBlob(mac2)
	require.True(t, ok, "mac2 should be found after round-trip")
	require.Equal(t, blob2, got2)
}

// TestNewPackfileInMemoryFromBytesTooShort ensures that a buffer too small to
// contain a valid footer results in an error.
func TestNewPackfileInMemoryFromBytesTooShort(t *testing.T) {
	// Fewer bytes than FOOTER_SIZE — Seek(-FOOTER_SIZE, SeekEnd) will fail or
	// binary.Read will return an error.
	_, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), []byte("tiny"))
	require.Error(t, err)
}

// TestPackfileInMemorySerializeEmptyRoundtrip checks that an empty packfile
// survives a full serialise / deserialise cycle without error.
func TestPackfileInMemorySerializeEmptyRoundtrip(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	footer := p.Footer

	reader, _, err := p.Serialize(func(r io.Reader) (io.Reader, error) { return r, nil })
	require.NoError(t, err)

	serialized, err := io.ReadAll(reader)
	require.NoError(t, err)

	footer = p.Footer
	unwrapped := unwrapSerializedPackfile(t, serialized, footer)

	hasher2 := sha256.New()
	p2, err := NewPackfileInMemoryFromBytes(hasher2, versioning.FromString(VERSION), unwrapped)
	require.NoError(t, err)
	require.NotNil(t, p2)
	require.Equal(t, uint32(0), p2.Footer.Count)
	require.Empty(t, p2.Index)
}
