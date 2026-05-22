package objects

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRandomMAC(t *testing.T) {
	m1 := RandomMAC()
	m2 := RandomMAC()

	// Should be non-zero
	require.NotEqual(t, NilMac, m1)
	require.NotEqual(t, NilMac, m2)
	// Two random MACs should differ (with overwhelming probability)
	require.NotEqual(t, m1, m2)
}

func TestMACFormatHex(t *testing.T) {
	m := MAC{0x01, 0x02, 0x03}
	hex := m.FormatHex()
	require.Equal(t, 64, len(hex))
	require.Contains(t, hex, "010203")
}

func TestMACParseHex(t *testing.T) {
	original := MAC{1, 2, 3, 4}
	hexStr := original.FormatHex()

	var parsed MAC
	require.NoError(t, parsed.ParseHex(hexStr))
	require.Equal(t, original, parsed)
}

func TestMACParseHexInvalid(t *testing.T) {
	var m MAC
	require.Error(t, m.ParseHex("not-hex"))
}

func TestMACParseHexWrongLength(t *testing.T) {
	var m MAC
	require.Error(t, m.ParseHex("0102"))
}

func TestMACJSONRoundtrip(t *testing.T) {
	original := MAC{5, 6, 7, 8, 9}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored MAC
	require.NoError(t, json.Unmarshal(data, &restored))
	require.Equal(t, original, restored)
}

func TestMACUnmarshalJSONBadJSON(t *testing.T) {
	var m MAC
	require.Error(t, json.Unmarshal([]byte(`12345`), &m))
}

func TestNewChunk(t *testing.T) {
	c := NewChunk()
	require.NotNil(t, c)
	require.Equal(t, NilMac, c.ContentMAC)
	require.Equal(t, uint32(0), c.Length)
	require.Equal(t, float64(0), c.Entropy)
}

func TestChunkSerializeRoundtrip(t *testing.T) {
	c := NewChunk()
	c.ContentMAC = MAC{1, 2, 3}
	c.Length = 1024
	c.Entropy = 0.75

	data, err := c.Serialize()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	restored, err := NewChunkFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, c.ContentMAC, restored.ContentMAC)
	require.Equal(t, c.Length, restored.Length)
	require.InDelta(t, c.Entropy, restored.Entropy, 0.001)
}

func TestNewChunkFromBytesInvalid(t *testing.T) {
	_, err := NewChunkFromBytes([]byte{0x01, 0x02, 0x03})
	require.Error(t, err)
}

func TestChunkMarshalJSON(t *testing.T) {
	c := NewChunk()
	c.ContentMAC = MAC{10, 11, 12}
	c.Length = 512

	data, err := json.Marshal(c)
	require.NoError(t, err)
	require.NotEmpty(t, data)
}

func TestObjectSize(t *testing.T) {
	o := NewObject()
	require.Equal(t, int64(0), o.Size())

	o.Chunks = []Chunk{
		{Length: 100},
		{Length: 200},
		{Length: 50},
	}
	require.Equal(t, int64(350), o.Size())
}

func TestObjectSerializeRoundtrip(t *testing.T) {
	o := NewObject()
	o.ContentMAC = MAC{20, 21, 22}
	o.ContentType = "application/octet-stream"
	o.Entropy = 0.5
	o.Chunks = []Chunk{
		{Length: 512, ContentMAC: MAC{1}},
		{Length: 1024, ContentMAC: MAC{2}},
	}

	data, err := o.Serialize()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	restored, err := NewObjectFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, o.ContentMAC, restored.ContentMAC)
	require.Equal(t, o.ContentType, restored.ContentType)
	require.InDelta(t, o.Entropy, restored.Entropy, 0.001)
	require.Len(t, restored.Chunks, 2)
}

func TestNewObjectFromBytesInvalid(t *testing.T) {
	_, err := NewObjectFromBytes([]byte{0xFF, 0xFE})
	require.Error(t, err)
}

func TestCachedPathRoundtrip(t *testing.T) {
	cp := &CachedPath{
		MAC:         MAC{30},
		ObjectMAC:   MAC{31},
		ContentType: "text/plain",
		Chunks:      5,
		Entropy:     0.3,
	}

	data, err := cp.Serialize()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	restored, err := NewCachedPathFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, cp.MAC, restored.MAC)
	require.Equal(t, cp.ContentType, restored.ContentType)
	require.Equal(t, cp.Chunks, restored.Chunks)
}

func TestNewCachedPathFromBytesInvalid(t *testing.T) {
	_, err := NewCachedPathFromBytes([]byte{0x01})
	require.Error(t, err)
}

func TestCachedPathStat(t *testing.T) {
	cp := &CachedPath{
		FileInfo: FileInfo{Lname: "test.txt"},
	}
	fi := cp.Stat()
	require.NotNil(t, fi)
	require.Equal(t, "test.txt", fi.Lname)
}
