package hashing

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetHasherSHA256(t *testing.T) {
	h := GetHasher("SHA256")
	require.NotNil(t, h)

	h.Write([]byte("hello"))
	sum := h.Sum(nil)
	require.Equal(t, 32, len(sum))
}

func TestGetHasherBLAKE3(t *testing.T) {
	h := GetHasher("BLAKE3")
	require.NotNil(t, h)

	h.Write([]byte("hello"))
	sum := h.Sum(nil)
	require.Equal(t, 32, len(sum))
}

func TestGetHasherUnknown(t *testing.T) {
	h := GetHasher("MD5")
	require.Nil(t, h)
}

func TestGetHasherEmpty(t *testing.T) {
	h := GetHasher("")
	require.Nil(t, h)
}

func TestGetHasherDifferentAlgorithmsProduceDifferentHashes(t *testing.T) {
	data := []byte("test data")

	h1 := GetHasher("SHA256")
	h1.Write(data)
	sum1 := h1.Sum(nil)

	h2 := GetHasher("BLAKE3")
	h2.Write(data)
	sum2 := h2.Sum(nil)

	require.NotEqual(t, sum1, sum2, "SHA256 and BLAKE3 should produce different hashes")
}

func TestGetMACHasherSHA256(t *testing.T) {
	secret := make([]byte, 32)
	h := GetMACHasher("SHA256", secret)
	require.NotNil(t, h)

	h.Write([]byte("message"))
	sum := h.Sum(nil)
	require.Equal(t, 32, len(sum))
}

func TestGetMACHasherBLAKE3(t *testing.T) {
	secret := make([]byte, 32)
	h := GetMACHasher("BLAKE3", secret)
	require.NotNil(t, h)

	h.Write([]byte("message"))
	sum := h.Sum(nil)
	require.Equal(t, 32, len(sum))
}

func TestGetMACHasherUnknown(t *testing.T) {
	h := GetMACHasher("MD5", []byte("key"))
	require.Nil(t, h)
}

func TestGetMACHasherDeterministic(t *testing.T) {
	secret := []byte("supersecretkey!!")
	secret32 := make([]byte, 32)
	copy(secret32, secret)

	data := []byte("hello")

	h1 := GetMACHasher("SHA256", secret32)
	h1.Write(data)
	sum1 := h1.Sum(nil)

	h2 := GetMACHasher("SHA256", secret32)
	h2.Write(data)
	sum2 := h2.Sum(nil)

	require.Equal(t, sum1, sum2, "MAC should be deterministic")
}

func TestGetMACHasherDifferentKeysProduceDifferentMACs(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	key2[0] = 1

	data := []byte("same data")

	h1 := GetMACHasher("BLAKE3", key1)
	h1.Write(data)
	sum1 := h1.Sum(nil)

	h2 := GetMACHasher("BLAKE3", key2)
	h2.Write(data)
	sum2 := h2.Sum(nil)

	require.NotEqual(t, sum1, sum2, "different keys should produce different MACs")
}

func TestLookupDefaultConfigurationSHA256(t *testing.T) {
	cfg, err := LookupDefaultConfiguration("SHA256")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "SHA256", cfg.Algorithm)
	require.Equal(t, uint32(256), cfg.Bits)
}

func TestNewDefaultConfiguration(t *testing.T) {
	cfg := NewDefaultConfiguration()
	require.NotNil(t, cfg)
	require.Equal(t, DEFAULT_HASHING_ALGORITHM, cfg.Algorithm)
}

func TestHasherReset(t *testing.T) {
	h := GetHasher("SHA256")
	h.Write([]byte("hello"))
	h.Reset()
	h.Write([]byte("world"))
	sum1 := h.Sum(nil)

	h2 := GetHasher("SHA256")
	h2.Write([]byte("world"))
	sum2 := h2.Sum(nil)

	require.Equal(t, sum1, sum2, "reset should clear prior state")
}
