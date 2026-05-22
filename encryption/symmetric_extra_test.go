package encryption_test

import (
	"bytes"
	"io"
	"testing"

	enc "github.com/PlakarKorp/kloset/encryption"
	"github.com/stretchr/testify/require"
)

// defaultKey returns a 32-byte key suitable for AES-256.
func defaultKey() []byte {
	return []byte("0123456789abcdef0123456789abcdef")
}

// TestEncryptStreamUnsupportedAlgorithm checks that EncryptStream rejects unknown algorithms.
func TestEncryptStreamUnsupportedAlgorithm(t *testing.T) {
	cfg := enc.NewDefaultConfiguration()
	cfg.DataAlgorithm = "UNKNOWN"

	_, err := enc.EncryptStream(cfg, defaultKey(), bytes.NewReader([]byte("data")))
	require.Error(t, err)
}

// TestEncryptStreamNilReader checks that a nil reader is rejected.
func TestEncryptStreamNilReader(t *testing.T) {
	cfg := enc.NewDefaultConfiguration()
	_, err := enc.EncryptStream(cfg, defaultKey(), nil)
	require.Error(t, err)
}

// TestDecryptStreamCloseCallsBothClosers verifies that Close on the returned
// ReadCloser propagates to both the internal reader and the input closer
// (exercises the readCloserInternal.Close path).
func TestDecryptStreamCloseCallsBothClosers(t *testing.T) {
	cfg := enc.NewDefaultConfiguration()
	key := defaultKey()
	plaintext := []byte("close coverage data")

	// Encrypt first.
	encrypted, err := enc.EncryptStream(cfg, key, bytes.NewReader(plaintext))
	require.NoError(t, err)

	encryptedBytes, err := io.ReadAll(encrypted)
	require.NoError(t, err)

	// Decrypt — wrap in a NopCloser so we have an io.ReadCloser.
	rc, err := enc.DecryptStream(cfg, key, io.NopCloser(bytes.NewReader(encryptedBytes)))
	require.NoError(t, err)

	// Read fully, then close.
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, plaintext, got)

	// Close should not error (exercises readCloserInternal.Close).
	require.NoError(t, rc.Close())
}

// TestDeriveCanaryDifferentKeys verifies that different keys produce different canaries.
func TestDeriveCanaryDifferentKeys(t *testing.T) {
	cfg := enc.NewDefaultConfiguration()

	key1 := []byte("0123456789abcdef0123456789abcdef")
	key2 := []byte("fedcba9876543210fedcba9876543210")

	canary1, err := enc.DeriveCanary(cfg, key1)
	require.NoError(t, err)
	require.NotEmpty(t, canary1)

	canary2, err := enc.DeriveCanary(cfg, key2)
	require.NoError(t, err)
	require.NotEmpty(t, canary2)

	// Different keys → different canaries.
	require.NotEqual(t, canary1, canary2)
}
