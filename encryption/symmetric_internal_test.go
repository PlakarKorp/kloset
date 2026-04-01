package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"testing"

	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (key []byte, subkey []byte, nonceSize int) {
	t.Helper()

	key = []byte("0123456789abcdef0123456789abcdef")
	subkey = []byte("abcdefghijklmnopqrstuvwxyz012345")

	block, err := aes.NewCipher(key)
	require.NoError(t, err)

	gcm, err := cipher.NewGCM(block)
	require.NoError(t, err)

	return key, subkey, gcm.NonceSize()
}

func Test_decryptSubkey_AES256_GCM(t *testing.T) {
	key, subkey, nonceSize := setup(t)

	t.Run("InvalidKey", func(t *testing.T) {
		decrypted, err := decryptSubkey_AES256_GCM([]byte("short"), bytes.NewReader([]byte("data")))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("ShortNonce", func(t *testing.T) {
		decrypted, err := decryptSubkey_AES256_GCM(key, bytes.NewReader(make([]byte, nonceSize-1)))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("TruncatedCiphertext", func(t *testing.T) {
		encrypted, err := encryptSubkey_AES256_GCM(key, subkey)
		require.NoError(t, err)
		require.NotNil(t, encrypted)
		require.Greater(t, len(encrypted), nonceSize)

		decrypted, err := decryptSubkey_AES256_GCM(key, bytes.NewReader(encrypted[:nonceSize+1]))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("CorruptedCiphertext", func(t *testing.T) {
		encrypted, err := encryptSubkey_AES256_GCM(key, subkey)
		require.NoError(t, err)
		require.NotNil(t, encrypted)

		encrypted[nonceSize] ^= 0xff

		decrypted, err := decryptSubkey_AES256_GCM(key, bytes.NewReader(encrypted))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})
}

func Test_encryptSubkey_AES256_KW(t *testing.T) {
	key, _, _ := setup(t)

	t.Run("InvalidSubkeyLength", func(t *testing.T) {
		encrypted, err := encryptSubkey_AES256_KW(key, bytes.Repeat([]byte{0x01}, 31))
		require.Error(t, err)
		require.Nil(t, encrypted)
	})
}
