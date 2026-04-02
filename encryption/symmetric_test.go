package encryption_test

import (
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/compression"
	enc "github.com/PlakarKorp/kloset/encryption"
	"github.com/stretchr/testify/require"
)

type SymmetricParams struct {
	config *enc.Configuration
	key    []byte
}

func TestNewDefaultKDFParams(t *testing.T) {
	t.Run("ARGON2ID", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams("ARGON2ID")
		require.NoError(t, err)
		require.NotNil(t, params)

		require.Equal(t, "ARGON2ID", params.KDF)
		require.NotNil(t, params.Salt)
		require.Len(t, params.Salt, 16)

		require.NotNil(t, params.Argon2idParams)
		require.EqualValues(t, 16, params.Argon2idParams.SaltSize)
		require.EqualValues(t, 4, params.Argon2idParams.Time)
		require.EqualValues(t, 256*1024, params.Argon2idParams.Memory)
		require.EqualValues(t, 1, params.Argon2idParams.Threads)
		require.EqualValues(t, 32, params.Argon2idParams.KeyLen)

		require.Nil(t, params.ScryptParams)
		require.Nil(t, params.Pbkdf2Params)
	})

	t.Run("SCRYPT", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams("SCRYPT")
		require.NoError(t, err)
		require.NotNil(t, params)

		require.Equal(t, "SCRYPT", params.KDF)
		require.NotNil(t, params.Salt)
		require.Len(t, params.Salt, 16)

		require.NotNil(t, params.ScryptParams)
		require.EqualValues(t, 16, params.ScryptParams.SaltSize)
		require.Equal(t, 1<<15, params.ScryptParams.N)
		require.Equal(t, 8, params.ScryptParams.R)
		require.Equal(t, 1, params.ScryptParams.P)
		require.Equal(t, 32, params.ScryptParams.KeyLen)

		require.Nil(t, params.Argon2idParams)
		require.Nil(t, params.Pbkdf2Params)
	})

	t.Run("PBKDF2", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams("PBKDF2")
		require.NoError(t, err)
		require.NotNil(t, params)

		require.Equal(t, "PBKDF2", params.KDF)
		require.NotNil(t, params.Salt)
		require.Len(t, params.Salt, 16)

		require.NotNil(t, params.Pbkdf2Params)
		require.EqualValues(t, 16, params.Pbkdf2Params.SaltSize)
		require.Equal(t, 100000, params.Pbkdf2Params.Iterations)
		require.Equal(t, 32, params.Pbkdf2Params.KeyLen)
		require.Equal(t, "SHA256", params.Pbkdf2Params.Hashing)

		require.Nil(t, params.Argon2idParams)
		require.Nil(t, params.ScryptParams)
	})

	t.Run("UnsupportedKDF", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams("NOPE")
		require.Error(t, err)
		require.Nil(t, params)
	})
}

func testSetup(t *testing.T, hashing string) SymmetricParams {
	config := enc.NewConfiguration(hashing)

	salt := make([]byte, uint32(16))
	if _, err := rand.Read(salt); err != nil {
		t.Fatalf("Failed to generate random salt: %v", err)
	}
	config.KDFParams.Salt = salt

	passphrase := []byte("strong passphrase")
	key, err := enc.DeriveKey(config.KDFParams, passphrase)
	if err != nil {
		t.Fatalf("Failed to derive key from passphrase: %v", err)
	}

	params := SymmetricParams{config, key}
	return params
}

func TestDeriveKey(t *testing.T) {
	testCases := []SymmetricParams{
		testSetup(t, enc.DEFAULT_KDF),
		testSetup(t, "SCRYPT"),
		testSetup(t, "PBKDF2"),
	}

	for _, tc := range testCases {
		t.Run(tc.config.KDFParams.KDF, func(t *testing.T) {
			// Verify that derived key is non-nil and of expected length
			if tc.key == nil || len(tc.key) != 32 {
				t.Errorf("Unexpected derived key length. Got %d, want 32", len(tc.key))
			}
		})
	}
}

func TestEncryptDecryptStream(t *testing.T) {
	params := testSetup(t, enc.DEFAULT_KDF)

	// Original data to encrypt and decrypt
	originalData := "This is a test data string for encryption and decryption"
	r := strings.NewReader(originalData)

	// Encrypt the data
	encryptedReader, err := enc.EncryptStream(params.config, params.key, r)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Decrypt the data
	decryptedReader, err := enc.DecryptStream(params.config, params.key, io.NopCloser(encryptedReader))
	if err != nil {
		t.Fatalf("Failed to decrypt data: %v", err)
	}

	// Read the decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data matches the original data
	if string(decryptedData) != originalData {
		t.Errorf("Decrypted data does not match original. Got: %q, want: %q", string(decryptedData), originalData)
	}
}

func TestEncryptDecryptEmptyStream(t *testing.T) {
	params := testSetup(t, enc.DEFAULT_KDF)

	// Original data to encrypt and decrypt
	originalData := ""
	r := strings.NewReader(originalData)

	// Encrypt the data
	encryptedReader, err := enc.EncryptStream(params.config, params.key, r)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Decrypt the data
	decryptedReader, err := enc.DecryptStream(params.config, params.key, io.NopCloser(encryptedReader))
	if err != nil {
		t.Fatalf("Failed to decrypt data: %v", err)
	}

	// Read the decrypted data
	decryptedData, err := io.ReadAll(decryptedReader)
	if err != nil {
		t.Fatalf("Failed to read decrypted data: %v", err)
	}

	// Verify the decrypted data matches the original data
	if string(decryptedData) != originalData {
		t.Errorf("Decrypted data does not match original. Got: %q, want: %q", string(decryptedData), originalData)
	}
}

func TestEncryptDecryptStreamWithIncorrectKey(t *testing.T) {
	params := testSetup(t, enc.DEFAULT_KDF)

	// Original data to encrypt and decrypt
	originalData := "Sensitive information to protect"
	r := strings.NewReader(originalData)

	// Encrypt the data
	encryptedReader, err := enc.EncryptStream(params.config, params.key, r)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Generate an incorrect key for decryption
	incorrectKey := make([]byte, len(params.key))
	if _, err := rand.Read(incorrectKey); err != nil {
		t.Fatalf("Failed to generate incorrect decryption key: %v", err)
	}

	// Attempt to decrypt the data with the incorrect key
	decryptedReader, err := enc.DecryptStream(params.config, incorrectKey, io.NopCloser(encryptedReader))
	if err == nil {
		// Attempt to read the (likely) invalid decrypted data to trigger an error
		if _, readErr := io.ReadAll(decryptedReader); readErr == nil {
			t.Error("Expected error during decryption with incorrect key, but got none")
		}
	} else {
		t.Logf("Decryption failed as expected with incorrect key: %v", err)
	}
}

func TestCompressEncryptThenDecryptDecompressStream(t *testing.T) {
	params := testSetup(t, enc.DEFAULT_KDF)

	// Original data to compress, encrypt, decrypt, and decompress
	originalData := "This is a test string for compression and encryption. It should work!"
	r := strings.NewReader(originalData)

	// Step 1: Compress the data
	compressedReader, err := compression.DeflateStream("GZIP", r)
	if err != nil {
		t.Fatalf("Failed to compress data: %v", err)
	}

	// Step 2: Encrypt the compressed data
	encryptedReader, err := enc.EncryptStream(params.config, params.key, compressedReader)
	if err != nil {
		t.Fatalf("Failed to encrypt data: %v", err)
	}

	// Step 3: Decrypt the data
	decryptedReader, err := enc.DecryptStream(params.config, params.key, io.NopCloser(encryptedReader))
	if err != nil {
		t.Fatalf("Failed to decrypt data: %v", err)
	}

	// Step 4: Decompress the decrypted data
	decompressedReader, err := compression.InflateStream("GZIP", decryptedReader)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	// Read the final output
	finalData, err := io.ReadAll(decompressedReader)
	if err != nil {
		t.Fatalf("Failed to read decompressed data: %v", err)
	}

	// Verify the final data matches the original data
	if string(finalData) != originalData {
		t.Errorf("Final data does not match original. Got: %q, want: %q", string(finalData), originalData)
	}
}
