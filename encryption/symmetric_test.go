package encryption_test

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/PlakarKorp/kloset/compression"
	enc "github.com/PlakarKorp/kloset/encryption"
	"github.com/stretchr/testify/require"
)

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

func compareConfigParams(
	t *testing.T,
	config *enc.Configuration,
	params *enc.KDFParams,
) {
	t.Helper()

	require.Equal(t, "AES256-KW", config.SubKeyAlgorithm)
	require.Equal(t, "AES256-GCM-SIV", config.DataAlgorithm)
	require.Equal(t, 64*1024, config.ChunkSize)

	require.Equal(t, params.KDF, config.KDFParams.KDF)
	require.Len(t, config.KDFParams.Salt, len(params.Salt))
	require.NotEqual(t, params.Salt, config.KDFParams.Salt)
	require.Equal(t, params.Argon2idParams, config.KDFParams.Argon2idParams)
	require.Equal(t, params.ScryptParams, config.KDFParams.ScryptParams)
	require.Equal(t, params.Pbkdf2Params, config.KDFParams.Pbkdf2Params)
}

func TestNewConfiguration(t *testing.T) {
	testCases := []string{
		"ARGON2ID",
		"SCRYPT",
		"PBKDF2",
	}

	for _, tc := range testCases {
		t.Run(tc, func(t *testing.T) {
			params, err := enc.NewDefaultKDFParams(tc)
			require.NoError(t, err)

			config := enc.NewConfiguration(tc)
			require.NotNil(t, config)

			compareConfigParams(t, config, params)
		})
	}

	t.Run("UnsupportedKDF", func(t *testing.T) {
		require.Panics(t, func() {
			enc.NewConfiguration("NOPE")
		})
	})
}

func TestNewDefaultConfiguration(t *testing.T) {
	t.Run("DefaultKDF", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams(enc.DEFAULT_KDF)
		require.NoError(t, err)

		config := enc.NewDefaultConfiguration()
		require.NotNil(t, config)

		compareConfigParams(t, config, params)
	})

	t.Run("vs_NewConfiguration", func(t *testing.T) {
		defaultConfig := enc.NewDefaultConfiguration()
		config := enc.NewConfiguration(enc.DEFAULT_KDF)

		require.NotNil(t, defaultConfig)
		require.NotNil(t, config)

		require.Equal(t, config.SubKeyAlgorithm, defaultConfig.SubKeyAlgorithm)
		require.Equal(t, config.DataAlgorithm, defaultConfig.DataAlgorithm)
		require.Equal(t, config.ChunkSize, defaultConfig.ChunkSize)

		require.Equal(t, config.KDFParams.KDF, defaultConfig.KDFParams.KDF)
		require.Len(t, defaultConfig.KDFParams.Salt, len(config.KDFParams.Salt))
		require.NotEqual(t, defaultConfig.KDFParams.Salt, config.KDFParams.Salt)
		require.Equal(t, config.KDFParams.Argon2idParams, defaultConfig.KDFParams.Argon2idParams)
		require.Equal(t, config.KDFParams.ScryptParams, defaultConfig.KDFParams.ScryptParams)
		require.Equal(t, config.KDFParams.Pbkdf2Params, defaultConfig.KDFParams.Pbkdf2Params)
	})
}

func TestDeriveKey(t *testing.T) {
	assertKey := func(t *testing.T, kdf string, passphrase []byte) []byte {
		t.Helper()

		params, err := enc.NewDefaultKDFParams(kdf)
		require.NoError(t, err)
		require.NotNil(t, params)

		key, err := enc.DeriveKey(*params, passphrase)
		require.NoError(t, err)
		require.NotNil(t, key)
		require.Len(t, key, 32)

		return key
	}

	testCases := []string{
		"ARGON2ID",
		"SCRYPT",
		"PBKDF2",
	}

	for _, tc := range testCases {
		t.Run("ValidKey:"+tc, func(t *testing.T) {
			assertKey(t, tc, []byte("strong passphrase"))
		})
	}

	t.Run("NilPassphraseIsValid", func(t *testing.T) {
		assertKey(t, "SCRYPT", nil)
	})

	t.Run("SameParamsAndPassphrase_SameKey", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams("ARGON2ID")
		require.NoError(t, err)

		passphrase := []byte("strong passphrase")

		first, err := enc.DeriveKey(*params, passphrase)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.Len(t, first, 32)

		second, err := enc.DeriveKey(*params, passphrase)
		require.NoError(t, err)
		require.NotNil(t, second)
		require.Len(t, second, 32)

		require.Equal(t, first, second)
	})

	t.Run("DifferentPassphrases_DifferentKeys", func(t *testing.T) {
		params, err := enc.NewDefaultKDFParams("ARGON2ID")
		require.NoError(t, err)

		first, err := enc.DeriveKey(*params, []byte("strong passphrase"))
		require.NoError(t, err)
		require.NotNil(t, first)
		require.Len(t, first, 32)

		second, err := enc.DeriveKey(*params, []byte("different passphrase"))
		require.NoError(t, err)
		require.NotNil(t, second)
		require.Len(t, second, 32)

		require.NotEqual(t, first, second)
	})

	t.Run("DifferentSalts_DifferentKeys", func(t *testing.T) {
		firstParams, err := enc.NewDefaultKDFParams("ARGON2ID")
		require.NoError(t, err)

		secondParams, err := enc.NewDefaultKDFParams("ARGON2ID")
		require.NoError(t, err)

		passphrase := []byte("strong passphrase")

		first, err := enc.DeriveKey(*firstParams, passphrase)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.Len(t, first, 32)

		second, err := enc.DeriveKey(*secondParams, passphrase)
		require.NoError(t, err)
		require.NotNil(t, second)
		require.Len(t, second, 32)

		require.NotEqual(t, first, second)
	})

	t.Run("UnsupportedKDF", func(t *testing.T) {
		params := enc.KDFParams{
			KDF:  "NOPE",
			Salt: []byte("0123456789abcdef"),
		}

		key, err := enc.DeriveKey(params, []byte("strong passphrase"))
		require.Error(t, err)
		require.Nil(t, key)
	})
}

func TestEncryptSubkey(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	subkey := []byte("abcdefghijklmnopqrstuvwxyz012345")

	t.Run("AES256-GCM", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", key, subkey)
		require.NoError(t, err)
		require.NotNil(t, encrypted)
		require.NotEmpty(t, encrypted)
	})

	t.Run("AES256-KW", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-KW", key, subkey)
		require.NoError(t, err)
		require.NotNil(t, encrypted)
		require.NotEmpty(t, encrypted)
	})

	t.Run("NilKeyIsInvalid", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", nil, subkey)
		require.Error(t, err)
		require.Nil(t, encrypted)
	})

	t.Run("NilSubKeyIsValid", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", key, nil)
		require.NoError(t, err)
		require.NotNil(t, encrypted)
		require.NotEmpty(t, encrypted)
	})

	t.Run("InvalidKey", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", []byte("short"), subkey)
		require.Error(t, err)
		require.Nil(t, encrypted)
	})

	t.Run("InvaliSubdKeyIsValid", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", key, []byte("short"))
		require.NoError(t, err)
		require.NotNil(t, encrypted)
	})

	t.Run("SameAlgorithmAndKey_DifferentSubKey", func(t *testing.T) {
		first, err := enc.EncryptSubkey("AES256-GCM", key, subkey)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.NotEmpty(t, first)

		second, err := enc.EncryptSubkey("AES256-GCM", key, subkey)
		require.NoError(t, err)
		require.NotNil(t, second)
		require.NotEmpty(t, second)

		require.NotEqual(t, first, second)
	})

	t.Run("UnsupportedAlgorithm", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("NOPE", key, subkey)
		require.Error(t, err)
		require.Nil(t, encrypted)
	})
}

func TestDecryptSubkey(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	subkey := []byte("abcdefghijklmnopqrstuvwxyz012345")

	t.Run("AES256-GCM", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", key, subkey)
		require.NoError(t, err)

		decrypted, err := enc.DecryptSubkey("AES256-GCM", key, bytes.NewReader(encrypted))
		require.NoError(t, err)
		require.NotNil(t, decrypted)
		require.Equal(t, subkey, decrypted)
	})

	t.Run("AES256-KW", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-KW", key, subkey)
		require.NoError(t, err)

		decrypted, err := enc.DecryptSubkey("AES256-KW", key, bytes.NewReader(encrypted))
		require.NoError(t, err)
		require.NotNil(t, decrypted)
		require.Equal(t, subkey, decrypted)
	})

	t.Run("NilKeyIsInvalid", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-KW", key, subkey)
		require.NoError(t, err)

		decrypted, err := enc.DecryptSubkey("AES256-KW", nil, bytes.NewReader(encrypted))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("NilSubKeyIsInvalid", func(t *testing.T) {
		require.Panics(t, func() { enc.DecryptSubkey("AES256-GCM", key, nil) })
	})

	t.Run("IncorrectKey", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", key, subkey)
		require.NoError(t, err)

		incorrectKey := []byte("fedcba9876543210fedcba9876543210")
		decrypted, err := enc.DecryptSubkey("AES256-GCM", incorrectKey, bytes.NewReader(encrypted))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("IncorrectSubKey", func(t *testing.T) {
		decrypted, err := enc.DecryptSubkey("AES256-GCM", key, bytes.NewReader([]byte("whatever")))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("UnsupportedAlgorithm", func(t *testing.T) {
		decrypted, err := enc.DecryptSubkey("NOPE", key, bytes.NewReader([]byte("whatever")))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("TruncatedData", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-KW", key, subkey)
		require.NoError(t, err)

		decrypted, err := enc.DecryptSubkey("AES256-KW", key, bytes.NewReader(encrypted[:len(encrypted)-4]))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})

	t.Run("TruncatedAfterNonce_AES256_GCM", func(t *testing.T) {
		encrypted, err := enc.EncryptSubkey("AES256-GCM", key, subkey)
		require.NoError(t, err)
		require.NotNil(t, encrypted)

		const nonceSize = 12
		require.Greater(t, len(encrypted), nonceSize)

		decrypted, err := enc.DecryptSubkey("AES256-GCM", key, bytes.NewReader(encrypted[:nonceSize+1]))
		require.Error(t, err)
		require.Nil(t, decrypted)
	})
}

type Params_t struct {
	config *enc.Configuration
	key    []byte
}

func setup(t *testing.T) *Params_t {
	t.Helper()
	return &Params_t{
		config: enc.NewDefaultConfiguration(),
		key:    []byte("0123456789abcdef0123456789abcdef"),
	}
}

func TestEncryptStream(t *testing.T) {
	t.Run("ValidStream", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, params.key, strings.NewReader("hello world"))
		require.NoError(t, err)
		require.NotNil(t, rd)

		data, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.NotEmpty(t, data)
	})

	t.Run("EmptyStreamIsValid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, params.key, strings.NewReader(""))
		require.NoError(t, err)
		require.NotNil(t, rd)

		data, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.NotNil(t, data)
	})

	t.Run("ConfigNilIsInvalid", func(t *testing.T) {
		require.Panics(t, func() {
			enc.EncryptStream(nil, []byte("0123456789abcdef0123456789abcdef"), strings.NewReader("hello"))
		})
	})

	t.Run("UnsupportedDataAlgorithmIsInvalid", func(t *testing.T) {
		params := setup(t)
		params.config.DataAlgorithm = "NOPE"

		rd, err := enc.EncryptStream(params.config, params.key, strings.NewReader("hello world"))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("UnsupportedSubKeyAlgorithmIsInvalid", func(t *testing.T) {
		params := setup(t)
		params.config.SubKeyAlgorithm = "NOPE"

		rd, err := enc.EncryptStream(params.config, params.key, strings.NewReader("hello world"))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyNilIsInvalid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, nil, strings.NewReader("hello world"))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyEmptyIsInvalid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, []byte{}, strings.NewReader("hello world"))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyTooShortIsInvalid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, []byte("short"), strings.NewReader("hello world"))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyTooLongIsInvalid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, []byte("0123456789abcdef0123456789abcdefx"), strings.NewReader("hello world"))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("Fails_IfReaderIsInvalid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.EncryptStream(params.config, params.key, nil)
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("Fails_IfReaderFails", func(t *testing.T) {
		params := setup(t)
		readErr := errors.New("reader failed")

		rd, err := enc.EncryptStream(params.config, params.key, iotest.ErrReader(readErr))
		require.NoError(t, err)
		require.NotNil(t, rd)

		data, err := io.ReadAll(rd)
		require.ErrorIs(t, err, readErr)
		// EncryptStream writes the encrypted subkey header before reading from the
		// source reader, so partial output is expected even when the reader later fails.
		require.NotEmpty(t, data)
	})
}

type errAfterNReadCloser struct {
	data []byte
	n    int
	err  error
	pos  int
}

func (r *errAfterNReadCloser) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, r.err
	}

	remaining := len(r.data) - r.pos
	if remaining > r.n {
		remaining = r.n
	}
	if remaining > len(p) {
		remaining = len(p)
	}

	copy(p, r.data[r.pos:r.pos+remaining])
	r.pos += remaining
	return remaining, nil
}

func (r *errAfterNReadCloser) Close() error {
	return nil
}

func TestDecryptStream(t *testing.T) {
	encrypt := func(t *testing.T, params *Params_t, data string) []byte {
		t.Helper()

		rd, err := enc.EncryptStream(params.config, params.key, strings.NewReader(data))
		require.NoError(t, err)

		encrypted, err := io.ReadAll(rd)
		require.NoError(t, err)

		return encrypted
	}

	t.Run("ValidStream", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(bytes.NewReader(encrypted)))
		require.NoError(t, err)
		require.NotNil(t, rd)

		data, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Equal(t, "hello world", string(data))
	})

	t.Run("EmptyPayload", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "")

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(bytes.NewReader(encrypted)))
		require.NoError(t, err)
		require.NotNil(t, rd)

		data, err := io.ReadAll(rd)
		require.NoError(t, err)
		require.Empty(t, data)
	})

	t.Run("ConfigNil", func(t *testing.T) {
		require.Panics(t, func() {
			enc.DecryptStream(nil, []byte("0123456789abcdef0123456789abcdef"), io.NopCloser(bytes.NewReader([]byte("data"))))
		})
	})

	t.Run("UnsupportedDataAlgorithm", func(t *testing.T) {
		params := setup(t)
		params.config.DataAlgorithm = "NOPE"

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(bytes.NewReader([]byte("data"))))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("UnsupportedSubKeyAlgorithm", func(t *testing.T) {
		params := setup(t)
		params.config.SubKeyAlgorithm = "NOPE"

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(bytes.NewReader([]byte("data"))))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyNilIsInvalid", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")

		rd, err := enc.DecryptStream(params.config, nil, io.NopCloser(bytes.NewReader(encrypted)))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyEmptyIsInvalid", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")

		rd, err := enc.DecryptStream(params.config, []byte{}, io.NopCloser(bytes.NewReader(encrypted)))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyTooShortIsInvalid", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")

		rd, err := enc.DecryptStream(params.config, []byte("short"), io.NopCloser(bytes.NewReader(encrypted)))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("KeyTooLongIsInvalid", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")

		rd, err := enc.DecryptStream(params.config, []byte("0123456789abcdef0123456789abcdefx"), io.NopCloser(bytes.NewReader(encrypted)))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("IncorrectKeyIsInvalid", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")

		rd, err := enc.DecryptStream(params.config, []byte("fedcba9876543210fedcba9876543210"), io.NopCloser(bytes.NewReader(encrypted)))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("InvalidEncryptedStreamIsInvalid", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(bytes.NewReader(nil)))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("TruncatedEncryptedStreamIsIValid", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, "hello world")
		require.Greater(t, len(encrypted), 1)

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(bytes.NewReader(encrypted[:len(encrypted)-4])))
		require.NoError(t, err)
		require.NotNil(t, rd)

		// The error is raised when reading
		data, err := io.ReadAll(rd)
		require.Error(t, err)
		require.Empty(t, data)
	})

	t.Run("ReaderFails_AfterFirstEncryptedChunk", func(t *testing.T) {
		params := setup(t)

		rd, err := enc.DecryptStream(params.config, params.key, io.NopCloser(iotest.ErrReader(errors.New("reader failed"))))
		require.Error(t, err)
		require.Nil(t, rd)
	})

	t.Run("ReaderFailsWhileReadingEncryptedPayload", func(t *testing.T) {
		params := setup(t)
		encrypted := encrypt(t, params, strings.Repeat("a", params.config.ChunkSize+1))

		const wrappedSubkeySize = 40
		const aesGCMSIVOverhead = 28
		firstEncryptedChunkSize := params.config.ChunkSize + aesGCMSIVOverhead

		require.Greater(t, len(encrypted), wrappedSubkeySize+firstEncryptedChunkSize)

		readErr := errors.New("reader failed")
		r := &errAfterNReadCloser{
			data: encrypted[:wrappedSubkeySize+firstEncryptedChunkSize],
			n:    wrappedSubkeySize + firstEncryptedChunkSize,
			err:  readErr,
		}

		rd, err := enc.DecryptStream(params.config, params.key, r)
		require.NoError(t, err)
		require.NotNil(t, rd)

		data, err := io.ReadAll(rd)
		require.ErrorIs(t, err, readErr)
		require.NotEmpty(t, data)
	})
}

func TestCompressEncryptThenDecryptDecompressStream(t *testing.T) {
	params := setup(t)

	// Original data to compress, encrypt, decrypt, and decompress
	originalData := "This is a test string for compression and encryption. It should work!"
	r := strings.NewReader(originalData)

	// Step 1: Compress the data
	compressedReader, err := compression.DeflateStream("GZIP", r)
	require.NoError(t, err)
	require.NotEmpty(t, compressedReader)

	// Step 2: Encrypt the compressed data
	encryptedReader, err := enc.EncryptStream(params.config, params.key, compressedReader)
	require.NoError(t, err)
	require.NotEmpty(t, encryptedReader)

	// Step 3: Decrypt the data
	decryptedReader, err := enc.DecryptStream(params.config, params.key, io.NopCloser(encryptedReader))
	require.NoError(t, err)
	require.NotEmpty(t, decryptedReader)

	// Step 4: Decompress the decrypted data
	decompressedReader, err := compression.InflateStream("GZIP", decryptedReader)
	require.NoError(t, err)
	require.NotEmpty(t, decompressedReader)

	// Read the final output
	finalData, err := io.ReadAll(decompressedReader)
	require.NoError(t, err)
	require.NotEmpty(t, finalData)

	require.Equal(t, string(finalData), originalData)
}

func TestDeriveCanary(t *testing.T) {
	t.Run("ConfigNil", func(t *testing.T) {
		require.Panics(t, func() {
			enc.DeriveCanary(nil, []byte("0123456789abcdef0123456789abcdef"))
		})
	})

	t.Run("ValidConfiguration", func(t *testing.T) {
		params := setup(t)

		canary, err := enc.DeriveCanary(params.config, params.key)
		require.NoError(t, err)
		require.NotNil(t, canary)
		require.NotEmpty(t, canary)
	})

	t.Run("SameConfigurations_DifferentCanaries", func(t *testing.T) {
		params := setup(t)

		first, err := enc.DeriveCanary(params.config, params.key)
		require.NoError(t, err)
		require.NotNil(t, first)
		require.NotEmpty(t, first)

		second, err := enc.DeriveCanary(params.config, params.key)
		require.NoError(t, err)
		require.NotNil(t, second)
		require.NotEmpty(t, second)

		require.NotEqual(t, first, second)
	})

	t.Run("UnsupportedDataAlgorithm", func(t *testing.T) {
		params := setup(t)
		params.config.DataAlgorithm = "NOPE"

		canary, err := enc.DeriveCanary(params.config, params.key)
		require.Error(t, err)
		require.Nil(t, canary)
	})

	t.Run("UnsupportedSubKeyAlgorithm", func(t *testing.T) {
		params := setup(t)
		params.config.SubKeyAlgorithm = "NOPE"

		canary, err := enc.DeriveCanary(params.config, params.key)
		require.Error(t, err)
		require.Nil(t, canary)
	})

	t.Run("KeyNilIsInvalid", func(t *testing.T) {
		params := setup(t)

		canary, err := enc.DeriveCanary(params.config, nil)
		require.Error(t, err)
		require.Nil(t, canary)
	})

	t.Run("KeyEmptyIsInvalid", func(t *testing.T) {
		params := setup(t)

		canary, err := enc.DeriveCanary(params.config, []byte{})
		require.Error(t, err)
		require.Nil(t, canary)
	})

	t.Run("KeyTooShortIsInvalid", func(t *testing.T) {
		params := setup(t)

		canary, err := enc.DeriveCanary(params.config, []byte("short"))
		require.Error(t, err)
		require.Nil(t, canary)
	})

	t.Run("KeyTooLongIsInvalid", func(t *testing.T) {
		params := setup(t)

		canary, err := enc.DeriveCanary(params.config, []byte("0123456789abcdef0123456789abcdefx"))
		require.Error(t, err)
		require.Nil(t, canary)
	})
}
