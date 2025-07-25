package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"hash"
	"io"

	"github.com/PlakarKorp/kloset/hashing"
	aeskw "github.com/nickball/go-aes-key-wrap"
	"github.com/tink-crypto/tink-go/v2/aead/subtle"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/pbkdf2"
	"golang.org/x/crypto/scrypt"
)

const (
	chunkSize         = 64 * 1024 // Size of each chunk for encryption/decryption
	DEFAULT_KDF       = "ARGON2ID"
	AESGMSIV_OVERHEAD = subtle.AESGCMSIVNonceSize + aes.BlockSize
)

type Configuration struct {
	SubKeyAlgorithm string    `json:"subkey_algorithm"`
	DataAlgorithm   string    `json:"data_algorithm"`
	ChunkSize       int       `json:"chunk_size"`
	KDFParams       KDFParams `json:"KDF_params"`
	Canary          []byte    `json:"canary"`
}

type KDFParams struct {
	KDF            string          `json:"KDF"`
	Salt           []byte          `json:"salt"`
	Argon2idParams *Argon2idParams `msgpack:"argon2id,omitempty" json:"argon2id,omitempty"`
	ScryptParams   *ScryptParams   `msgpack:"scrypt,omitempty" json:"scrypt,omitempty"`
	Pbkdf2Params   *PBKDF2Params   `msgpack:"pbkdf2,omitempty" json:"pbkdf2,omitempty"`
}

func NewDefaultKDFParams(KDF string) (*KDFParams, error) {
	saltSize := uint32(16)
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}

	switch KDF {
	case "ARGON2ID":
		return &KDFParams{
			KDF:  "ARGON2ID",
			Salt: salt,
			Argon2idParams: &Argon2idParams{
				SaltSize: saltSize,
				Time:     4,
				// from: https://pkg.go.dev/golang.org/x/crypto/argon2
				// [...] the memory parameter specifies the size of the memory in KiB.
				// For example memory=64*1024 sets the memory cost to ~64 MB
				Memory:  256 * 1024,
				Threads: 1,
				KeyLen:  32,
			},
		}, nil
	case "SCRYPT":
		return &KDFParams{
			KDF:  "SCRYPT",
			Salt: salt,
			ScryptParams: &ScryptParams{
				SaltSize: saltSize,
				N:        1 << 15,
				R:        8,
				P:        1,
				KeyLen:   32,
			},
		}, nil
	case "PBKDF2":
		return &KDFParams{
			KDF:  "PBKDF2",
			Salt: salt,
			Pbkdf2Params: &PBKDF2Params{
				SaltSize:   saltSize,
				Iterations: 100000,
				KeyLen:     32,
				Hashing:    "SHA256",
			},
		}, nil
	}
	return nil, fmt.Errorf("unsupported KDF: %s", KDF)
}

type Argon2idParams struct {
	SaltSize uint32
	Time     uint32
	Memory   uint32
	Threads  uint8
	KeyLen   uint32
}

type ScryptParams struct {
	SaltSize uint32
	N        int
	R        int
	P        int
	KeyLen   int
}

type PBKDF2Params struct {
	SaltSize   uint32
	Iterations int
	KeyLen     int
	Hashing    string
}

func NewDefaultConfiguration() *Configuration {
	kdfParams, err := NewDefaultKDFParams(DEFAULT_KDF)
	if err != nil {
		panic(err)
	}

	return &Configuration{
		SubKeyAlgorithm: "AES256-KW",
		DataAlgorithm:   "AES256-GCM-SIV",
		ChunkSize:       chunkSize,
		KDFParams:       *kdfParams,
	}
}

func Salt() (salt []byte, err error) {
	_, err = rand.Read(salt[:])
	return
}

// DeriveKey generates a secret from a passphrase using configured KDF parameters
func DeriveKey(params KDFParams, passphrase []byte) ([]byte, error) {
	switch params.KDF {
	case "ARGON2ID":
		return argon2.IDKey(passphrase, params.Salt[:], params.Argon2idParams.Time, params.Argon2idParams.Memory, params.Argon2idParams.Threads, params.Argon2idParams.KeyLen), nil
	case "SCRYPT":
		return scrypt.Key(passphrase, params.Salt[:], params.ScryptParams.N, params.ScryptParams.R, params.ScryptParams.P, params.ScryptParams.KeyLen)
	case "PBKDF2":
		return pbkdf2.Key(passphrase, params.Salt[:], params.Pbkdf2Params.Iterations, params.Pbkdf2Params.KeyLen, func() hash.Hash { return hashing.GetHasher(params.Pbkdf2Params.Hashing) }), nil
	}
	return nil, fmt.Errorf("unsupported KDF: %s", params.KDF)
}

func DeriveCanary(config *Configuration, key []byte) ([]byte, error) {
	canary := make([]byte, 32)
	if _, err := rand.Read(canary); err != nil {
		return nil, err
	}

	rd, err := EncryptStream(config, key, bytes.NewReader(canary))
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rd)
}

func VerifyCanary(config *Configuration, key []byte) bool {
	rd, err := DecryptStream(config, key, io.NopCloser(bytes.NewReader(config.Canary)))
	if err != nil {
		return false
	}
	_, err = io.ReadAll(rd)
	return err == nil
}

func encryptSubkey_AES256_GCM(key []byte, subkey []byte) ([]byte, error) {
	// Encrypt the subkey with the main key using AES-GCM
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Generate a nonce for subkey encryption
	subkeyNonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(subkeyNonce); err != nil {
		return nil, err
	}

	// return block with nonce and encrypted subkey
	return append(subkeyNonce, gcm.Seal(nil, subkeyNonce, subkey, nil)...), nil
}

func encryptSubkey_AES256_KW(key []byte, subkey []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return aeskw.Wrap(block, subkey)
}

func EncryptSubkey(algorithm string, key []byte, subkey []byte) ([]byte, error) {
	switch algorithm {
	case "AES256-GCM":
		return encryptSubkey_AES256_GCM(key, subkey)
	case "AES256-KW":
		return encryptSubkey_AES256_KW(key, subkey)
	}
	return nil, fmt.Errorf("not implemented")
}

func decryptSubkey_AES256_GCM(key []byte, r io.Reader) ([]byte, error) {
	// Set up to decrypt the subkey from the input
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Read and decrypt the subkey
	subkeyNonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(r, subkeyNonce); err != nil {
		return nil, err
	}

	encSubkey := make([]byte, gcm.Overhead()+32) // GCM overhead for the 32-byte subkey
	if _, err := io.ReadFull(r, encSubkey); err != nil {
		return nil, err
	}

	subkey, err := gcm.Open(nil, subkeyNonce, encSubkey, nil)
	if err != nil {
		return nil, err
	}

	return subkey, nil
}

func decryptSubkey_AES256_KW(key []byte, r io.Reader) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// 40 is the size of the wrapped key
	subkeyBlock := make([]byte, 40)
	if _, err := io.ReadFull(r, subkeyBlock); err != nil {
		return nil, err
	}

	return aeskw.Unwrap(block, subkeyBlock)
}

func DecryptSubkey(algorithm string, key []byte, r io.Reader) ([]byte, error) {
	switch algorithm {
	case "AES256-GCM":
		return decryptSubkey_AES256_GCM(key, r)
	case "AES256-KW":
		return decryptSubkey_AES256_KW(key, r)
	}
	return nil, fmt.Errorf("not implemented")
}

// EncryptStream encrypts a stream using AES-GCM with a random session-specific subkey
func EncryptStream(config *Configuration, key []byte, r io.Reader) (io.Reader, error) {
	if config.DataAlgorithm != "AES256-GCM-SIV" {
		return nil, fmt.Errorf("unsupported data encryption algorithm: %s", config.DataAlgorithm)
	}

	// Generate a random subkey for data encryption
	subkey := make([]byte, 32)
	if _, err := rand.Read(subkey); err != nil {
		return nil, err
	}

	subkeyBlock, err := EncryptSubkey(config.SubKeyAlgorithm, key, subkey)
	if err != nil {
		return nil, err
	}

	dataGCM, err := subtle.NewAESGCMSIV(subkey)
	if err != nil {
		return nil, err
	}

	// Set up the pipe for streaming encryption
	pr, pw := io.Pipe()

	// Start encryption in a goroutine
	go func() {
		defer pw.Close()

		// Write the encrypted subkey and both nonces to the output stream
		if _, err := pw.Write(subkeyBlock); err != nil {
			pw.CloseWithError(err)
			return
		}

		// Encrypt and write data chunks
		chunk := make([]byte, config.ChunkSize)
		for {
			// Use ReadFull to read exactly chunkSize or less at EOF
			n, err := io.ReadFull(r, chunk)
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				pw.CloseWithError(err)
				return
			}

			if n > 0 {
				encryptedChunk, err := dataGCM.Encrypt(chunk[:n], nil)
				if err != nil {
					pw.CloseWithError(err)
					return
				}
				if _, err := pw.Write(encryptedChunk); err != nil {
					pw.CloseWithError(err)
					return
				}
			}

			// Stop when EOF is reached
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
		}
	}()

	return pr, nil
}

// DecryptStream decrypts a stream using AES-GCM with a random session-specific subkey
func DecryptStream(config *Configuration, key []byte, r io.ReadCloser) (io.ReadCloser, error) {
	if config.DataAlgorithm != "AES256-GCM-SIV" {
		return nil, fmt.Errorf("unsupported data encryption algorithm: %s", config.DataAlgorithm)
	}

	subkey, err := DecryptSubkey(config.SubKeyAlgorithm, key, r)
	if err != nil {
		return nil, err
	}

	// Set up AES-GCM for actual data decryption using the subkey
	dataGCM, err := subtle.NewAESGCMSIV(subkey)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()

	// Start decryption in a goroutine
	go func() {
		defer pw.Close()

		buffer := make([]byte, config.ChunkSize+AESGMSIV_OVERHEAD)
		for {
			n, err := r.Read(buffer)
			if err != nil {
				if err != io.EOF {
					pw.CloseWithError(err)
					return
				}
			}

			if n == 0 {
				return
			}

			// Decrypt each chunk and write it to the pipe
			decryptedChunk, err := dataGCM.Decrypt(buffer[:n], nil)
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(decryptedChunk); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return pr, nil
}
