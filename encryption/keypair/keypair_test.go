package keypair_test

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/PlakarKorp/kloset/encryption/keypair"
	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	t.Run("ValidKeyPair", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)
		require.NotNil(t, kp)

		require.NotNil(t, kp.PrivateKey)
		require.NotNil(t, kp.PublicKey)

		require.Len(t, kp.PrivateKey, ed25519.PrivateKeySize)
		require.Len(t, kp.PublicKey, ed25519.PublicKeySize)

		publicKey := kp.PrivateKey.Public().(ed25519.PublicKey)
		require.Equal(t, publicKey, kp.PublicKey)
	})

	t.Run("TwoCallsProduceDifferentKeyPairs", func(t *testing.T) {
		first, err := keypair.Generate()
		require.NoError(t, err)
		require.NotNil(t, first)

		second, err := keypair.Generate()
		require.NoError(t, err)
		require.NotNil(t, second)

		require.NotEqual(t, first.PrivateKey, second.PrivateKey)
		require.NotEqual(t, first.PublicKey, second.PublicKey)
	})
}

func TestToBytes(t *testing.T) {
	t.Run("ValidKeyPair", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		data, err := kp.ToBytes()
		require.NoError(t, err)
		require.NotNil(t, data)
		require.NotEmpty(t, data)
	})

	t.Run("SameKeyPair_ProducesSameBytes", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		first, err := kp.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, first)

		second, err := kp.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, second)

		require.Equal(t, first, second)
	})

	t.Run("DifferentKeyPairs_ProduceDifferentBytes", func(t *testing.T) {
		firstKP, err := keypair.Generate()
		require.NoError(t, err)

		secondKP, err := keypair.Generate()
		require.NoError(t, err)

		first, err := firstKP.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, first)

		second, err := secondKP.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, second)

		require.NotEqual(t, first, second)
	})
}

func TestFromBytes(t *testing.T) {
	t.Run("InvalidBytes", func(t *testing.T) {
		kp, err := keypair.FromBytes(nil)
		require.Error(t, err)
		require.Nil(t, kp)
	})

	t.Run("EmptyBytes", func(t *testing.T) {
		kp, err := keypair.FromBytes([]byte{})
		require.Error(t, err)
		require.Nil(t, kp)
	})

	t.Run("InvalidByteEncoding", func(t *testing.T) {
		kp, err := keypair.FromBytes([]byte{0x00, 0x01, 0x02})
		require.Error(t, err)
		require.Nil(t, kp)
	})

	t.Run("SameBytes_SameKeyPair", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		data, err := kp.ToBytes()
		require.NoError(t, err)

		first, err := keypair.FromBytes(data)
		require.NoError(t, err)
		require.NotNil(t, first)

		second, err := keypair.FromBytes(data)
		require.NoError(t, err)
		require.NotNil(t, second)

		require.Equal(t, first.PrivateKey, second.PrivateKey)
		require.Equal(t, first.PublicKey, second.PublicKey)
	})

	t.Run("ToBytes->FromBytes->ToBytes", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		data, err := kp.ToBytes()
		require.NoError(t, err)

		decoded, err := keypair.FromBytes(data)
		require.NoError(t, err)
		require.NotNil(t, decoded)

		require.Equal(t, decoded.PrivateKey, kp.PrivateKey)
		require.Equal(t, decoded.PublicKey, kp.PublicKey)

		sameData, err := decoded.ToBytes()
		require.NoError(t, err)
		require.Equal(t, data, sameData)
	})
}

// TestFromPrivateKey checks if a key pair can be created from an existing private key
func TestFromPrivateKey(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	kp := keypair.FromPrivateKey(privateKey)
	if kp.PrivateKey == nil || kp.PublicKey == nil {
		t.Fatal("Key pair from private key has nil keys")
	}

	// Check if the public key matches the private key's public key
	if !bytes.Equal(kp.PublicKey, privateKey.Public().(ed25519.PublicKey)) {
		t.Fatal("Public key does not match the private key's public key")
	}
}

// TestFromPublicKey checks if a key pair can be created from an existing public key
func TestFromPublicKey(t *testing.T) {
	publicKey, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate public key: %v", err)
	}

	kp := keypair.FromPublicKey(publicKey)
	if kp.PrivateKey != nil {
		t.Fatal("Key pair from public key should have nil private key")
	}

	if !bytes.Equal(kp.PublicKey, publicKey) {
		t.Fatal("Public key in key pair does not match the input public key")
	}
}

// TestSignAndVerify checks if signing and verification using a key pair works correctly
func TestSignAndVerify(t *testing.T) {
	// Generate a key pair for testing
	kp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}

	data := []byte("This is a test message.")
	signature := kp.Sign(data)

	// Verify the signature using the same key pair
	if !kp.Verify(data, signature) {
		t.Fatal("Failed to verify signature with the same key pair")
	}

	// Create a different key pair to test signature verification failure
	otherKp, err := keypair.Generate()
	if err != nil {
		t.Fatalf("Failed to generate a different key pair: %v", err)
	}

	if otherKp.Verify(data, signature) {
		t.Fatal("Signature verified with a different key pair")
	}
}

// TestSignWithNilPrivateKey checks that signing fails when private key is nil
func TestSignWithNilPrivateKey(t *testing.T) {
	kp := keypair.FromPublicKey(make(ed25519.PublicKey, ed25519.PublicKeySize))
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic when signing with nil private key")
		}
	}()
	_ = kp.Sign([]byte("Some data"))
}
