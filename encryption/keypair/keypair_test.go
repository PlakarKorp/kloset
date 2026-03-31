package keypair_test

import (
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

func TestFromPrivateKey(t *testing.T) {
	t.Run("NilPrivateKey", func(t *testing.T) {
		require.Panics(t, func() { keypair.FromPrivateKey(nil) })
	})

	t.Run("ValidPrivateKey", func(t *testing.T) {
		original, err := keypair.Generate()
		require.NoError(t, err)

		kp := keypair.FromPrivateKey(original.PrivateKey)
		require.NotNil(t, kp)

		require.Equal(t, kp.PrivateKey, original.PrivateKey)
		require.Equal(t, kp.PublicKey, original.PublicKey)
	})
}

func TestFromPublicKey(t *testing.T) {
	t.Run("PublicKey Nil", func(t *testing.T) {
		kp := keypair.FromPublicKey(nil)
		require.NotNil(t, kp)
		require.Nil(t, kp.PrivateKey)
		require.Nil(t, kp.PublicKey)
	})

	t.Run("ValidPublicKey", func(t *testing.T) {
		original, err := keypair.Generate()
		require.NoError(t, err)

		kp := keypair.FromPublicKey(original.PublicKey)
		require.NotNil(t, kp)
		require.Nil(t, kp.PrivateKey)
		require.Equal(t, kp.PublicKey, original.PublicKey)
	})

	t.Run("SameSourceKeyPair", func(t *testing.T) {
		original, err := keypair.Generate()
		require.NoError(t, err)

		fromPrivate := keypair.FromPrivateKey(original.PrivateKey)
		require.NotNil(t, fromPrivate)

		publicFromPrivate := keypair.FromPublicKey(fromPrivate.PublicKey)
		require.NotNil(t, publicFromPrivate)

		require.Equal(t, fromPrivate.PrivateKey, original.PrivateKey)
		require.Equal(t, fromPrivate.PublicKey, original.PublicKey)
		require.Nil(t, publicFromPrivate.PrivateKey)
		require.Equal(t, publicFromPrivate.PublicKey, original.PublicKey)
	})
}

func TestSign(t *testing.T) {
	t.Run("ValidData", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		signature := kp.Sign([]byte("hello"))
		require.NotNil(t, signature)
		require.Len(t, signature, ed25519.SignatureSize)
	})

	t.Run("NilPrivateKey", func(t *testing.T) {
		kp := keypair.FromPublicKey(ed25519.PublicKey("public"))
		require.Panics(t, func() { kp.Sign([]byte("hello")) })
	})

	t.Run("NilData", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		signature := kp.Sign(nil)
		require.NotNil(t, signature)
		require.Len(t, signature, ed25519.SignatureSize)
	})

	t.Run("SameKeyPairSameData_SameSignature", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		data := []byte("hello")

		first := kp.Sign(data)
		require.Len(t, first, ed25519.SignatureSize)

		second := kp.Sign(data)
		require.Len(t, second, ed25519.SignatureSize)

		require.Equal(t, first, second)
	})

	t.Run("SameKeyPairDifferentData_DifferentSignatures", func(t *testing.T) {
		kp, err := keypair.Generate()
		require.NoError(t, err)

		first := kp.Sign([]byte("hello"))
		require.Len(t, first, ed25519.SignatureSize)

		second := kp.Sign([]byte("world"))
		require.Len(t, second, ed25519.SignatureSize)

		require.NotEqual(t, first, second)
	})
}
