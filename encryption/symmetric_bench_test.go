package encryption_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	enc "github.com/PlakarKorp/kloset/encryption"
	"github.com/stretchr/testify/require"
)

// benchPayload is incompressible on purpose: the streams here are the ones a
// backup writes, and by the time a chunk reaches them it has already been
// through the compressor.
func benchPayload(b *testing.B, n int) []byte {
	b.Helper()
	data := make([]byte, n)
	_, err := rand.Read(data)
	require.NoError(b, err)
	return data
}

func benchKey(b *testing.B) []byte {
	b.Helper()
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(b, err)
	return key
}

// BenchmarkEncryptStream and BenchmarkDecryptStream measure the two halves of
// the path every byte of a repository goes through, at sizes that span several
// of the ChunkSize frames the streams are cut into.
func BenchmarkEncryptStream(b *testing.B) {
	config := enc.NewDefaultConfiguration()
	key := benchKey(b)

	for _, size := range []int{1 << 20, 16 << 20} {
		payload := benchPayload(b, size)
		b.Run(humanSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rd, err := enc.EncryptStream(config, key, bytes.NewReader(payload))
				require.NoError(b, err)
				_, err = io.Copy(io.Discard, rd)
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkDecryptStream(b *testing.B) {
	config := enc.NewDefaultConfiguration()
	key := benchKey(b)

	for _, size := range []int{1 << 20, 16 << 20} {
		payload := benchPayload(b, size)

		rd, err := enc.EncryptStream(config, key, bytes.NewReader(payload))
		require.NoError(b, err)
		sealed, err := io.ReadAll(rd)
		require.NoError(b, err)

		b.Run(humanSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				rd, err := enc.DecryptStream(config, key, io.NopCloser(bytes.NewReader(sealed)))
				require.NoError(b, err)
				_, err = io.Copy(io.Discard, rd)
				require.NoError(b, err)
				require.NoError(b, rd.Close())
			}
		})
	}
}

func humanSize(n int) string {
	return fmt.Sprintf("%dMiB", n>>20)
}
