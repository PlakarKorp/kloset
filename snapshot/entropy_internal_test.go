package snapshot

import (
	"bytes"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// referenceEntropy is the straightforward float64 histogram over every byte.
func referenceEntropy(data []byte) float64 {
	if len(data) == 0 {
		return 0.0
	}
	var freq [256]float64
	for _, b := range data {
		freq[b]++
	}
	e := 0.0
	n := float64(len(data))
	for _, f := range freq {
		if f > 0 {
			p := f / n
			e -= p * math.Log2(p)
		}
	}
	return e
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.NewChaCha8([32]byte{}).Read(b)
	return b
}

// Every byte is counted, so the result must match the reference exactly (up
// to float rounding).
func TestEntropyMatchesReference(t *testing.T) {
	random := randomBytes(1 << 20)

	cases := map[string][]byte{
		"empty":     nil,
		"one byte":  {42},
		"odd tail":  random[:len(random)-3],
		"random":    random,
		"zeros":     make([]byte, 1<<20),
		"text":      bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 1<<14),
		"two bytes": bytes.Repeat([]byte{0, 255}, 1<<15),
	}
	for name, data := range cases {
		require.InDelta(t, referenceEntropy(data), entropy(data), 1e-12, name)
	}
}
