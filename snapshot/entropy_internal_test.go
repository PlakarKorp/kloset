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

// Below the sampling threshold every byte is counted, so the result must
// match the reference exactly (up to float rounding).
func TestEntropyExactBelowThreshold(t *testing.T) {
	random := randomBytes(entropySampleThreshold - 1)

	cases := map[string][]byte{
		"empty":     nil,
		"one byte":  {42},
		"odd tail":  random[:len(random)-2],
		"random":    random,
		"zeros":     make([]byte, entropySampleThreshold-1),
		"text":      bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 4096),
		"two bytes": bytes.Repeat([]byte{0, 255}, 1<<15),
	}
	for name, data := range cases {
		require.InDelta(t, referenceEntropy(data), entropy(data), 1e-12, name)
	}
}

// At or above the threshold the value is estimated from samples: exact for
// data whose byte distribution is the same in every block, close otherwise.
func TestEntropySampledAboveThreshold(t *testing.T) {
	cases := map[string]struct {
		data  []byte
		delta float64
	}{
		"zeros":             {make([]byte, 1<<20), 1e-12},
		"two bytes":         {bytes.Repeat([]byte{0, 255}, 1<<19), 1e-12},
		"random":            {randomBytes(1 << 20), 0.01},
		"random odd size":   {randomBytes(entropySampleThreshold + 12345), 0.01},
		"text":              {bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 1<<15), 0.01},
		"zeros then random": {append(make([]byte, 1<<19), randomBytes(1<<19)...), 0.05},
	}
	for name, c := range cases {
		require.InDelta(t, referenceEntropy(c.data), entropy(c.data), c.delta, name)
	}
}

// pages returns size bytes of page-structured data starting at phase within
// a page: each page begins with hdr zero bytes followed by random payload.
func pages(size, page, hdr, phase int) []byte {
	payload := randomBytes(size + page)
	for i := range payload {
		if i%page < hdr {
			payload[i] = 0
		}
	}
	return payload[phase : phase+size]
}

// Pages whose size divides the sampling stride must not bias the estimate,
// whatever the phase the chunk starts at within a page.
func TestEntropySampledPeriodicData(t *testing.T) {
	for _, c := range []struct{ page, hdr int }{
		{8 << 10, 4 << 10},
		{8 << 10, 1 << 10},
		{16 << 10, 4 << 10},
		{32 << 10, 16 << 10},
	} {
		for phase := 0; phase < c.page; phase += 1 << 10 {
			data := pages(1<<20, c.page, c.hdr, phase)
			require.InDelta(t, referenceEntropy(data), entropy(data), 0.3,
				"page %d, header %d, phase %d", c.page, c.hdr, phase)
		}
	}
}
