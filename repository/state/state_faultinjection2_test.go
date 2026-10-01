package state

import (
	"bytes"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// TestDeserializeFromStreamV100ReadErrorsAtEveryOffset drives the v1.0.0
// deserializer against a reader that faults at every byte offset of a valid
// v100 stream, covering its read-error branches.
func TestDeserializeFromStreamV100ReadErrorsAtEveryOffset(t *testing.T) {
	valid := buildV100Stream(t).Bytes()
	require.Greater(t, len(valid), 0)

	for cutoff := 0; cutoff < len(valid); cutoff++ {
		st, _ := newAggregate(t)

		r := &failAfterReader{data: valid, max: cutoff}
		err := st.deserializeFromStreamv100(r)
		require.Error(t, err, "cutoff=%d should fail to deserialize v100", cutoff)
	}

	// Sanity: full stream still deserializes.
	st, _ := newAggregate(t)
	require.NoError(t, st.deserializeFromStreamv100(bytes.NewReader(valid)))
}

// TestMergeFromCacheParseErrors exercises each "failed to deserialize ... entry"
// branch of mergeFromCache by storing a corrupt buffer (payloads are opaque
// bytes for the cache) in one entry category at a time.
func TestMergeFromCacheParseErrors(t *testing.T) {
	garbage := []byte{0xFF}

	cases := []struct {
		name string
		seed func(sc *caching.ScanCache) error
	}{
		{"delta", func(sc *caching.ScanCache) error {
			return sc.PutDelta(resources.RT_OBJECT, objects.MAC{0x01}, objects.MAC{0x10}, garbage)
		}},
		{"coloured", func(sc *caching.ScanCache) error {
			return sc.PutColoured(resources.RT_OBJECT, objects.MAC{0x02}, garbage)
		}},
		{"packfile", func(sc *caching.ScanCache) error {
			return sc.PutPackfile(objects.MAC{0x03}, garbage)
		}},
		{"configuration", func(sc *caching.ScanCache) error {
			return sc.PutConfiguration("k", garbage)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			from := newScanCache(t)
			require.NoError(t, tc.seed(from))

			dst, _ := newAggregate(t)

			err := dst.mergeFromCache(from)
			require.Error(t, err)
		})
	}
}
