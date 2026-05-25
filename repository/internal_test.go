package repository

import (
	"bytes"
	"testing"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/stretchr/testify/require"
)

// TestEncodeBufferRoundtrip exercises the unexported encodeBuffer helper. We
// build a minimal Repository (no compression, no encryption) via the
// Inexistent constructor so the encode path becomes a pass-through and we
// don't need a real on-disk store.
func TestEncodeBufferRoundtrip(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.Client = "encodebuffer-test/1.0"
	ctx.SetLogger(logging.NewLogger(nil, nil))
	t.Cleanup(func() { ctx.Close() })

	repo, err := Inexistent(ctx, map[string]string{
		"type":     "mock",
		"location": "mock:///encodebuffer",
	})
	require.NoError(t, err)
	require.NotNil(t, repo)

	original := []byte("encode buffer round-trip test data")
	encoded, err := repo.encodeBuffer(original)
	require.NoError(t, err)

	decoded, err := repo.decodeBuffer(encoded)
	require.NoError(t, err)
	require.True(t, bytes.Equal(original, decoded))
}

// TestEncodeBufferEmpty exercises encodeBuffer on an empty input.
func TestEncodeBufferEmpty(t *testing.T) {
	ctx := kcontext.NewKContext()
	ctx.Client = "encodebuffer-empty/1.0"
	ctx.SetLogger(logging.NewLogger(nil, nil))
	t.Cleanup(func() { ctx.Close() })

	repo, err := Inexistent(ctx, map[string]string{
		"type":     "mock",
		"location": "mock:///encodebuffer-empty",
	})
	require.NoError(t, err)

	encoded, err := repo.encodeBuffer(nil)
	require.NoError(t, err)

	decoded, err := repo.decodeBuffer(encoded)
	require.NoError(t, err)
	require.Empty(t, decoded)
}
