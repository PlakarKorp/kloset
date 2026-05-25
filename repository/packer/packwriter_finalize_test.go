package packer

import (
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"sync"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func newPackingCache(t *testing.T) *caching.PackingCache {
	t.Helper()
	tmpDir := t.TempDir()
	cm := caching.NewManager(pebble.Constructor(tmpDir))
	t.Cleanup(func() { cm.Close() })
	pc, err := cm.Packing()
	require.NoError(t, err)
	t.Cleanup(func() { pc.Close() })
	return pc
}

// drainPutter is a putter that drains the PackWriter's Reader pipe — the
// canonical way PackWriter is consumed in production.
func drainPutter(p *PackWriter) error {
	_, err := io.Copy(io.Discard, p.Reader)
	return err
}

// TestPackWriterFinalizeFull drives the full WriteBlob → Finalize flow with a
// putter that drains the Reader. serializeIndex and serializeFooter both run.
func TestPackWriterFinalizeFull(t *testing.T) {
	pc := newPackingCache(t)

	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }

	pw := NewPackWriter(drainPutter, encoder, hasher, pc)

	for i := 0; i < 3; i++ {
		mac := objects.RandomMAC()
		require.NoError(t, pw.WriteBlob(resources.RT_CHUNK, versioning.FromString("1.0.0"), mac, []byte("payload"), 0))
	}

	require.NoError(t, pw.Finalize())
	require.Greater(t, pw.Footer.Count, uint32(0))
}

// TestPackWriterFinalizeEmpty calls Finalize without writing any blobs;
// serializeIndex's per-record loop is skipped and only the footer is written.
func TestPackWriterFinalizeEmpty(t *testing.T) {
	pc := newPackingCache(t)

	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	hasher := func() hash.Hash { return sha256.New() }

	pw := NewPackWriter(drainPutter, encoder, hasher, pc)
	require.NoError(t, pw.Finalize())
}

// failingEncoderOnNthCall lets us inject an encoder failure on a specific
// invocation. Used to hit the encoder-error branch of serializeIndex /
// serializeFooter.
type failingEncoderOnNthCall struct {
	failAt int
	called int
	mu     sync.Mutex
}

func (f *failingEncoderOnNthCall) encode(r io.Reader) (io.Reader, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called++
	if f.called == f.failAt {
		return nil, errors.New("forced encoder failure")
	}
	return r, nil
}

// TestPackWriterSerializeIndexEncoderError makes the encoder fail when
// serializeIndex calls it (WriteBlob's encoder = 1st, serializeIndex = 2nd).
func TestPackWriterSerializeIndexEncoderError(t *testing.T) {
	pc := newPackingCache(t)

	fe := &failingEncoderOnNthCall{failAt: 2}
	hasher := func() hash.Hash { return sha256.New() }

	pw := NewPackWriter(drainPutter, fe.encode, hasher, pc)

	mac := objects.RandomMAC()
	require.NoError(t, pw.WriteBlob(resources.RT_CHUNK, versioning.FromString("1.0.0"), mac, []byte("x"), 0))
	require.Error(t, pw.Finalize())
}

// TestPackWriterSerializeFooterEncoderError makes the encoder fail on the
// third call (WriteBlob = 1, serializeIndex = 2, serializeFooter = 3).
func TestPackWriterSerializeFooterEncoderError(t *testing.T) {
	pc := newPackingCache(t)

	fe := &failingEncoderOnNthCall{failAt: 3}
	hasher := func() hash.Hash { return sha256.New() }

	pw := NewPackWriter(drainPutter, fe.encode, hasher, pc)

	mac := objects.RandomMAC()
	require.NoError(t, pw.WriteBlob(resources.RT_CHUNK, versioning.FromString("1.0.0"), mac, []byte("x"), 0))
	require.Error(t, pw.Finalize())
}
