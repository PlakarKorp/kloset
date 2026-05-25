package packer

import (
	"crypto/sha256"
	"hash"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/stretchr/testify/require"
)

// newTestManager returns a seqPackerManager that has all of its dependencies
// configured for in-memory packfiles with an identity encoder and a no-op
// flusher. Useful for exercising AddPadding directly.
func newTestManager(t *testing.T) *seqPackerManager {
	t.Helper()
	ctx := kcontext.NewKContext()
	t.Cleanup(func() { ctx.Close() })

	storageConf := storage.NewConfiguration()
	hashFactory := func() hash.Hash { return sha256.New() }
	encoder := func(r io.Reader) (io.Reader, error) { return r, nil }
	packfileFactory := func(hf packfile.HashFactory) (packfile.Packfile, error) {
		return packfile.NewPackfileInMemory(hf)
	}
	flusher := func(pf packfile.Packfile) error { return nil }

	mgr := NewSeqPackerManager(ctx, storageConf, encoder, packfileFactory, hashFactory, flusher).(*seqPackerManager)
	return mgr
}

// TestAddPaddingNegative exercises the maxSize < 0 branch.
func TestAddPaddingNegative(t *testing.T) {
	mgr := newTestManager(t)
	pf, err := packfile.NewPackfileInMemory(sha256.New)
	require.NoError(t, err)
	require.Error(t, mgr.AddPadding(pf, -1))
}

// TestAddPaddingZero exercises the maxSize == 0 early return.
func TestAddPaddingZero(t *testing.T) {
	mgr := newTestManager(t)
	pf, err := packfile.NewPackfileInMemory(sha256.New)
	require.NoError(t, err)
	require.NoError(t, mgr.AddPadding(pf, 0))
	require.Equal(t, uint64(0), pf.Size())
}

// TestAddPaddingPositive ensures a positive maxSize actually appends padding.
func TestAddPaddingPositive(t *testing.T) {
	mgr := newTestManager(t)
	pf, err := packfile.NewPackfileInMemory(sha256.New)
	require.NoError(t, err)
	require.NoError(t, mgr.AddPadding(pf, 64))
	require.Greater(t, pf.Size(), uint64(0))
}
