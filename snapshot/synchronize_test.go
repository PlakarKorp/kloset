package snapshot_test

import (
	"bytes"
	"io"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestSynchronize copies a snapshot from a source repository into a destination
// repository.  This exercises Synchronize and, transitively, every method on
// the unexported syncImporter type (Origin/Type/Root/Flags/Ping/Close/Import).
func TestSynchronize(t *testing.T) {
	srcRepo := ptesting.GenerateRepository(t, nil, nil, nil)
	dstRepo := ptesting.GenerateRepository(t, nil, nil, nil)

	srcSnap := ptesting.GenerateSnapshot(t, srcRepo, []ptesting.MockFile{
		ptesting.NewMockDir("docs"),
		ptesting.NewMockFile("docs/readme.txt", 0644, "sync me"),
		ptesting.NewMockFile("docs/notes.txt", 0644, "and me too"),
	})
	defer srcSnap.Close()

	dstBuilder, err := snapshot.Create(dstRepo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name: "sync-test",
	})
	require.NoError(t, err)
	require.NotNil(t, dstBuilder)

	require.NoError(t, srcSnap.Synchronize(dstBuilder))
	require.NoError(t, dstBuilder.Close())
	require.NoError(t, dstBuilder.Repository().RebuildState())

	syncedID := dstBuilder.Header.Identifier
	synced, err := snapshot.Load(dstRepo, syncedID)
	require.NoError(t, err)
	defer synced.Close()

	fs, err := synced.Filesystem()
	require.NoError(t, err)

	var hasReadme, hasNotes bool
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.HasSuffix(p, "readme.txt") {
			hasReadme = true
		}
		if strings.HasSuffix(p, "notes.txt") {
			hasNotes = true
		}
	}
	require.True(t, hasReadme, "readme.txt missing from synchronized snapshot")
	require.True(t, hasNotes, "notes.txt missing from synchronized snapshot")
}

// TestSynchronizeLargeFile copies a file spanning many read windows: its
// content must arrive intact in the destination repository.
func TestSynchronizeLargeFile(t *testing.T) {
	srcRepo := ptesting.GenerateRepository(t, nil, nil, nil)
	dstRepo := ptesting.GenerateRepository(t, nil, nil, nil)

	content := make([]byte, 40<<20)
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	for i := range content {
		content[i] = byte(rng.Uint32())
	}

	srcSnap := ptesting.GenerateSnapshot(t, srcRepo, []ptesting.MockFile{
		ptesting.NewMockFile("large.bin", 0644, string(content)),
	})
	defer srcSnap.Close()

	dstBuilder, err := snapshot.Create(dstRepo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name: "sync-large",
	})
	require.NoError(t, err)

	require.NoError(t, srcSnap.Synchronize(dstBuilder))
	require.NoError(t, dstBuilder.Close())
	require.NoError(t, dstBuilder.Repository().RebuildState())

	synced, err := snapshot.Load(dstRepo, dstBuilder.Header.Identifier)
	require.NoError(t, err)
	defer synced.Close()

	fs, err := synced.Filesystem()
	require.NoError(t, err)

	var pathname string
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.HasSuffix(p, "/large.bin") {
			pathname = p
		}
	}
	require.NotEmpty(t, pathname, "large.bin missing from synchronized snapshot")

	f, err := fs.Open(pathname)
	require.NoError(t, err)
	defer f.Close()
	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.True(t, bytes.Equal(content, got), "synchronized content differs")
}

// TestSynchronizeNoCommit covers the NoCommit branch of Synchronize, where the
// destination builder is told not to commit and Synchronize falls back to
// PutSnapshot.
func TestSynchronizeNoCommit(t *testing.T) {
	srcRepo := ptesting.GenerateRepository(t, nil, nil, nil)
	dstRepo := ptesting.GenerateRepository(t, nil, nil, nil)

	srcSnap := ptesting.GenerateSnapshot(t, srcRepo, []ptesting.MockFile{
		ptesting.NewMockFile("hello.txt", 0644, "world"),
	})
	defer srcSnap.Close()

	dstBuilder, err := snapshot.Create(dstRepo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:     "sync-nocommit",
		NoCommit: true,
	})
	require.NoError(t, err)
	require.NotNil(t, dstBuilder)
	defer dstBuilder.Close()

	require.NoError(t, srcSnap.Synchronize(dstBuilder))
}

// TestSynchronizeRich synchronizes a source snapshot that carries xattrs and a
// recorded error, exercising the xattr/error loops of syncImporter.Import and
// Filesystem.ResolveXattr.
func TestSynchronizeRich(t *testing.T) {
	srcSnap := generateRichBackup(t)
	defer srcSnap.Close()

	dstRepo := ptesting.GenerateRepository(t, nil, nil, nil)
	dstBuilder, err := snapshot.Create(dstRepo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name: "sync-rich",
	})
	require.NoError(t, err)
	require.NotNil(t, dstBuilder)

	require.NoError(t, srcSnap.Synchronize(dstBuilder))
	require.NoError(t, dstBuilder.Close())
	require.NoError(t, dstBuilder.Repository().RebuildState())

	synced, err := snapshot.Load(dstRepo, dstBuilder.Header.Identifier)
	require.NoError(t, err)
	defer synced.Close()

	fs, err := synced.Filesystem()
	require.NoError(t, err)

	// The xattr should survive into the synchronized snapshot.
	e, err := fs.GetEntry("/data/file.txt")
	require.NoError(t, err)
	rd, err := e.Xattr(fs, "user.note")
	require.NoError(t, err)
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, "note value", string(data))
}
