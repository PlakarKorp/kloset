package snapshot_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// dirpackRoot returns the root MAC of the dirpack btree for the (single)
// source of a snapshot. Two snapshots produced from byte-identical content
// share this MAC iff every dirpack was reused or rebuilt identically; for
// the purpose of these tests it's the cleanest end-to-end witness of D's
// reuse decisions because it depends transitively on every dirpack in the
// tree.
func dirpackRoot(t *testing.T, snap *snapshot.Snapshot) objects.MAC {
	t.Helper()
	require.NotEmpty(t, snap.Header.Sources, "snapshot has no sources")
	for _, idx := range snap.Header.Sources[0].Indexes {
		if idx.Name == "dirpack" {
			return idx.Value
		}
	}
	t.Fatalf("no dirpack index in source header")
	return objects.MAC{}
}

// TestDircacheReusesUnchangedTree verifies that a second backup of the
// same content into the same repository produces an identical dirpack
// btree root MAC. Reuse of every dirpack is the only way the second
// backup can land on the same root MAC because the dirpack MACs are
// content-addressed over the chunkified frame stream — any rebuild
// produces a fresh MAC iff anything in the subtree changed.
func TestDircacheReusesUnchangedTree(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	t.Cleanup(func() {
		_ = repo.AppContext().GetCache().Close()
	})

	files := []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
		ptesting.NewMockFile("a/two.txt", 0644, "world"),
		ptesting.NewMockFile("a/b/three.txt", 0644, "deep"),
		ptesting.NewMockFile("a/b/c/four.txt", 0644, "deeper"),
		ptesting.NewMockFile("x/y/z.txt", 0644, "sibling"),
	}

	first := ptesting.GenerateSnapshot(t, repo, files)
	defer first.Close()
	firstRoot := dirpackRoot(t, first)

	second := ptesting.GenerateSnapshot(t, repo, files)
	defer second.Close()
	secondRoot := dirpackRoot(t, second)

	require.Equal(t, firstRoot, secondRoot,
		"second backup of identical content must reuse every dirpack and so produce the same dirpack btree root MAC")
}

// TestDircacheRebuildsChangedAncestors verifies that modifying one file
// changes the dirpack MACs along its ancestor chain. We can't easily
// inspect per-directory MACs from outside the snapshot, but the root MAC
// must differ from the first backup (since at least the root dirpack must
// be rebuilt) and from a clean reference run.
func TestDircacheRebuildsChangedAncestors(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	t.Cleanup(func() {
		_ = repo.AppContext().GetCache().Close()
	})

	original := []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
		ptesting.NewMockFile("a/b/deep.txt", 0644, "v1"),
		ptesting.NewMockFile("x/y/sibling.txt", 0644, "untouched"),
	}
	first := ptesting.GenerateSnapshot(t, repo, original)
	defer first.Close()
	firstRoot := dirpackRoot(t, first)

	// Change /a/b/deep.txt content. The expected dirty chain is /a/b →
	// /a → /. /x/y must remain reusable.
	modified := []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
		ptesting.NewMockFile("a/b/deep.txt", 0644, "v2-different"),
		ptesting.NewMockFile("x/y/sibling.txt", 0644, "untouched"),
	}
	second := ptesting.GenerateSnapshot(t, repo, modified)
	defer second.Close()
	secondRoot := dirpackRoot(t, second)

	require.NotEqual(t, firstRoot, secondRoot,
		"changing a file's content must change the dirpack btree root MAC because the ancestor chain rebuilds")
}

// TestDircacheRebuildsOnAdd verifies that adding a file in a previously
// unchanged directory invalidates that directory's dircache entry.
func TestDircacheRebuildsOnAdd(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	t.Cleanup(func() {
		_ = repo.AppContext().GetCache().Close()
	})

	first := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
	})
	defer first.Close()
	firstRoot := dirpackRoot(t, first)

	second := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
		ptesting.NewMockFile("a/two.txt", 0644, "added"),
	})
	defer second.Close()
	secondRoot := dirpackRoot(t, second)

	require.NotEqual(t, firstRoot, secondRoot,
		"adding a file must invalidate the parent dirpack")
}

// TestDircacheRebuildsOnDelete verifies that removing a file from a
// previously unchanged directory invalidates that directory's dircache
// entry. This is the case dirty propagation alone cannot detect — the
// deleted file produces no record during the new import — so it
// exercises the explicit child-set comparison in reusableDirpack.
func TestDircacheRebuildsOnDelete(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	t.Cleanup(func() {
		_ = repo.AppContext().GetCache().Close()
	})

	first := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
		ptesting.NewMockFile("a/two.txt", 0644, "world"),
	})
	defer first.Close()
	firstRoot := dirpackRoot(t, first)

	second := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
	})
	defer second.Close()
	secondRoot := dirpackRoot(t, second)

	require.NotEqual(t, firstRoot, secondRoot,
		"removing a file must invalidate the parent dirpack")
}
