package snapshot_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// generateListPackfilesSnapshot builds a tree with every blob in its own
// packfile, so that each packfile yielded by ListPackfiles stands for exactly
// one blob. It holds more than two batches of vfs entries and more than one
// batch of directories, each with a remainder, to go through every flush.
func generateListPackfilesSnapshot(t *testing.T) (*repository.Repository, *snapshot.Snapshot) {
	t.Helper()

	files := []ptesting.MockFile{ptesting.NewMockDir("/")}
	for d := range 1100 {
		dir := fmt.Sprintf("/dir%04d", d)
		// no blob is shared: contents are unique, and so are directory
		// summaries since every file has its own size.
		files = append(files,
			ptesting.NewMockDir(dir),
			ptesting.NewMockFile(dir+"/file", 0644, dir+strings.Repeat("x", d)))
	}

	repo := ptesting.GenerateRepositoryWithConfig(t, nil, nil, nil, func(c *storage.Configuration) {
		c.Packfile.MaxSize = 1
		// keeps the per packfile padding small
		c.Chunking.MinSize = 1024
	})
	return repo, ptesting.GenerateSnapshot(t, repo, files)
}

// listPackfiles walks the whole listing, carrying on after errors.
func listPackfiles(t *testing.T, snap *snapshot.Snapshot) (map[objects.MAC]int, []error) {
	t.Helper()

	it, err := snap.ListPackfiles()
	require.NoError(t, err)

	seen := make(map[objects.MAC]int)
	var errs []error
	for pf, err := range it {
		if err != nil {
			errs = append(errs, err)
			continue
		}
		seen[pf]++
	}
	return seen, errs
}

func allPackfiles(repo *repository.Repository) map[objects.MAC]struct{} {
	all := make(map[objects.MAC]struct{})
	for pf := range repo.ListPackfiles() {
		all[pf] = struct{}{}
	}
	return all
}

func packfileOf(t *testing.T, repo *repository.Repository, rtype resources.Type, mac objects.MAC) objects.MAC {
	t.Helper()

	pf, found, err := repo.GetPackfileForBlob(rtype, mac)
	require.NoError(t, err)
	require.True(t, found)
	return pf
}

// reload drops the caches of snap, so that nothing it already read can hide a
// packfile deleted afterwards.
func reload(t *testing.T, repo *repository.Repository, snap *snapshot.Snapshot) *snapshot.Snapshot {
	t.Helper()

	id := snap.Header.Identifier
	snap.Close()

	snap, err := snapshot.Load(repo, id)
	require.NoError(t, err)
	t.Cleanup(func() { snap.Close() })
	return snap
}

// A single snapshot reaches every packfile of its repository, each exactly
// once: one blob missed or fetched again would show.
func TestListPackfilesReachesEveryBlobOnce(t *testing.T) {
	repo, snap := generateListPackfilesSnapshot(t)
	defer snap.Close()

	seen, errs := listPackfiles(t, snap)
	require.Empty(t, errs)

	all := allPackfiles(repo)
	require.Len(t, seen, len(all))
	for pf := range all {
		require.Equal(t, 1, seen[pf], "packfile %x", pf)
	}
}

// An object that cannot be fetched is reported, and the listing carries on
// with everything else.
func TestListPackfilesMissingObject(t *testing.T) {
	repo, snap := generateListPackfilesSnapshot(t)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fs.GetEntry("/dir0500/file")
	require.NoError(t, err)
	require.NotNil(t, entry.ResolvedObject)

	// the object stays located through the state, only its chunks become
	// unreachable once its packfile is gone.
	unreachable := make(map[objects.MAC]struct{})
	for _, chunk := range entry.ResolvedObject.Chunks {
		unreachable[packfileOf(t, repo, resources.RT_CHUNK, chunk.ContentMAC)] = struct{}{}
	}
	require.NoError(t, repo.DeletePackfile(packfileOf(t, repo, resources.RT_OBJECT, entry.Object)))

	snap = reload(t, repo, snap)
	seen, errs := listPackfiles(t, snap)

	require.Len(t, errs, 1)
	require.ErrorContains(t, errs[0], fmt.Sprintf("%x", entry.Object))

	for pf := range allPackfiles(repo) {
		if _, ok := unreachable[pf]; ok {
			require.NotContains(t, seen, pf)
		} else {
			require.Contains(t, seen, pf)
		}
	}
}

// A btree node that cannot be fetched must be reported, not end the walk as if
// the tree was complete.
func TestListPackfilesMissingNode(t *testing.T) {
	tests := []struct {
		name  string
		rtype resources.Type
		nodes func(*testing.T, *snapshot.Snapshot) []objects.MAC
		err   string
	}{
		{
			name:  "vfs",
			rtype: resources.RT_VFS_NODE,
			nodes: func(t *testing.T, snap *snapshot.Snapshot) []objects.MAC {
				fs, err := snap.Filesystem()
				require.NoError(t, err)

				var leaves []objects.MAC
				it := fs.IterNodes()
				for it.Next() {
					mac, node := it.Current()
					if len(node.Values) > 0 {
						leaves = append(leaves, mac)
					}
				}
				require.NoError(t, it.Err())
				return leaves
			},
			err: "failed to walk the vfs",
		},
		{
			name:  "dirpack",
			rtype: resources.RT_BTREE_NODE,
			nodes: func(t *testing.T, snap *snapshot.Snapshot) []objects.MAC {
				dirpack, err := snap.DirPack()
				require.NoError(t, err)

				var leaves []objects.MAC
				it := dirpack.IterDFS()
				for it.Next() {
					mac, node := it.Current()
					if len(node.Values) > 0 {
						leaves = append(leaves, mac)
					}
				}
				require.NoError(t, it.Err())
				return leaves
			},
			err: "failed to walk the dirpack tree",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, snap := generateListPackfilesSnapshot(t)

			// the last leaf: the walk fails after having gone through
			// some of the tree.
			leaves := tc.nodes(t, snap)
			require.Greater(t, len(leaves), 1)
			leaf := leaves[len(leaves)-1]
			require.NoError(t, repo.DeletePackfile(packfileOf(t, repo, tc.rtype, leaf)))

			snap = reload(t, repo, snap)
			_, errs := listPackfiles(t, snap)

			require.NotEmpty(t, errs)
			var found bool
			for _, err := range errs {
				if err != nil && strings.Contains(err.Error(), tc.err) {
					found = true
				}
			}
			require.True(t, found, "no %q error among %v", tc.err, errs)
		})
	}
}

// A consumer stopping early ends the listing right away.
func TestListPackfilesEarlyStop(t *testing.T) {
	_, snap := generateListPackfilesSnapshot(t)
	defer snap.Close()

	it, err := snap.ListPackfiles()
	require.NoError(t, err)

	n := 0
	for _, err := range it {
		require.NoError(t, err)
		n++
		if n == 10 {
			break
		}
	}
	require.Equal(t, 10, n)
}
