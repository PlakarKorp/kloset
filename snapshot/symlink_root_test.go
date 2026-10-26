package snapshot_test

import (
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func dirRecord(p string) *connectors.Record {
	return connectors.NewRecord(p, "", objects.FileInfo{Lname: path.Base(p), Lmode: os.ModeDir | 0o755}, nil, nil)
}

func fileRecord(p string) *connectors.Record {
	return connectors.NewRecord(p, "", objects.FileInfo{Lname: path.Base(p), Lsize: int64(len(p)), Lmode: 0o644}, nil,
		func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(p)), nil })
}

func symlinkRecord(p, target string) *connectors.Record {
	return connectors.NewRecord(p, target, objects.FileInfo{Lname: path.Base(p), Lmode: os.ModeSymlink | 0o777}, nil, nil)
}

// rootedSnapshot backs up records from an importer that reports root as
// its root, the way the fs importer does when pointed at a symlink: it
// emits the link, the directories above both the link and its target, and
// walks the target.
func rootedSnapshot(t *testing.T, root string, records ...*connectors.Record) *snapshot.Snapshot {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{})
	require.NoError(t, err)

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{}, "mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).SetGenerator(func(ch chan<- *connectors.Record) {
		for _, r := range records {
			ch <- r
		}
	})

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)
	snapshot.SetSourceRoot(src, root)

	require.NoError(t, builder.Backup(src))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, repo.RebuildState())

	snap, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)
	return snap
}

func errorsBeneath(t *testing.T, snap *snapshot.Snapshot, beneath string) []string {
	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var names []string
	for item, err := range fs.Errors(beneath) {
		require.NoError(t, err)
		names = append(names, item.Name)
	}
	return names
}

// The target tree the fs importer walks through a root symlink.
func targetTree() []*connectors.Record {
	return []*connectors.Record{
		dirRecord("/"), dirRecord("/x"), dirRecord("/x/real"), dirRecord("/x/real/sub"),
		fileRecord("/x/real/ok.txt"), fileRecord("/x/real/sub/deep.txt"),
		connectors.NewError("/x/real/sub/noread.txt", os.ErrPermission),
	}
}

func TestRootSymlinkToDirectory(t *testing.T) {
	records := append(targetTree(), dirRecord("/a"), dirRecord("/a/b"), symlinkRecord("/a/b/lnk", "/x/real"))
	snap := rootedSnapshot(t, "/a/b/lnk", records...)

	source := snap.Header.GetSource(0)
	require.Equal(t, "/x/real", source.Importer.Directory)
	require.Equal(t, uint64(1), source.Summary.Directory.Errors+source.Summary.Below.Errors)
	require.Equal(t, []string{"/x/real/sub/noread.txt"}, errorsBeneath(t, snap, source.Importer.Directory))

	fs, err := snap.Filesystem()
	require.NoError(t, err)
	require.Equal(t, []string{"ok.txt", "sub"}, childNames(t, fs, source.Importer.Directory))
}

func TestRootSymlinkRelativeTarget(t *testing.T) {
	records := append(targetTree(), dirRecord("/a"), dirRecord("/a/b"), symlinkRecord("/a/b/lnk", "../../x/real"))
	snap := rootedSnapshot(t, "/a/b/lnk", records...)

	require.Equal(t, "/x/real", snap.Header.GetSource(0).Importer.Directory)
}

func TestRootSymlinkToFile(t *testing.T) {
	records := append(targetTree(), dirRecord("/a"), dirRecord("/a/b"), symlinkRecord("/a/b/lnk", "/x/real/ok.txt"))
	snap := rootedSnapshot(t, "/a/b/lnk", records...)

	require.Equal(t, "/x/real", snap.Header.GetSource(0).Importer.Directory)
}

func TestRootSymlinkChain(t *testing.T) {
	records := append(targetTree(), dirRecord("/a"), dirRecord("/a/b"),
		symlinkRecord("/a/b/lnk", "/a/b/lnk2"), symlinkRecord("/a/b/lnk2", "/x/real"))
	snap := rootedSnapshot(t, "/a/b/lnk", records...)

	require.Equal(t, "/x/real", snap.Header.GetSource(0).Importer.Directory)
}

// An importer that does not follow a root symlink emits the link alone;
// the snapshot stays rooted at the link's parent, as before.
func TestRootSymlinkNotFollowed(t *testing.T) {
	snap := rootedSnapshot(t, "/a/b/lnk", dirRecord("/"), dirRecord("/a"), dirRecord("/a/b"), symlinkRecord("/a/b/lnk", "/x/real"))

	require.Equal(t, "/a/b", snap.Header.GetSource(0).Importer.Directory)
}

func TestRootSymlinkLoop(t *testing.T) {
	snap := rootedSnapshot(t, "/a/b/lnk", dirRecord("/"), dirRecord("/a"), dirRecord("/a/b"),
		symlinkRecord("/a/b/lnk", "lnk2"), symlinkRecord("/a/b/lnk2", "lnk"))

	require.Equal(t, "/a/b", snap.Header.GetSource(0).Importer.Directory)
}

func TestRootFile(t *testing.T) {
	snap := rootedSnapshot(t, "/x/real/ok.txt", targetTree()...)

	require.Equal(t, "/x/real", snap.Header.GetSource(0).Importer.Directory)
}
