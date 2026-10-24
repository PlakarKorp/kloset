package snapshot_test

import (
	"io"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// generateXattrBackup builds a snapshot rooted a few directories deep, with
// extended attributes on files at two depths and on a directory, so that the
// index keys sort differently under PathCmp and plain string comparison.
func generateXattrBackup(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	dir := func(p string) *connectors.Record {
		return connectors.NewRecord(p, "", objects.FileInfo{
			Lname: path.Base(p), Lmode: os.ModeDir | 0755,
		}, nil, nil)
	}
	file := func(p, content string, xattrs map[string]string) []*connectors.Record {
		names := slices.Sorted(maps.Keys(xattrs))
		recs := []*connectors.Record{connectors.NewRecord(p, "", objects.FileInfo{
			Lname: path.Base(p), Lmode: 0644, Lsize: int64(len(content)),
		}, names, func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(content)), nil
		})}
		for _, name := range names {
			value := xattrs[name]
			recs = append(recs, connectors.NewXattr(p, name, objects.AttributeExtended,
				func() (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader(value)), nil
				}))
		}
		return recs
	}

	return ptesting.GenerateSnapshot(t, repo, nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			for _, p := range []string{"/", "/tmp", "/tmp/src", "/tmp/src/sub"} {
				ch <- dir(p)
			}
			ch <- connectors.NewXattr("/tmp/src/sub", "com.apple.provenance", objects.AttributeExtended,
				func() (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("dir")), nil
				})
			for _, r := range file("/tmp/src/plain.txt", "plain", map[string]string{
				"com.apple.provenance": "p1",
			}) {
				ch <- r
			}
			for _, r := range file("/tmp/src/sub/file.txt", "payload here", map[string]string{
				"com.apple.provenance": "p2",
				"user.bin":             "a\tb",
				"user.note":            "note value",
			}) {
				ch <- r
			}
		}),
	)
}

// TestEntryXattrAcrossDepths looks up every recorded xattr through
// Entry.Xattr. The index is built with PathCmp, so opening it with a
// different comparator makes the binary search miss keys once entries at
// several depths coexist.
func TestEntryXattrAcrossDepths(t *testing.T) {
	snap := generateXattrBackup(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	want := map[string]map[string]string{
		"/tmp/src/plain.txt": {"com.apple.provenance": "p1"},
		"/tmp/src/sub/file.txt": {
			"com.apple.provenance": "p2",
			"user.bin":             "a\tb",
			"user.note":            "note value",
		},
	}
	for p, xattrs := range want {
		e, err := fs.GetEntry(p)
		require.NoError(t, err)
		require.ElementsMatch(t, slices.Sorted(maps.Keys(xattrs)), e.ExtendedAttributes, p)
		for name, value := range xattrs {
			rd, err := e.Xattr(fs, name)
			require.NoError(t, err, "%s %s", p, name)
			data, err := io.ReadAll(rd)
			require.NoError(t, err)
			require.Equal(t, value, string(data), "%s %s", p, name)
		}
	}
}
