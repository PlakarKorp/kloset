package snapshot_test

import (
	"context"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func generateSnapshotWithTracks(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	file := func(p, content string) *connectors.Record {
		return connectors.NewRecord(p, "", objects.FileInfo{
			Lname: path.Base(p),
			Lmode: 0644,
			Lsize: int64(len(content)),
		}, nil, func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(content)), nil
		})
	}

	return ptesting.GenerateSnapshot(t, repo, nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- connectors.NewRecord("/", "", objects.FileInfo{
				Lname: "/",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/music", "", objects.FileInfo{
				Lname: "music",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- file("/music/song11 (rock n roll).txt", "first track")
			ch <- file("/music/song12 (rock n roll).txt", "second track")
			ch <- file("/music/song1 (hiphop).txt", "third track")
		}))
}

func searchNames(t *testing.T, snap *snapshot.Snapshot, opts *snapshot.SearchOpts) []string {
	t.Helper()
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)

	var names []string
	for entry, err := range it {
		require.NoError(t, err)
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

// A name filter must find the same files whether the search goes through the
// content-type index (recursive, with mimes) or walks the tree: the index used
// to match globs only, so a bare word or a regexp found nothing there.
func TestSearchNameFilterIndexedMatchesWalk(t *testing.T) {
	snap := generateSnapshotWithTracks(t)
	defer snap.Close()

	rock := []string{"song11 (rock n roll).txt", "song12 (rock n roll).txt"}

	for _, tc := range []struct {
		pattern string
		want    []string
	}{
		{"rock", rock},        // substring, through the regexp
		{"^song1[12] ", rock}, // regexp
		{"*rock*", rock},      // glob
		{"song1 (hiphop).txt", []string{"song1 (hiphop).txt"}}, // exact name
		{"jazz", nil},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			walk := searchNames(t, snap, &snapshot.SearchOpts{
				Recursive:  true,
				Prefix:     "/",
				NameFilter: tc.pattern,
			})
			indexed := searchNames(t, snap, &snapshot.SearchOpts{
				Recursive:  true,
				Prefix:     "/",
				Mimes:      []string{"text"},
				NameFilter: tc.pattern,
			})
			require.Equal(t, tc.want, walk)
			require.Equal(t, tc.want, indexed)
		})
	}
}
