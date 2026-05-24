package locate_test

import (
	"encoding/hex"
	"testing"

	"github.com/PlakarKorp/kloset/locate"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestParseSnapshotPath tests the pure string-parsing helper.
func TestParseSnapshotPath(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantPrefix    string
		wantPattern   string
	}{
		{
			name:        "absolute path — no prefix",
			input:       "/foo/bar",
			wantPrefix:  "",
			wantPattern: "/foo/bar",
		},
		{
			name:        "prefix only — no colon",
			input:       "abc123",
			wantPrefix:  "abc123",
			wantPattern: "",
		},
		{
			name:        "prefix with pattern",
			input:       "abc123:/some/path",
			wantPrefix:  "abc123",
			wantPattern: "/some/path",
		},
		{
			name:        "prefix with multiple colons — only first splits",
			input:       "abc123:/some:path",
			wantPrefix:  "abc123",
			wantPattern: "/some:path",
		},
		{
			name:        "empty string",
			input:       "",
			wantPrefix:  "",
			wantPattern: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPrefix, gotPattern := locate.ParseSnapshotPath(tt.input)
			require.Equal(t, tt.wantPrefix, gotPrefix)
			require.Equal(t, tt.wantPattern, gotPattern)
		})
	}
}

func TestLocateSnapshotByPrefix(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockFile("subdir/a.txt", 0644, "hello"),
	}
	s1 := ptesting.GenerateSnapshot(t, repo, files)
	s2 := ptesting.GenerateSnapshot(t, repo, files)
	defer s1.Close()
	defer s2.Close()

	// LookupSnapshotByPrefix with empty prefix must list all snapshots.
	all := locate.LookupSnapshotByPrefix(repo, "")
	require.GreaterOrEqual(t, len(all), 2)

	id1 := s1.Header.Identifier
	hexID := hex.EncodeToString(id1[:])

	// Full hex must locate the exact snapshot.
	got, err := locate.LocateSnapshotByPrefix(repo, hexID)
	require.NoError(t, err)
	require.Equal(t, id1, got)

	// Bogus prefix must error.
	_, err = locate.LocateSnapshotByPrefix(repo, "deadbeefnotanid")
	require.Error(t, err)
}

func TestLocateSnapshotIDsAndMatch(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockFile("subdir/a.txt", 0644, "hello"),
	}
	s1 := ptesting.GenerateSnapshot(t, repo, files)
	s2 := ptesting.GenerateSnapshot(t, repo, files)
	defer s1.Close()
	defer s2.Close()

	ids, err := locate.LocateSnapshotIDs(repo, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(ids), 2)

	matches, reasons, err := locate.Match(repo, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(matches), 2)
	require.NotNil(t, reasons)
}

func TestOpenSnapshotByPath(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockFile("subdir/a.txt", 0644, "hello"),
	}
	s := ptesting.GenerateSnapshot(t, repo, files)
	defer s.Close()

	id := s.Header.Identifier
	hexID := hex.EncodeToString(id[:])

	snap, root, err := locate.OpenSnapshotByPath(repo, hexID+":/subdir")
	require.NoError(t, err)
	require.NotNil(t, snap)
	defer snap.Close()
	require.Contains(t, root, "/subdir")

	snap2, root2, rel, err := locate.OpenSnapshotByPathRelative(repo, hexID+":subdir")
	require.NoError(t, err)
	require.NotNil(t, snap2)
	defer snap2.Close()
	require.NotEmpty(t, root2)
	require.Equal(t, "subdir", rel)
}
