package vfs

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Entries decoded from a repository must describe a path within the tree they
// belong to.  These cover the malformed name and parent-path shapes validate
// is meant to reject, alongside the legitimate ones it must accept.
func TestEntryValidate(t *testing.T) {
	for _, tc := range []struct {
		name       string
		parentPath string
		lname      string
		wantErr    bool
	}{
		{"plain file", "/etc", "passwd", false},
		{"file at root", "/", "passwd", false},
		{"root entry, empty parent", "", "/", false},
		{"root entry, slash parent", "/", "/", false},
		{"dotfile is fine", "/home/user", ".bashrc", false},
		{"name with dots is fine", "/tmp", "..hidden", false},

		{"separator in name", "/tmp", "link/child", true},
		{"parent traversal name", "/tmp", "..", true},
		{"current dir name", "/tmp", ".", true},
		{"empty name", "/tmp", "", true},
		{"absolute name", "/tmp", "/etc/passwd", true},
		{"relative parent", "tmp", "file", true},
		{"unclean parent", "/tmp/../etc", "passwd", true},
		{"parent with trailing slash", "/tmp/", "file", true},
		{"empty parent non-root name", "", "file", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &Entry{
				ParentPath: tc.parentPath,
				FileInfo:   objects.FileInfo{Lname: tc.lname},
			}
			err := e.validate()
			if tc.wantErr {
				require.ErrorIs(t, err, ErrMalformedEntry)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// EntryFromBytes is the single funnel for entry deserialization, so the
// validation has to bite there and not only in the explicit call.
func TestEntryFromBytesRejectsMalformed(t *testing.T) {
	malformed := &Entry{
		ParentPath: "/",
		FileInfo:   objects.FileInfo{Lname: "link/child"},
	}
	buf, err := msgpack.Marshal(malformed)
	require.NoError(t, err)

	_, err = EntryFromBytes(buf)
	require.ErrorIs(t, err, ErrMalformedEntry)
}

func TestEntryFromBytesAcceptsWellFormed(t *testing.T) {
	good := &Entry{
		ParentPath: "/etc",
		FileInfo:   objects.FileInfo{Lname: "passwd"},
	}
	buf, err := msgpack.Marshal(good)
	require.NoError(t, err)

	entry, err := EntryFromBytes(buf)
	require.NoError(t, err)
	require.Equal(t, "/etc/passwd", entry.Path())
}

// A well-formed name under a parent that happens to be a symlink passes
// validate on its own: name and parent are both structurally fine.  This is why
// the dirpack parent check and the export-side symlink guard exist in addition
// to validate -- structural validation alone does not cover this case.
func TestEntryPathValidUnderSymlinkParent(t *testing.T) {
	e := &Entry{
		ParentPath: "/link",
		FileInfo:   objects.FileInfo{Lname: "child"},
	}
	require.NoError(t, e.validate())
	require.Equal(t, "/link/child", e.Path())
}
