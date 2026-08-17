package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSymlinkAncestor(t *testing.T) {
	t.Run("empty set never matches", func(t *testing.T) {
		_, ok := symlinkAncestor(map[string]struct{}{}, "/link/child")
		require.False(t, ok)
	})

	symlinks := map[string]struct{}{
		"/link":            {},
		"/deep/nested/lnk": {},
	}

	t.Run("direct child of symlink", func(t *testing.T) {
		parent, ok := symlinkAncestor(symlinks, "/link/child")
		require.True(t, ok)
		require.Equal(t, "/link", parent)
	})

	t.Run("deep descendant of symlink", func(t *testing.T) {
		parent, ok := symlinkAncestor(symlinks, "/link/a/b/c")
		require.True(t, ok)
		require.Equal(t, "/link", parent)
	})

	t.Run("nested symlink is caught", func(t *testing.T) {
		parent, ok := symlinkAncestor(symlinks, "/deep/nested/lnk/evil")
		require.True(t, ok)
		require.Equal(t, "/deep/nested/lnk", parent)
	})

	// The symlink entry itself must still be exported -- it is a legitimate
	// entry, and only things *below* it are refused.
	t.Run("symlink itself is not its own ancestor", func(t *testing.T) {
		_, ok := symlinkAncestor(symlinks, "/link")
		require.False(t, ok)
	})

	t.Run("unrelated paths pass", func(t *testing.T) {
		for _, p := range []string{"/etc/passwd", "/linkage/file", "/deep/nested/other", "/"} {
			_, ok := symlinkAncestor(symlinks, p)
			require.False(t, ok, "path %q should not be blocked", p)
		}
	})

	// A sibling whose name merely shares a prefix with the symlink must not be
	// caught by a sloppy string-prefix check.
	t.Run("prefix sibling is not blocked", func(t *testing.T) {
		_, ok := symlinkAncestor(symlinks, "/link2/file")
		require.False(t, ok)
	})
}
