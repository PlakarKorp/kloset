package resources_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

func TestTypeString(t *testing.T) {
	cases := []struct {
		typ  resources.Type
		want string
	}{
		{resources.RT_CONFIG, "config"},
		{resources.RT_LOCK, "lock"},
		{resources.RT_STATE, "state"},
		{resources.RT_PACKFILE, "packfile"},
		{resources.RT_SNAPSHOT, "snapshot"},
		{resources.RT_SIGNATURE, "signature"},
		{resources.RT_OBJECT, "object"},
		{resources.RT_CHUNK, "chunk"},
		{resources.RT_VFS_BTREE, "vfs-btree"},
		{resources.RT_VFS_NODE, "vfs-node"},
		{resources.RT_VFS_ENTRY, "vfs-entry"},
		{resources.RT_ERROR_BTREE, "error-btree"},
		{resources.RT_ERROR_NODE, "error-node"},
		{resources.RT_ERROR_ENTRY, "error-entry"},
		{resources.RT_XATTR_BTREE, "xattr-btree"},
		{resources.RT_XATTR_NODE, "xattr-node"},
		{resources.RT_XATTR_ENTRY, "xattr-entry"},
		{resources.RT_BTREE_ROOT, "btree-root"},
		{resources.RT_BTREE_NODE, "btree-node"},
		{resources.RT_VFS_SUMMARY, "vfs-summary"},
		{resources.RT_RANDOM, "random"},
		{resources.Type(0), "unknown"},
		{resources.Type(255), "random"},
	}

	for _, c := range cases {
		got := c.typ.String()
		require.Equal(t, c.want, got, "Type(%d).String()", c.typ)
	}
}

func TestFromString(t *testing.T) {
	cases := []struct {
		name string
		typ  resources.Type
	}{
		{"config", resources.RT_CONFIG},
		{"lock", resources.RT_LOCK},
		{"state", resources.RT_STATE},
		{"packfile", resources.RT_PACKFILE},
		{"snapshot", resources.RT_SNAPSHOT},
		{"signature", resources.RT_SIGNATURE},
		{"object", resources.RT_OBJECT},
		{"chunk", resources.RT_CHUNK},
		{"vfs-btree", resources.RT_VFS_BTREE},
		{"vfs-node", resources.RT_VFS_NODE},
		{"vfs-entry", resources.RT_VFS_ENTRY},
		{"error-btree", resources.RT_ERROR_BTREE},
		{"error-node", resources.RT_ERROR_NODE},
		{"error-entry", resources.RT_ERROR_ENTRY},
		{"xattr-btree", resources.RT_XATTR_BTREE},
		{"xattr-node", resources.RT_XATTR_NODE},
		{"xattr-entry", resources.RT_XATTR_ENTRY},
		{"btree-root", resources.RT_BTREE_ROOT},
		{"btree-node", resources.RT_BTREE_NODE},
		{"vfs-summary", resources.RT_VFS_SUMMARY},
		{"random", resources.RT_RANDOM},
	}

	for _, c := range cases {
		got, err := resources.FromString(c.name)
		require.NoError(t, err, "FromString(%q)", c.name)
		require.Equal(t, c.typ, got, "FromString(%q)", c.name)
	}
}

func TestFromStringUnknown(t *testing.T) {
	_, err := resources.FromString("unknown-type")
	require.ErrorIs(t, err, resources.ErrInvalid)
}

func TestFromStringRoundTrip(t *testing.T) {
	for _, typ := range resources.Types() {
		name := typ.String()
		if name == "unknown" {
			continue
		}
		got, err := resources.FromString(name)
		require.NoError(t, err, "round-trip for %v", typ)
		require.Equal(t, typ, got, "round-trip for %v", typ)
	}
}

func TestTypes(t *testing.T) {
	types := resources.Types()
	require.NotEmpty(t, types)

	// All returned types should have non-empty string representations
	for _, typ := range types {
		s := typ.String()
		require.NotEmpty(t, s)
		require.NotEqual(t, "unknown", s, "Types() should not return unknown type values")
	}
}

func TestTypeValues(t *testing.T) {
	// Verify the constant values match the documented spec
	require.Equal(t, resources.Type(1), resources.RT_CONFIG)
	require.Equal(t, resources.Type(2), resources.RT_LOCK)
	require.Equal(t, resources.Type(3), resources.RT_STATE)
	require.Equal(t, resources.Type(4), resources.RT_PACKFILE)
	require.Equal(t, resources.Type(5), resources.RT_SNAPSHOT)
	require.Equal(t, resources.Type(6), resources.RT_SIGNATURE)
	require.Equal(t, resources.Type(7), resources.RT_OBJECT)
	require.Equal(t, resources.Type(9), resources.RT_CHUNK)
	require.Equal(t, resources.Type(255), resources.RT_RANDOM)
}
