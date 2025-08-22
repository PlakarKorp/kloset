package resources

type Type uint32

const (
	RT_CONFIG    Type = 1
	RT_LOCK      Type = 2
	RT_STATE     Type = 3
	RT_PACKFILE  Type = 4
	RT_SNAPSHOT  Type = 5
	RT_SIGNATURE Type = 6
	RT_OBJECT    Type = 7
	// Type = 8 - unused, can't be used
	RT_CHUNK       Type = 9
	RT_VFS_BTREE   Type = 10
	RT_VFS_NODE    Type = 11
	RT_VFS_ENTRY   Type = 12
	RT_ERROR_BTREE Type = 13
	RT_ERROR_NODE  Type = 14
	RT_ERROR_ENTRY Type = 15
	RT_XATTR_BTREE Type = 16
	RT_XATTR_NODE  Type = 17
	RT_XATTR_ENTRY Type = 18
	RT_BTREE_ROOT  Type = 19
	RT_BTREE_NODE  Type = 20

	// Type is a uint32 but we can't set it a value > 255 as state v1
	// assume it's a uint8
	RT_RANDOM Type = 255
)

func Types() []Type {
	return []Type{
		RT_CONFIG,
		RT_LOCK,
		RT_STATE,
		RT_PACKFILE,
		RT_SNAPSHOT,
		RT_SIGNATURE,
		RT_OBJECT,
		RT_CHUNK,
		RT_VFS_BTREE,
		RT_VFS_NODE,
		RT_VFS_ENTRY,
		RT_ERROR_BTREE,
		RT_ERROR_NODE,
		RT_ERROR_ENTRY,
		RT_XATTR_BTREE,
		RT_XATTR_NODE,
		RT_XATTR_ENTRY,
		RT_BTREE_ROOT,
		RT_BTREE_NODE,
		RT_RANDOM,
	}
}

func (r Type) String() string {
	switch r {
	case RT_CONFIG:
		return "config"
	case RT_LOCK:
		return "lock"
	case RT_STATE:
		return "state"
	case RT_PACKFILE:
		return "packfile"
	case RT_SNAPSHOT:
		return "snapshot"
	case RT_SIGNATURE:
		return "signature"
	case RT_OBJECT:
		return "object"
	case RT_CHUNK:
		return "chunk"
	case RT_VFS_BTREE:
		return "vfs-btree"
	case RT_VFS_NODE:
		return "vfs-node"
	case RT_VFS_ENTRY:
		return "vfs-entry"
	case RT_ERROR_BTREE:
		return "error-btree"
	case RT_ERROR_NODE:
		return "error-node"
	case RT_ERROR_ENTRY:
		return "error-entry"
	case RT_XATTR_BTREE:
		return "xattr-btree"
	case RT_XATTR_NODE:
		return "xattr-node"
	case RT_XATTR_ENTRY:
		return "xattr-entry"
	case RT_BTREE_ROOT:
		return "btree-root"
	case RT_BTREE_NODE:
		return "btree-node"
	case RT_RANDOM:
		return "random"
	default:
		return "unknown"
	}
}
