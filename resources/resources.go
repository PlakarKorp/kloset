package resources

import "errors"

type Type uint32

var (
	ErrInvalid = errors.New("invalid type")
)

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

func FromString(typ string) (Type, error) {
	switch typ {
	case "config":
		return RT_CONFIG, nil
	case "lock":
		return RT_LOCK, nil
	case "state":
		return RT_STATE, nil
	case "packfile":
		return RT_PACKFILE, nil
	case "snapshot":
		return RT_SNAPSHOT, nil
	case "signature":
		return RT_SIGNATURE, nil
	case "object":
		return RT_OBJECT, nil
	case "chunk":
		return RT_CHUNK, nil
	case "vfs-btree":
		return RT_VFS_BTREE, nil
	case "vfs-node":
		return RT_VFS_NODE, nil
	case "vfs-entry":
		return RT_VFS_ENTRY, nil
	case "error-btree":
		return RT_ERROR_BTREE, nil
	case "error-node":
		return RT_ERROR_NODE, nil
	case "error-entry":
		return RT_ERROR_ENTRY, nil
	case "xattr-btree":
		return RT_XATTR_BTREE, nil
	case "xattr-node":
		return RT_XATTR_NODE, nil
	case "xattr-entry":
		return RT_XATTR_ENTRY, nil
	case "btree-root":
		return RT_BTREE_ROOT, nil
	case "btree-node":
		return RT_BTREE_NODE, nil
	case "random":
		return RT_RANDOM, nil
	default:
		return Type(0), ErrInvalid
	}
}
