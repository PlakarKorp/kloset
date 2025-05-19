package vfs

import (
	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const VFS_XATTR_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_XATTR_BTREE, versioning.FromString(btree.BTREE_VERSION))
	versioning.Register(resources.RT_XATTR_NODE, versioning.FromString(btree.NODE_VERSION))
	versioning.Register(resources.RT_XATTR_ENTRY, versioning.FromString(VFS_XATTR_VERSION))
}

type Xattr struct {
	Version versioning.Version `msgpack:"version" json:"version"`
	Path    string             `msgpack:"path" json:"path"`
	Name    string             `msgpack:"name" json:"name"`
	Size    int64              `msgpack:"size" json:"size"`
	Type    objects.Attribute  `msgpack:"type" json:"type"`
	Object  objects.MAC        `msgpack:"object,omitempty" json:"-"`

	// This the true object, resolved when opening the
	// xattr. Beware we serialize it as "Object" only for json to
	// not break API compat.
	ResolvedObject *objects.Object `msgpack:"-" json:"object,omitempty"`
}

func NewXattr(record *importer.ScanRecord, objectMAC objects.MAC, size int64) *Xattr {
	return &Xattr{
		Version: versioning.FromString(VFS_XATTR_VERSION),
		Path:    record.Pathname,
		Name:    record.XattrName,
		Type:    record.XattrType,
		Object:  objectMAC,
		Size:    size,
	}
}

func (x *Xattr) ToPath() string {
	var sep string
	switch x.Type {
	case objects.AttributeExtended:
		sep = ":"
	case objects.AttributeADS:
		sep = "@"
	default:
		sep = "#"
	}
	return x.Path + x.Name + sep
}

func XattrNodeFromBytes(bytes []byte) (*btree.Node[string, objects.MAC, objects.MAC], error) {
	var node btree.Node[string, objects.MAC, objects.MAC]
	err := msgpack.Unmarshal(bytes, &node)
	return &node, err
}

func XattrFromBytes(bytes []byte) (*Xattr, error) {
	xattr := &Xattr{}
	err := msgpack.Unmarshal(bytes, &xattr)
	return xattr, err
}

func (x *Xattr) ToBytes() ([]byte, error) {
	return msgpack.Marshal(x)
}
