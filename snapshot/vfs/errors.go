package vfs

import (
	"io"
	"iter"
	"strings"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/iterator"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const VFS_ERROR_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_ERROR_BTREE, versioning.FromString(btree.BTREE_VERSION))
	versioning.Register(resources.RT_ERROR_NODE, versioning.FromString(btree.NODE_VERSION))
	versioning.Register(resources.RT_ERROR_ENTRY, versioning.FromString(VFS_ERROR_VERSION))
}

type ErrorItem struct {
	Version versioning.Version `msgpack:"version" json:"version"`
	Name    string             `msgpack:"name" json:"name"`
	Error   string             `msgpack:"error" json:"error"`
}

func (e *ErrorItem) ToBytes() ([]byte, error) {
	return msgpack.Marshal(e)
}

func ErrorItemFromBytes(bytes []byte) (*ErrorItem, error) {
	e := &ErrorItem{}
	err := msgpack.Unmarshal(bytes, e)
	return e, err
}

func ErrorNodeFromBytes(bytes []byte) (*btree.Node[string, objects.MAC, objects.MAC], error) {
	var e btree.Node[string, objects.MAC, objects.MAC]
	err := msgpack.Unmarshal(bytes, &e)
	return &e, err
}

func NewErrorItem(path, error string) *ErrorItem {
	return &ErrorItem{
		Version: versioning.FromString(VFS_ERROR_VERSION),
		Name:    path,
		Error:   error,
	}
}

func (fsc *Filesystem) Errors(beneath string) (iter.Seq2[*ErrorItem, error], error) {
	if !strings.HasSuffix(beneath, "/") {
		beneath += "/"
	}

	return func(yield func(*ErrorItem, error) bool) {
		iter, err := fsc.errors.ScanFrom(beneath)
		if err != nil {
			yield(&ErrorItem{}, err)
			return
		}

		for iter.Next() {
			_, csum := iter.Current()

			rd, err := fsc.repo.GetBlob(resources.RT_ERROR_ENTRY, csum)
			if err != nil {
				yield(&ErrorItem{}, err)
				return
			}

			bytes, err := io.ReadAll(rd)
			if err != nil {
				yield(&ErrorItem{}, err)
				return
			}

			item, err := ErrorItemFromBytes(bytes)
			if err != nil {
				yield(&ErrorItem{}, err)
				return
			}

			if !strings.HasPrefix(item.Name, beneath) {
				break
			}
			if !yield(item, nil) {
				return
			}
		}
		if err := iter.Err(); err != nil {
			yield(&ErrorItem{}, err)
			return
		}
	}, nil
}

func (fsc *Filesystem) IterErrorNodes() iterator.Iterator[objects.MAC, *btree.Node[string, objects.MAC, objects.MAC]] {
	return fsc.errors.IterDFS()
}
