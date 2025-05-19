package repository

import (
	"errors"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/vmihailenco/msgpack/v5"
)

var ErrStoreReadOnly = errors.New("read only store")

type RepositoryStore[K, V any] struct {
	repo     *Repository
	blobtype resources.Type
}

func NewRepositoryStore[K, V any](repo *Repository, blobtype resources.Type) *RepositoryStore[K, V] {
	return &RepositoryStore[K, V]{
		repo:     repo,
		blobtype: blobtype,
	}
}

func (rs *RepositoryStore[K, V]) Get(sum objects.MAC) (*btree.Node[K, objects.MAC, V], error) {
	rd, err := rs.repo.GetBlob(rs.blobtype, sum)
	if err != nil {
		return nil, err
	}
	node := &btree.Node[K, objects.MAC, V]{}
	err = msgpack.NewDecoder(rd).Decode(node)
	return node, nil
}

func (rs *RepositoryStore[K, V]) Update(sum objects.MAC, node *btree.Node[K, objects.MAC, V]) error {
	return ErrStoreReadOnly
}

func (rs *RepositoryStore[K, V]) Put(node *btree.Node[K, objects.MAC, V]) (csum objects.MAC, err error) {
	return csum, ErrStoreReadOnly
}
