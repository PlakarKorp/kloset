package snapshot

import (
	"bytes"
	"strings"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

func (snap *Snapshot) getidx(name, kind string) (objects.MAC, bool) {
	for i := range snap.Header.Sources { // or maybe get i as a parameter of getidx
		source := snap.Header.GetSource(i)
		for i := range source.Indexes {
			if source.Indexes[i].Name == name && source.Indexes[i].Type == kind {
				return source.Indexes[i].Value, true
			}
		}
	}
	return objects.MAC{}, false
}

func (snap *Snapshot) ContentTypeIdx() (*btree.BTree[string, objects.MAC, objects.MAC], error) {
	mac, found := snap.getidx("content-type", "btree")
	if !found {
		return nil, nil
	}

	d, err := snap.repository.GetBlobBytes(resources.RT_BTREE_ROOT, mac)
	if err != nil {
		return nil, err
	}

	store := SnapshotStore[string, objects.MAC]{
		blobtype:   resources.RT_BTREE_NODE,
		snapReader: snap,
	}
	return btree.Deserialize(bytes.NewReader(d), &store, strings.Compare)
}
