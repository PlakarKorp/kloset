package vfs

import (
	"iter"
	"path"

	"github.com/PlakarKorp/kloset/objects"
)

// Quirk for unclean root bug. Some importers (and kloset) were not cleaning up
// the importer's Root() path, as a result in backup when doing the relink of
// the importer's Root node, we would insert the directory as a child of itself
// leaving a "shadow" node in the Root's dirpack.

type uncleanImporterRoot struct {
	key    string
	parent string
	name   string
	mac    objects.MAC
}

// Detect the Unclean importer root bug and hides the resulting shadow entry
// from most listing. (Most because we still list in in ListPackfiles, for
// maintenance to be able to collect it)
func (fsc *Filesystem) DetectUncleanImporterRoot(root string) error {
	if root == path.Clean(root) {
		return nil
	}

	// We have a faulty looking snapshot, let's lookup the shadow entry
	mac, found, err := fsc.tree.Find(root)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	fsc.uncleanImporterRoot = &uncleanImporterRoot{
		key:    root,
		parent: path.Dir(root),
		name:   path.Base(root),
		mac:    mac,
	}
	return nil
}

func (fsc *Filesystem) isUncleanImporterRoot(key string) bool {
	return fsc.uncleanImporterRoot != nil && key == fsc.uncleanImporterRoot.key
}

// skipUncleanImporterRoot drops the phantom directory from the dirpack listing of
// dirpath. We only skip the first, in the possible event that there is a real
// same name directory under the actual root.
func (fsc *Filesystem) skipUncleanImporterRoot(dirpath string, dents iter.Seq2[*Entry, error]) iter.Seq2[*Entry, error] {
	u := fsc.uncleanImporterRoot
	if u == nil || dirpath != u.parent {
		return dents
	}

	return func(yield func(*Entry, error) bool) {
		skipped := false
		for entry, err := range dents {
			if err == nil && !skipped && entry.FileInfo.Lname == u.name && entry.MAC == u.mac {
				skipped = true
				continue
			}
			if !yield(entry, err) {
				return
			}
		}
	}
}
