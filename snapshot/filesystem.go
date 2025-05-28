package snapshot

import (
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/PlakarKorp/kloset/storage"
)

func (s *Snapshot) Filesystem(idx int) (*vfs.Filesystem, error) {
	if s.repository.Store().Mode()&storage.ModeRead == 0 {
		return nil, repository.ErrNotReadable
	}

	v := s.Header.GetSource(idx).VFS

	if s.filesystem != nil {
		return s.filesystem, nil
	} else if fs, err := vfs.NewFilesystem(s.repository, v.Root, v.Xattrs, v.Errors); err != nil {
		return nil, err
	} else {
		s.filesystem = fs
		return fs, nil
	}
}
