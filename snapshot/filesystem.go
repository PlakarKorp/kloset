package snapshot

import (
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/PlakarKorp/kloset/storage"
)

func (s *Snapshot) Filesystem() (*vfs.Filesystem, error) {
	if s.filesystem != nil {
		return s.filesystem, nil
	}

	mode, err := s.repository.Store().Mode(s.AppContext())
	if err != nil {
		return nil, err
	}

	if mode&storage.ModeRead == 0 {
		return nil, repository.ErrNotReadable
	}

	didx, err := s.DirPack()
	if err != nil {
		return nil, err
	}

	v := s.Header.GetSource(0).VFS
	fs, err := vfs.NewFilesystem(s.repository, v.Root, v.Xattrs, v.Errors, didx)
	if err != nil {
		return nil, err
	}

	s.filesystem = fs
	return fs, nil
}
