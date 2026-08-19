package snapshot

import (
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

func (s *Snapshot) Filesystem() (*vfs.Filesystem, error) {
	if s.filesystem != nil {
		return s.filesystem, nil
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

func (s *Snapshot) FilesystemWithCache() (*vfs.Filesystem, error) {
	if s.filesystem != nil {
		return s.filesystem, nil
	}

	didx, err := s.DirPack()
	if err != nil {
		return nil, err
	}

	v := s.Header.GetSource(0).VFS
	fs, err := vfs.NewFilesystemWithCache(s.repository, v.Root, v.Xattrs, v.Errors, didx)
	if err != nil {
		return nil, err
	}

	s.filesystem = fs
	return fs, nil
}
