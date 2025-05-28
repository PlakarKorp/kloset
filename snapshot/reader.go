package snapshot

import (
	"io"
	"io/fs"
	"os"
	"path"
)

func (snapshot *Snapshot) NewReader(pathname string) (io.ReadCloser, error) {
	return NewReader(snapshot, pathname)
}

func NewReader(snap *Snapshot, pathname string) (io.ReadCloser, error) {
	pathname = path.Clean(pathname)

	fsc, err := snap.Filesystem(0) //TODO: use the correct index
	if err != nil {
		return nil, err
	}

	file, err := fsc.Open(pathname)
	if err != nil {
		return nil, err
	}

	if _, isdir := file.(fs.ReadDirFile); isdir {
		file.Close()
		return nil, os.ErrInvalid
	}
	return file, nil
}
