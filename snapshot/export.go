package snapshot

import (
	"io"
	"strings"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

func (snap *Snapshot) Export(exp exporter.Exporter, base string, pathname string, opts *RestoreOptions) error {
	emitter := snap.Emitter("restore")
	defer emitter.Close()

	fileCount := snap.Header.GetSource(0).Summary.Directory.Files + snap.Header.GetSource(0).Summary.Below.Files
	dirCount := snap.Header.GetSource(0).Summary.Directory.Directories + snap.Header.GetSource(0).Summary.Below.Directories - uint64(len(strings.Split(pathname, "/"))-2)
	symlinkCount := snap.Header.GetSource(0).Summary.Directory.Symlinks + snap.Header.GetSource(0).Summary.Below.Symlinks
	totalSize := snap.Header.GetSource(0).Summary.Directory.Size + snap.Header.GetSource(0).Summary.Below.Size

	emitter.FilesystemSummary(fileCount, dirCount, symlinkCount, 0, totalSize)

	pvfs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	c := make(chan *connectors.Record, 1000)
	r := make(chan *connectors.Result, 1000)

	go func() {
		for ack := range r {
			if ack.Err != nil {
				if ack.Record.FileInfo.IsDir() {
					emitter.DirectoryError(ack.Record.Pathname, ack.Err)
				} else {
					emitter.FileError(ack.Record.Pathname, ack.Err)
				}
			} else if !ack.Record.IsXattr {
				if ack.Record.FileInfo.IsDir() {
					emitter.DirectoryOk(ack.Record.Pathname)
				} else {
					emitter.FileOk(ack.Record.Pathname)
				}
			}
		}
	}()

	go func() {
		defer close(c)

		i := 1
		if err := pvfs.WalkDir("/", func(entrypath string, e *vfs.Entry, err error) error {
			if i%1000 == 0 {
				if err := snap.AppContext().Err(); err != nil {
					return err
				}
			}
			if e.FileInfo.IsDir() {
				emitter.Directory(entrypath)
			} else if e.FileInfo.Mode().IsRegular() {
				emitter.File(entrypath)
			}
			c <- connectors.NewRecord(entrypath, e.SymlinkTarget, e.FileInfo, e.ExtendedAttributes,
				func() (io.ReadCloser, error) {
					return e.Open(pvfs)
				})
			return nil
		}); err != nil {
			return
		}
	}()

	return exp.Export(snap.AppContext(), c, r)
}
