package snapshot

import (
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

type ExportOptions struct {
	Strip           string
	SkipPermissions bool
	ForceCompletion bool
}

func (snap *Snapshot) Export(exp exporter.Exporter, pathname string, opts *ExportOptions) error {
	emitter := snap.Emitter("export")
	defer emitter.Close()

	// these are wrong and need to be actually computed from the
	// entry that we're about to walk.  keeping for now for
	// simplicity.
	fileCount := snap.Header.GetSource(0).Summary.Directory.Files + snap.Header.GetSource(0).Summary.Below.Files
	dirCount := snap.Header.GetSource(0).Summary.Directory.Directories + snap.Header.GetSource(0).Summary.Below.Directories - uint64(len(strings.Split(pathname, "/"))-2)
	symlinkCount := snap.Header.GetSource(0).Summary.Directory.Symlinks + snap.Header.GetSource(0).Summary.Below.Symlinks
	totalSize := snap.Header.GetSource(0).Summary.Directory.Size + snap.Header.GetSource(0).Summary.Below.Size

	emitter.FilesystemSummary(fileCount, dirCount, symlinkCount, 0, totalSize)

	pvfs, err := snap.Filesystem()
	if err != nil {
		return err
	}

	entry, err := pvfs.GetEntry(pathname)
	if err != nil {
		return err
	}
	pathname = entry.Path()

	tostrip := pathname
	if !entry.IsDir() {
		tostrip = path.Dir(tostrip)
		if tostrip == "/" {
			tostrip = ""
		}
	}

	records := make(chan *connectors.Record, snap.AppContext().MaxConcurrency*2)
	results := make(chan *connectors.Result, snap.AppContext().MaxConcurrency*2)

	go func() {
		for ack := range results {
			if ack.Err != nil {
				if ack.Record.FileInfo.IsDir() {
					emitter.PathError(ack.Record.Pathname, ack.Err)
					emitter.DirectoryError(ack.Record.Pathname, ack.Err)
				} else {
					emitter.PathError(ack.Record.Pathname, ack.Err)
					emitter.FileError(ack.Record.Pathname, ack.Err)
				}
			} else if !ack.Record.IsXattr {
				if ack.Record.FileInfo.IsDir() {
					emitter.DirectoryOk(ack.Record.Pathname, ack.Record.FileInfo)
					emitter.PathOk(ack.Record.Pathname)
				} else {
					emitter.FileOk(ack.Record.Pathname, ack.Record.FileInfo)
					emitter.PathOk(ack.Record.Pathname)
				}
			}
		}
	}()

	go func() {
		// We are a single file, let's emit the root dir to create the parent
		// dir if any.
		if !entry.IsDir() {
			records <- connectors.NewRecord("/", "", objects.FileInfo{Lname: "/", Lmode: 0700 | os.ModeDir, LmodTime: time.Now()}, nil,
				func() (io.ReadCloser, error) {
					return nil, nil
				})
		}

		i := 0
		pvfs.WalkDir(pathname, func(entrypath string, e *vfs.Entry, err error) error {
			if i%1000 == 0 {
				if err := snap.AppContext().Err(); err != nil {
					return err
				}
			}
			i++

			// simulate a chroot: strip the parent path from the entry path
			entrypath = strings.TrimPrefix(entrypath, tostrip)
			if entrypath == "" {
				entrypath = "/"
			}

			emitter.Path(entrypath)

			if e.FileInfo.IsDir() {
				emitter.Directory(entrypath)
			} else if e.FileInfo.Mode().IsRegular() {
				emitter.File(entrypath)
			}
			records <- connectors.NewRecord(entrypath, e.SymlinkTarget, e.FileInfo, e.ExtendedAttributes,
				func() (io.ReadCloser, error) {
					return e.Open(pvfs)
				})
			return nil
		})
		close(records)
	}()

	return exp.Export(snap.AppContext(), records, results)
}
