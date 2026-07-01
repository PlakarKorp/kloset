package snapshot

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/iostat"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

// countingReadCloser accounts the bytes the exporter reads against a write
// span, so throughput reflects what is actually exported.
type countingReadCloser struct {
	rd   io.ReadCloser
	span *iostat.Span
}

func newCountingReadCloser(rd io.ReadCloser, span *iostat.Span) *countingReadCloser {
	return &countingReadCloser{rd: rd, span: span}
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rd.Read(p)
	if n > 0 {
		c.span.Add(int64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rd.Close()
}

type ExportOptions struct {
	Strip           string
	SkipPermissions bool
	ForceCompletion bool
}

func (snap *Snapshot) Export(exp exporter.Exporter, pathname string, opts *ExportOptions) error {
	emitter := snap.Emitter("export")
	defer emitter.Close()

	snap.repository.ExportStats = iostat.New()
	sampler := iostat.NewSampler(emitter, snap.AppContext().IOStatsInterval,
		iostat.ScopedTracker{Name: "destination", T: snap.repository.ExportStats},
		iostat.ScopedTracker{Name: "storage", T: snap.repository.IOStats()},
	)
	sampler.Start(snap.AppContext())
	defer sampler.Stop()

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
				emitter.PathError(ack.Record.Pathname, ack.Err)
				if ack.Record.FileInfo.IsDir() {
					emitter.DirectoryError(ack.Record.Pathname, ack.Err)
				} else if ack.Record.FileInfo.Mode()&os.ModeSymlink != 0 {
					emitter.SymlinkError(ack.Record.Pathname, ack.Err)
				} else {
					emitter.FileError(ack.Record.Pathname, ack.Err)
				}
			} else if !ack.Record.IsXattr {
				emitter.PathOk(ack.Record.Pathname)
				if ack.Record.FileInfo.IsDir() {
					emitter.DirectoryOk(ack.Record.Pathname, ack.Record.FileInfo)
				} else if ack.Record.FileInfo.Mode()&os.ModeSymlink != 0 {
					emitter.SymlinkOk(ack.Record.Pathname)
				} else {
					emitter.FileOk(ack.Record.Pathname, ack.Record.FileInfo)
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

			// path.Clean will not remove ".." elements if the path is not
			// absolute.  so, let's make it absolute, clean it for good
			// hygiene, then strip prefix and make sure it's still absolute.
			if !strings.HasPrefix(entrypath, "/") {
				entrypath = "/" + entrypath
			}

			entrypath = path.Clean(entrypath)

			// simulate a chroot: strip the parent path from the entry path
			entrypath = strings.TrimPrefix(entrypath, tostrip)
			if !strings.HasPrefix(entrypath, "/") {
				entrypath = "/" + entrypath
			}

			emitter.Path(entrypath)

			if !path.IsAbs(entrypath) {
				emitter.PathError(entrypath, fmt.Errorf("snapshot entry has non-absolute path %q", entrypath))
				return fmt.Errorf("snapshot entry has non-absolute path %q", entrypath)
			}

			isRegular := false
			if e.FileInfo.IsDir() {
				emitter.Directory(entrypath)
			} else if e.FileInfo.Mode()&os.ModeSymlink != 0 {
				emitter.Symlink(entrypath)
			} else if e.FileInfo.Mode().IsRegular() {
				isRegular = true
				emitter.File(entrypath)
			}
			records <- connectors.NewRecord(entrypath, e.SymlinkTarget, e.FileInfo, e.ExtendedAttributes,
				func() (io.ReadCloser, error) {
					f, err := e.Open(pvfs)
					if err != nil {
						return nil, err
					}
					var rd io.ReadCloser = f
					if isRegular {
						rd = newCountingReadCloser(rd, snap.repository.ExportStats.GetWriteSpan())
					}
					return rd, nil
				})
			return nil
		})
		close(records)
	}()

	return exp.Export(snap.AppContext(), records, results)
}
