package snapshot

import (
	"context"
	"fmt"
	"io"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/iostat"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/google/uuid"
)

// dummy importer
type syncImporter struct {
	root   string
	origin string
	typ    string
	fs     *vfs.Filesystem
	src    *Snapshot
}

func (p *syncImporter) Origin() string        { return p.origin }
func (p *syncImporter) Type() string          { return p.typ }
func (p *syncImporter) Root() string          { return p.root }
func (p *syncImporter) Flags() location.Flags { return 0 }

func (p *syncImporter) Ping(ctx context.Context) error {
	return nil
}

func (p *syncImporter) Close(ctx context.Context) error {
	return nil
}

func (p *syncImporter) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	defer close(records)

	_, _, xattrtree := p.fs.BTrees()
	xattriter, err := xattrtree.ScanFrom("/")
	if err != nil {
		return err
	}

	i := 0
	for erritem, err := range p.fs.Errors("/") {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++
		if err != nil {
			return err
		}
		records <- connectors.NewError(erritem.Name, fmt.Errorf("%s", erritem.Error))
	}

	i = 0
	for xattriter.Next() {
		if i%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		i++

		_, xattrmac := xattriter.Current()
		xattr, err := p.fs.ResolveXattr(xattrmac)
		if err != nil {
			return err
		}
		records <- connectors.NewXattr(xattr.Path, xattr.Name, objects.AttributeExtended,
			func() (io.ReadCloser, error) {
				return io.NopCloser(vfs.NewObjectReader(p.src.repository, xattr.ResolvedObject, xattr.Size, -1)), nil
			})
	}
	if err := xattriter.Err(); err != nil {
		return err
	}

	i = 0
	err = p.fs.WalkDir("/", func(path string, entry *vfs.Entry, err error) error {
		if err != nil {
			return err
		}

		if i%1024 == 0 {
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		i++

		records <- connectors.NewRecord(path, entry.SymlinkTarget, entry.FileInfo, entry.ExtendedAttributes,
			func() (io.ReadCloser, error) {
				return p.fs.Open(path)
			})
		return nil
	})

	return err
}

func (src *Snapshot) Synchronize(dst *Builder) error {
	if src.Header.Identity.Identifier != uuid.Nil {
		data, err := src.repository.GetBlobBytes(resources.RT_SIGNATURE, src.Header.Identifier)
		if err != nil {
			return err
		}

		newmac := dst.repository.ComputeMAC(data)
		dst.Header.Identifier = newmac
		if dst.repository.BlobExists(resources.RT_SIGNATURE, newmac) {
			err = dst.repository.PutBlob(resources.RT_SIGNATURE, newmac, data, true)
			if err != nil {
				return err
			}
		}
	}

	fs, err := src.Filesystem()
	if err != nil {
		return err
	}

	// Do not do the "is store part of the import" detection, we are in a sync,
	// not in a backup.
	dst.noSkipSelf = true
	imp := &syncImporter{
		root: src.Header.GetSource(0).Importer.Directory,
		typ:  src.Header.GetSource(0).Importer.Type,
		//origin: fmt.Sprintf("%s-%s-%x", uuid.NewString(), src.repository.Configuration().RepositoryID, src.Header.Identifier),
		origin: src.Header.GetSource(0).Importer.Origin,
		fs:     fs,
		src:    src,
	}

	// Capture the source totals before the header is aliased onto dst below
	// (dst.Header is a pointer to the same Header, and Sources gets nil'd).
	srcSummary := src.Header.GetSource(0).Summary

	dst.Header = src.Header
	dst.Header.Sources = nil

	dst.Header.Timestamp = src.Header.Timestamp
	dst.Header.Duration = src.Header.Duration
	dst.Header.Name = src.Header.Name
	dst.Header.Category = src.Header.Category
	dst.Header.Environment = src.Header.Environment
	dst.Header.Perimeter = src.Header.Perimeter
	dst.Header.Job = src.Header.Job
	dst.Header.Replicas = src.Header.Replicas
	dst.Header.Classifications = src.Header.Classifications
	dst.Header.Tags = src.Header.Tags
	dst.Header.Context = src.Header.Context

	source, err := NewSource(dst.AppContext(), imp)
	if err != nil {
		return err
	}

	// Import runs the backup code path and emits its events (file/chunk/…)
	// through dst.emitter, which is labelled "backup" by default. Relabel it
	// "synchronize" so consumers see the operation for what it is; the source
	// sampler below shares the same emitter so all sync events line up.
	syncEmitter := dst.Emitter("synchronize")
	dst.emitter = syncEmitter
	defer syncEmitter.Close()

	// The total is known up front from the source snapshot's header (captured
	// above), so emit a filesystem summary (as backup/export do) to drive the
	// progress bar and the tree/items counts.
	dst.emitter.FilesystemSummary(
		srcSummary.Directory.Files+srcSummary.Below.Files,
		srcSummary.Directory.Directories+srcSummary.Below.Directories,
		srcSummary.Directory.Symlinks+srcSummary.Below.Symlinks,
		0,
		srcSummary.Directory.Size+srcSummary.Below.Size,
	)

	// dst.Import samples the destination side; the source repository's reads
	// live on a separate tracker, so sample those here to cover both stores.
	// Reset both per snapshot so the two sides stay in step — each snapshot is
	// its own workflow (and its own progress view) — otherwise the source read
	// restarts from zero while the destination write keeps accumulating.
	src.repository.IOStats().Reset()
	dst.repository.IOStats().Reset()
	srcSampler := iostat.NewSampler(syncEmitter, dst.AppContext().IOStatsInterval,
		iostat.ScopedTracker{Name: "source-storage", T: src.repository.IOStats()},
	)
	srcSampler.Start(dst.AppContext())

	if err := dst.Import(source); err != nil {
		srcSampler.Stop()
		return err
	}
	srcSampler.Stop()

	srcErrors := imp.src.Header.GetSource(0).Summary.Directory.Errors + imp.src.Header.GetSource(0).Summary.Below.Errors
	nErrors := dst.Header.GetSource(0).Summary.Directory.Errors + dst.Header.GetSource(0).Summary.Below.Errors
	if nErrors != srcErrors {
		dst.repository.PackerManager.Wait()
		return fmt.Errorf("synchronization failed: source errors %d, destination errors %d", srcErrors, nErrors)
	}

	if !dst.builderOptions.NoCommit {
		return dst.Commit()
	} else {
		_, err := dst.PutSnapshot()
		return err
	}
}
