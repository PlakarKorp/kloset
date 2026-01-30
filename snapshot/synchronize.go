package snapshot

import (
	"context"
	"fmt"
	"io"

	"github.com/PlakarKorp/kloset/connectors"
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

	erriter, err := p.fs.Errors("/")
	if err != nil {
		return err
	}

	_, _, xattrtree := p.fs.BTrees()
	xattriter, err := xattrtree.ScanFrom("/")
	if err != nil {
		return err
	}

	i := 0
	for erritem, err := range erriter {
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
			err = dst.repository.PutBlob(resources.RT_SIGNATURE, newmac, data)
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

	source, err := NewSource(dst.AppContext(), 0, imp)
	if err != nil {
		return err
	}

	if err := dst.Import(source); err != nil {
		return err
	}

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
