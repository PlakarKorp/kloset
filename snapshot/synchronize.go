package snapshot

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/google/uuid"
)

// dummy importer
type syncImporter struct {
	root    string
	origin  string
	typ     string
	fs      *vfs.Filesystem
	src     *Snapshot
	failure error
}

func (p *syncImporter) Origin(ctx context.Context) (string, error) {
	return p.origin, nil
}

func (p *syncImporter) Type(ctx context.Context) (string, error) {
	return p.typ, nil
}

func (p *syncImporter) Root(ctx context.Context) (string, error) {
	return p.root, nil
}

func (p *syncImporter) Close(ctx context.Context) error {
	return nil
}

func (p *syncImporter) Scan(ctx context.Context) (<-chan *importer.ScanResult, error) {

	erriter, err := p.fs.Errors("/")
	if err != nil {
		return nil, err
	}

	_, _, xattrtree := p.fs.BTrees()
	xattriter, err := xattrtree.ScanFrom("/")
	if err != nil {
		return nil, err
	}

	results := make(chan *importer.ScanResult, 1000)

	go func() {
		defer close(results)

		i := 0
		for erritem, err := range erriter {
			if i%1024 == 0 {
				if ctx.Err() != nil {
					p.failure = ctx.Err()
					return
				}
			}
			i++
			if err != nil {
				p.failure = err
				return
			}
			results <- importer.NewScanError(erritem.Name, fmt.Errorf("%s", erritem.Error))
		}

		i = 0
		for xattriter.Next() {
			if i%1024 == 0 {
				if ctx.Err() != nil {
					p.failure = ctx.Err()
					return
				}
			}
			i++

			_, xattrmac := xattriter.Current()
			xattr, err := p.fs.ResolveXattr(xattrmac)
			if err != nil {
				p.failure = err
				return
			}
			results <- importer.NewScanXattr(xattr.Path, xattr.Name, objects.AttributeExtended,
				func() (io.ReadCloser, error) {
					return io.NopCloser(vfs.NewObjectReader(p.src.repository, xattr.ResolvedObject, xattr.Size)), nil
				})
		}
		if err := xattriter.Err(); err != nil {
			p.failure = err
			return
		}

		i = 0
		if err := p.fs.WalkDir("/", func(path string, entry *vfs.Entry, err error) error {
			if err != nil {
				return err
			}

			if i%1024 == 0 {
				if ctx.Err() != nil {
					return ctx.Err()
				}
			}
			i++

			results <- importer.NewScanRecord(path, entry.SymlinkTarget, entry.FileInfo, entry.ExtendedAttributes,
				func() (io.ReadCloser, error) {
					return p.fs.Open(path)
				})
			return nil
		}); err != nil {
			p.failure = err
		}
	}()

	return results, nil
}

func (src *Snapshot) Synchronize(dst *Builder, commit bool) error {
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

	imp := &syncImporter{
		root:   "/",
		typ:    "sync",
		origin: fmt.Sprintf("%s:%s:%x", uuid.NewString(), src.repository.Configuration().RepositoryID, src.Header.Identifier),
		fs:     fs,
		src:    src,
	}

	dst.Header.GetSource(0).Importer.Directory = src.Header.GetSource(0).Importer.Directory
	dst.Header.GetSource(0).Importer.Origin = src.Header.GetSource(0).Importer.Origin
	dst.Header.GetSource(0).Importer.Type = src.Header.GetSource(0).Importer.Type
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

	return dst.ingestSync(imp, &BackupOptions{
		MaxConcurrency:  uint64(src.AppContext().MaxConcurrency),
		CleanupVFSCache: true,
	}, commit)
}

func (snap *Builder) ingestSync(imp *syncImporter, options *BackupOptions, commit bool) error {
	done, err := snap.Lock()
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}
	defer snap.Unlock(done)

	backupCtx, err := snap.prepareBackup(imp, options)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* checkpoint handling */
	if !options.NoCheckpoint {
		backupCtx.flushTick = time.NewTicker(1 * time.Hour)
		go snap.flushDeltaState(backupCtx)
	}

	/* importer */
	if err := snap.importerJob(backupCtx); err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* tree builders */
	vfsHeader, rootSummary, indexes, err := snap.persistTrees(backupCtx)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	if imp.failure != nil {
		snap.repository.PackerManager.Wait()
		return imp.failure
	}

	snap.Header.GetSource(0).VFS = *vfsHeader
	snap.Header.GetSource(0).Summary = *rootSummary
	snap.Header.GetSource(0).Indexes = indexes

	srcErrors := imp.src.Header.GetSource(0).Summary.Directory.Errors + imp.src.Header.GetSource(0).Summary.Below.Errors
	nErrors := snap.Header.GetSource(0).Summary.Directory.Errors + snap.Header.GetSource(0).Summary.Below.Errors
	if nErrors != srcErrors {
		snap.repository.PackerManager.Wait()
		return fmt.Errorf("synchronization failed: source errors %d, destination errors %d", srcErrors, nErrors)
	}

	return snap.Commit(backupCtx, commit)
}
