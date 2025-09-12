package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/exclude"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/google/uuid"
	"github.com/pkg/xattr"
)

// dummy importer
type syncImporter struct {
	root   string
	origin string
	typ    string
	walk   func(ctx context.Context, results chan<- *importer.ScanResult)
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
	results := make(chan *importer.ScanResult, 1000)
	go p.walk(ctx, results)
	return results, nil
}

type syncOptions struct {
	MaxConcurrency  uint64
	CleanupVFSCache bool
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

	erriter, err := fs.Errors("/")
	if err != nil {
		return err
	}

	walk := func(ctx context.Context, results chan<- *importer.ScanResult) {
		for erritem, err := range erriter {
			if err != nil {
				results <- importer.NewScanError("/", err)
				continue
			}
			results <- importer.NewScanError(erritem.Name, fmt.Errorf("%s", erritem.Error))
		}

		err = fs.WalkDir("/", func(path string, entry *vfs.Entry, err error) error {
			var originFile string
			if entry.FileInfo.Mode()&os.ModeSymlink != 0 {
				originFile, err = os.Readlink(path)
				if err != nil {
					results <- importer.NewScanError(path, err)
					return nil
				}
			}

			fileinfo := entry.FileInfo
			extendedAttributes := entry.ExtendedAttributes

			results <- importer.NewScanRecord(path, originFile, fileinfo, extendedAttributes,
				func() (io.ReadCloser, error) {
					return os.Open(path)
				})
			for _, attr := range extendedAttributes {
				results <- importer.NewScanXattr(path, attr, objects.AttributeExtended,
					func() (io.ReadCloser, error) {
						data, err := xattr.Get(path, attr)
						if err != nil {
							return nil, err
						}
						return io.NopCloser(bytes.NewReader(data)), nil
					})
			}
			return nil
		})
		if err != nil {
			results <- importer.NewScanError("/", err)
		}
		close(results)
	}

	imp, err := func() (importer.Importer, error) {
		return &syncImporter{
			root:   src.Header.GetSource(0).Importer.Directory,
			typ:    src.Header.GetSource(0).Importer.Type,
			origin: src.Header.GetSource(0).Importer.Origin,
			walk:   walk,
		}, nil
	}()
	if err != nil {
		return err
	}

	dst.Header.GetSource(0).Importer.Directory = src.Header.GetSource(0).Importer.Directory
	dst.Header.GetSource(0).Importer.Origin = src.Header.GetSource(0).Importer.Origin
	dst.Header.GetSource(0).Importer.Type = src.Header.GetSource(0).Importer.Type
	dst.Header.Tags = src.Header.Tags
	dst.Header.Name = src.Header.Name
	dst.Header.Timestamp = src.Header.Timestamp
	dst.Header.Duration = src.Header.Duration

	return dst.ingestSync(imp, &syncOptions{
		MaxConcurrency:  uint64(src.AppContext().MaxConcurrency),
		CleanupVFSCache: false,
	})
}

func (snap *Builder) prepareSync(imp importer.Importer, options *syncOptions) (*BackupContext, error) {
	maxConcurrency := options.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = uint64(snap.AppContext().MaxConcurrency)
	}

	typ, err := imp.Type(snap.AppContext())
	if err != nil {
		return nil, err
	}

	origin, err := imp.Origin(snap.AppContext())
	if err != nil {
		return nil, err
	}

	vfsCache, err := snap.AppContext().GetCache().VFS(snap.repository.Configuration().RepositoryID, typ, origin, options.CleanupVFSCache)
	if err != nil {
		return nil, err
	}

	syncCtx := &BackupContext{
		imp:            imp,
		maxConcurrency: maxConcurrency,
		scanCache:      snap.scanCache,
		vfsCache:       vfsCache,
		flushEnd:       make(chan bool),
		flushEnded:     make(chan bool),
		stateId:        snap.Header.Identifier,
	}
	syncCtx.excludes = exclude.NewRuleSet()

	if bi, err := snap.makeBackupIndexes(); err != nil {
		return nil, err
	} else {
		syncCtx.indexes = append(syncCtx.indexes, bi)
	}

	return syncCtx, nil
}

func (snap *Builder) ingestSync(imp importer.Importer, options *syncOptions) error {
	done, err := snap.Lock()
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}
	defer snap.Unlock(done)

	syncCtx, err := snap.prepareSync(imp, options)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* checkpoint handling */
	syncCtx.flushTick = time.NewTicker(1 * time.Hour)
	go snap.flushDeltaState(syncCtx)

	/* importer */
	if err := snap.importerJob(syncCtx); err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	/* tree builders */
	vfsHeader, rootSummary, indexes, err := snap.persistTrees(syncCtx)
	if err != nil {
		snap.repository.PackerManager.Wait()
		return err
	}

	snap.Header.GetSource(0).VFS = *vfsHeader
	snap.Header.GetSource(0).Summary = *rootSummary
	snap.Header.GetSource(0).Indexes = indexes

	return snap.Commit(syncCtx, true)
}
