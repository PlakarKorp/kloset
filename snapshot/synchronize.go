package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

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

	return dst.ingestSync(imp, &BackupOptions{
		MaxConcurrency:  uint64(src.AppContext().MaxConcurrency),
		CleanupVFSCache: false,
	})
}

func (snap *Builder) ingestSync(imp importer.Importer, options *BackupOptions) error {
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
	backupCtx.flushTick = time.NewTicker(1 * time.Hour)
	go snap.flushDeltaState(backupCtx)

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

	snap.Header.GetSource(0).VFS = *vfsHeader
	snap.Header.GetSource(0).Summary = *rootSummary
	snap.Header.GetSource(0).Indexes = indexes

	return snap.Commit(backupCtx, true)
}
