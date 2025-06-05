package testing

import (
	"bytes"
	"context"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

type MockExporter struct {
	rootDir string
	files   map[string][]byte
}

func init() {
	exporter.Register("mock", NewMockExporter)
}

func NewMockExporter(appCtx context.Context, name string, config map[string]string) (exporter.Exporter, error) {
	rootDir := config["location"]
	if len(rootDir) > 7 && rootDir[:7] == "mock://" {
		rootDir = rootDir[7:]
	}

	return &MockExporter{
		rootDir: rootDir,
		files:   make(map[string][]byte),
	}, nil
}

func (e *MockExporter) Export(ctx context.Context, opts *exporter.ExporterOptions, base string, vfs *vfs.Filesystem) error {
	return exporter.Export(ctx, base, e, opts, vfs)
}

func (e *MockExporter) Root() string {
	return e.rootDir
}

func (e *MockExporter) CreateDirectory(pathname string) error {
	return nil
}

func (e *MockExporter) StoreFile(pathname string, fp io.Reader, size int64) error {
	if len(pathname) > 5 && pathname[:5] == "mock:" {
		pathname = pathname[5:]
	}
	writer := bytes.NewBuffer(nil)
	_, err := io.Copy(writer, fp)
	if err != nil {
		return err
	}
	e.files[pathname] = writer.Bytes()
	return nil
}

func (e *MockExporter) SetPermissions(pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (e *MockExporter) Close() error {
	return nil
}

func (e *MockExporter) Files() map[string][]byte {
	return e.files
}
