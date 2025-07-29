package testing

import (
	"bytes"
	"context"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
)

type MockExporter struct {
	rootDir string
	files   map[string][]byte
}

func init() {
	exporter.Register("mock", 0, NewMockExporter)
}

func NewMockExporter(appCtx context.Context, opt *exporter.Options, name string, config map[string]string) (exporter.Exporter, error) {
	rootDir := config["location"]
	if len(rootDir) > 7 && rootDir[:7] == "mock://" {
		rootDir = rootDir[7:]
	}

	return &MockExporter{
		rootDir: rootDir,
		files:   make(map[string][]byte),
	}, nil
}

func (e *MockExporter) Root() string {
	return e.rootDir
}

func (e *MockExporter) CreateDirectory(ctx context.Context, pathname string) error {
	return nil
}

func (e *MockExporter) StoreFile(ctx context.Context, pathname string, fp io.Reader, size int64) error {
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

func (e *MockExporter) SetPermissions(ctx context.Context, pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (e *MockExporter) Close() error {
	return nil
}

func (e *MockExporter) Files() map[string][]byte {
	return e.files
}
