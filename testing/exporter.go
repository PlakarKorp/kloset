package testing

import (
	"bytes"
	"context"
	"io"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
)

type MockExporter struct {
	rootDir string
	files   map[string][]byte
}

func init() {
	exporter.Register("mock", 0, NewMockExporter)
}

func NewMockExporter(appCtx context.Context, opt *connectors.Options, name string, config map[string]string) (exporter.Exporter, error) {
	rootDir := config["location"]
	if len(rootDir) > 7 && rootDir[:7] == "mock://" {
		rootDir = rootDir[7:]
	}

	return &MockExporter{
		rootDir: rootDir,
		files:   make(map[string][]byte),
	}, nil
}

func (e *MockExporter) Origin() string        { return e.rootDir }
func (e *MockExporter) Type() string          { return e.rootDir }
func (e *MockExporter) Root() string          { return e.rootDir }
func (p *MockExporter) Flags() location.Flags { return 0 }

func (e *MockExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	for record := range records {
		pathname := record.Pathname
		if len(pathname) > 5 && pathname[:5] == "mock:" {
			pathname = pathname[5:]
		}

		if record.Err != nil || !record.FileInfo.Mode().IsRegular() {
			results <- record.Ok()
			continue
		}

		writer := bytes.NewBuffer(nil)
		_, err := io.Copy(writer, record.Reader)
		if err != nil {
			return err
		}
		e.files[pathname] = writer.Bytes()

		results <- record.Ok()
	}

	return nil
}

func (e *MockExporter) Ping(ctx context.Context) error {
	return nil
}

func (e *MockExporter) Close(ctx context.Context) error {
	return nil
}

func (e *MockExporter) Files() map[string][]byte {
	return e.files
}
