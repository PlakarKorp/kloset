package testing

import (
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/PlakarKorp/kloset/appcontext"
	"github.com/PlakarKorp/kloset/snapshot/importer"
)

type MockImporter struct {
	location string
	files    map[string]MockFile

	gen  func(chan<- *importer.ScanResult)
	open func(string) (io.ReadCloser, error)
}

func init() {
	importer.Register("mock", NewMockImporter)
}

func NewMockImporter(appCtx *appcontext.AppContext, name string, config map[string]string) (importer.Importer, error) {
	return &MockImporter{
		location: config["location"],
	}, nil

}

func (p *MockImporter) SetFiles(files []MockFile) {
	p.files = make(map[string]MockFile)
	for _, file := range files {
		if !strings.HasPrefix(file.Path, "/") {
			file.Path = "/" + file.Path
		}

		// create all the leading directories
		parts := strings.Split(file.Path, "/")
		for i := range parts {
			comp := strings.Join(parts[:i], "/")
			if comp == "" {
				comp = "/"
			}
			if _, ok := p.files[comp]; !ok {
				p.files[comp] = NewMockDir(comp)
			}
		}

		p.files[file.Path] = file
	}
}

func (p *MockImporter) SetGenerator(gen func(chan<- *importer.ScanResult), open func(string) (io.ReadCloser, error)) {
	p.gen = gen
	p.open = open
}

func (p *MockImporter) Origin() string {
	return "mock"
}

func (p *MockImporter) Type() string {
	return "mock"
}

func (p *MockImporter) Scan() (<-chan *importer.ScanResult, error) {
	ch := make(chan *importer.ScanResult)
	if p.gen != nil {
		go p.gen(ch)
	} else {
		go func() {
			for _, file := range p.files {
				ch <- file.ScanResult()
			}
			close(ch)
		}()
	}
	return ch, nil
}

func (p *MockImporter) NewReader(pathname string) (io.ReadCloser, error) {
	if p.open != nil {
		return p.open(pathname)
	}

	file, ok := p.files[pathname]
	if !ok {
		return nil, os.ErrNotExist
	}

	if file.IsDir {
		return nil, os.ErrInvalid
	}

	if file.Mode&0400 == 0 {
		return nil, os.ErrPermission
	}

	return io.NopCloser(bytes.NewReader(file.Content)), nil
}

func (p *MockImporter) NewExtendedAttributeReader(pathname, attr string) (io.ReadCloser, error) {
	panic("should not be called")
}

func (p *MockImporter) Close() error {
	return nil
}

func (p *MockImporter) Root() string {
	return "/"
}
