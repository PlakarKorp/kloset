package exporter

import (
	"context"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
)

type Exporter interface {
	Root() string
	CreateDirectory(pathname string) error
	StoreFile(pathname string, fp io.Reader, size int64) error
	SetPermissions(pathname string, fileinfo *objects.FileInfo) error
	Close() error
}

type ExporterOptions interface {
}

type ExporterFn func(context.Context, *ExporterOptions, string, map[string]string) (Exporter, error)

var backends = location.New[ExporterFn]("fs")

func Register(name string, backend ExporterFn) {
	if !backends.Register(name, backend) {
		log.Fatalf("backend '%s' registered twice", name)
	}
}

func Backends() []string {
	return backends.Names()
}

func NewExporter(ctx *kcontext.KContext, opts *ExporterOptions, config map[string]string) (Exporter, error) {
	location, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, location, backend, ok := backends.Lookup(location)
	if !ok {
		return nil, fmt.Errorf("unsupported exporter protocol")
	}

	if proto == "fs" && !filepath.IsAbs(location) {
		location = filepath.Join(ctx.CWD, location)
		config["location"] = "fs://" + location
	} else {
		config["location"] = proto + "://" + location
	}

	return backend(ctx, opts, proto, config)
}
