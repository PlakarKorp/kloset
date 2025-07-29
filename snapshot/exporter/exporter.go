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

type Options struct {
	MaxConcurrency uint64

	Stdout io.Writer
	Stderr io.Writer
}

type Exporter interface {
	Root() string
	CreateDirectory(ctx context.Context, pathname string) error
	StoreFile(ctx context.Context, pathname string, fp io.Reader, size int64) error
	SetPermissions(ctx context.Context, pathname string, fileinfo *objects.FileInfo) error
	Close() error
}

type ExporterFn func(context.Context, *Options, string, map[string]string) (Exporter, error)

var backends = location.New[ExporterFn]("fs")

func Register(name string, flags location.Flags, backend ExporterFn) {
	if !backends.Register(name, backend, flags) {
		log.Fatalf("backend '%s' registered twice", name)
	}
}

func Backends() []string {
	return backends.Names()
}

func NewExporter(ctx *kcontext.KContext, config map[string]string) (Exporter, error) {
	loc, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, loc, backend, flags, ok := backends.Lookup(loc)
	if !ok {
		return nil, fmt.Errorf("unsupported exporter protocol")
	}

	if flags&location.FLAG_LOCALFS != 0 && !filepath.IsAbs(loc) {
		loc = filepath.Join(ctx.CWD, loc)
	}
	config["location"] = proto + "://" + loc

	opts := &Options{
		MaxConcurrency: uint64(ctx.MaxConcurrency),
		Stdout:         ctx.Stdout,
		Stderr:         ctx.Stderr,
	}

	return backend(ctx, opts, proto, config)
}
