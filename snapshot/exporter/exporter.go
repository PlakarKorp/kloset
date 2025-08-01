package exporter

import (
	"context"
	"fmt"
	"io"
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

type LinkType int

const (
	HARDLINK LinkType = iota
	SYMLINK
)

type Exporter interface {
	Root(ctx context.Context) (string, error)
	CreateDirectory(ctx context.Context, pathname string) error
	CreateLink(ctx context.Context, oldname string, newname string, ltype LinkType) error
	StoreFile(ctx context.Context, pathname string, fp io.Reader, size int64) error
	SetPermissions(ctx context.Context, pathname string, fileinfo *objects.FileInfo) error
	Close(ctx context.Context) error
}

type ExporterFn func(context.Context, *Options, string, map[string]string) (Exporter, error)

var backends = location.New[ExporterFn]("fs")

func Register(name string, flags location.Flags, backend ExporterFn) error {
	if !backends.Register(name, backend, flags) {
		return fmt.Errorf("exporter backend '%s' already registered", name)
	}
	return nil
}

func Unregister(name string) error {
	if !backends.Unregister(name) {
		return fmt.Errorf("exporter backend '%s' not registered", name)
	}
	return nil
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
