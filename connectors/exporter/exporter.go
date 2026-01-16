package exporter

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
)

type Exporter interface {
	Origin() string
	Type() string
	Root() string
	Flags() location.Flags
	Ping(context.Context) error
	Export(context.Context, <-chan *connectors.Record, chan<- *connectors.Result) error
	Close(context.Context) error
}

type ExporterFn func(context.Context, *connectors.Options, string, map[string]string) (Exporter, error)

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

func NewExporter(ctx *kcontext.KContext, opts *connectors.Options, config map[string]string) (Exporter, error) {
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

	exporter, err := backend(ctx, opts, proto, config)
	if err != nil {
		return nil, err
	}
	return exporter, nil
}
