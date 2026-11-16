package exporter

import (
	"fmt"
	"path/filepath"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/integration-sdk/location"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/kcontext"
)

type (
	Exporter   = integration.Exporter
	ExporterFn = integration.ExporterFn
)

func Register(name string, flags location.Flags, backend ExporterFn) error {
	return integration.RegisterExporter(name, flags, backend)
}

func Unregister(name string) error {
	return integration.UnregisterExporter(name)

}

func Backends() []string {
	return integration.ExporterBackends()
}

func NewExporter(ctx *kcontext.KContext, opts *connectors.Options, config map[string]string) (Exporter, error) {
	loc, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, loc, backend, flags, ok := integration.LookupExporter(loc)
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
