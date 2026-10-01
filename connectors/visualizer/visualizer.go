package visualizer

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

// Visualizer exposes a snapshot filesystem through an external representation
// or service. Expose may block until ctx is cancelled.
type Visualizer interface {
	Expose(context.Context, *vfs.Filesystem) error
}

type VisualizerFn func(context.Context, *connectors.Options, string, map[string]string) (Visualizer, error)

var backends = location.New[VisualizerFn]("")

func Register(name string, flags location.Flags, backend VisualizerFn) error {
	if !backends.Register(name, backend, flags) {
		return fmt.Errorf("visualizer backend '%s' already registered", name)
	}
	return nil
}

func Unregister(name string) error {
	if !backends.Unregister(name) {
		return fmt.Errorf("visualizer backend '%s' not registered", name)
	}
	return nil
}

func Backends() []string {
	return backends.Names()
}

func NewVisualizer(ctx *kcontext.KContext, opts *connectors.Options, config map[string]string) (Visualizer, error) {
	loc, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, loc, backend, flags, ok := backends.Lookup(loc)
	if !ok {
		return nil, fmt.Errorf("unsupported visualizer protocol")
	}

	if flags&location.FLAG_LOCALFS != 0 && !filepath.IsAbs(loc) {
		loc = filepath.Join(ctx.CWD, loc)
	}
	config["location"] = proto + "://" + loc

	instance, err := backend(ctx, opts, proto, config)
	if err != nil {
		return nil, err
	}
	return instance, nil
}
