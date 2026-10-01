package visualizer

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/stretchr/testify/require"
)

type testVisualizer struct{}

func (*testVisualizer) Expose(context.Context, *vfs.Filesystem) error {
	return nil
}

func TestNewVisualizerSelectsProtocol(t *testing.T) {
	const protocol = "visualizer-new-test"

	var gotProtocol string
	var gotLocation string
	require.NoError(t, Register(protocol, 0, func(_ context.Context, _ *connectors.Options, proto string, config map[string]string) (Visualizer, error) {
		gotProtocol = proto
		gotLocation = config["location"]
		return &testVisualizer{}, nil
	}))
	t.Cleanup(func() {
		require.NoError(t, Unregister(protocol))
	})

	instance, err := NewVisualizer(
		kcontext.NewKContext(),
		&connectors.Options{},
		map[string]string{"location": protocol + "://snapshot-id"},
	)
	require.NoError(t, err)
	require.IsType(t, &testVisualizer{}, instance)
	require.Equal(t, protocol, gotProtocol)
	require.Equal(t, protocol+"://snapshot-id", gotLocation)
}

func TestNewVisualizerRejectsUnsupportedProtocol(t *testing.T) {
	_, err := NewVisualizer(
		kcontext.NewKContext(),
		&connectors.Options{},
		map[string]string{"location": "not-registered://snapshot-id"},
	)
	require.EqualError(t, err, "unsupported visualizer protocol")
}
