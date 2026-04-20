package exporter_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	xport "github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/stretchr/testify/require"
)

type MockedBackend struct{}

func (m MockedBackend) Origin() string              { return "" }
func (m MockedBackend) Type() string                { return "" }
func (m MockedBackend) Root() string                { return "" }
func (m MockedBackend) Flags() location.Flags       { return 0 }
func (m MockedBackend) Ping(context.Context) error  { return nil }
func (m MockedBackend) Close(context.Context) error { return nil }

func (m MockedBackend) Export(
	ctx context.Context,
	records <-chan *connectors.Record,
	results chan<- *connectors.Result,
) error {
	close(results)
	return nil
}

func TestRegister(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		opts *connectors.Options,
		proto string,
		config map[string]string,
	) (xport.Exporter, error) {
		return nil, nil
	}

	t.Run("ValidBackendRegistration", func(t *testing.T) {
		backendName := "test-register-backend"

		err := xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		t.Cleanup(func() { _ = xport.Unregister(backendName) })
		require.NoError(t, err)
	})

	t.Run("Fails_IfBackendAlreadyRegistered", func(t *testing.T) {
		backendName := "test-register-duplicate"

		err := xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() { _ = xport.Unregister(backendName) })

		err = xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.EqualError(t, err, "exporter backend 'test-register-duplicate' already registered")
	})
}

func TestUnregister(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		opts *connectors.Options,
		proto string,
		config map[string]string,
	) (xport.Exporter, error) {
		return nil, nil
	}

	t.Run("ValidBackendUnregistration", func(t *testing.T) {
		backendName := "test-exporter-unregister-backend"

		err := xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = xport.Unregister(backendName)
		require.NoError(t, err)
	})

	t.Run("Fails_IfBackendIsNotRegistered", func(t *testing.T) {
		backendName := "test-exporter-unregister-missing"

		err := xport.Unregister(backendName)
		require.EqualError(t, err, "exporter backend 'test-exporter-unregister-missing' not registered")
	})

	t.Run("Register->Unregister->Register", func(t *testing.T) {
		backendName := "test-exporter-unregister-reregister"

		err := xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = xport.Unregister(backendName)
		require.NoError(t, err)

		err = xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() { _ = xport.Unregister(backendName) })
	})
}

func TestBackends(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		opts *connectors.Options,
		proto string,
		config map[string]string,
	) (xport.Exporter, error) {
		return nil, nil
	}

	t.Run("GetRegisteredBackends", func(t *testing.T) {
		backendName1 := "test-exporter-backends-first"
		backendName2 := "test-exporter-backends-second"

		err := xport.Register(backendName1, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = xport.Register(backendName2, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = xport.Unregister(backendName1)
			_ = xport.Unregister(backendName2)
		})

		backends := xport.Backends()
		require.Contains(t, backends, backendName1)
		require.Contains(t, backends, backendName2)
	})

	t.Run("DoNotGetUnregisteredBackends", func(t *testing.T) {
		backendName := "test-exporter-backends-removed"

		err := xport.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = xport.Unregister(backendName)
		require.NoError(t, err)

		backends := xport.Backends()
		require.NotContains(t, backends, backendName)
	})
}

func TestNewExporter(t *testing.T) {
	registerBackend := func(
		t *testing.T,
		name string,
		flags location.Flags,
		backendFn xport.ExporterFn,
	) {
		t.Helper()

		err := xport.Register(name, flags, backendFn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = xport.Unregister(name) })
	}

	t.Run("FailsIfLocationIsMissing", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		exporter, err := xport.NewExporter(appCtx, nil, map[string]string{})
		require.Nil(t, exporter)
		require.EqualError(t, err, "missing location")
	})

	t.Run("FailsIfProtocolIsUnsupported", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		exporter, err := xport.NewExporter(appCtx, nil, map[string]string{
			"location": "unsupported://some/path",
		})
		require.Nil(t, exporter)
		require.EqualError(t, err, "unsupported exporter protocol")
	})

	t.Run("UseDefaultFSProtocolForRelativeLocalPath", func(t *testing.T) {
		var (
			called           bool
			receivedCtx      context.Context
			receivedOpts     *connectors.Options
			receivedProto    string
			receivedLocation string
		)

		backend := func(
			ctx context.Context,
			opts *connectors.Options,
			proto string,
			config map[string]string,
		) (xport.Exporter, error) {
			called = true
			receivedCtx = ctx
			receivedOpts = opts
			receivedProto = proto
			receivedLocation = config["location"]
			return MockedBackend{}, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, backend)
		appCtx := kcontext.NewKContext()
		appCtx.CWD = t.TempDir()
		opts := &connectors.Options{}
		config := map[string]string{
			"location": "relative/path",
		}

		exporter, err := xport.NewExporter(appCtx, opts, config)
		require.NoError(t, err)
		require.NotNil(t, exporter)
		require.True(t, called)
		require.Same(t, appCtx, receivedCtx)
		require.Same(t, opts, receivedOpts)
		require.Equal(t, "fs", receivedProto)

		expectedLocation := "fs://" + filepath.Join(appCtx.CWD, "relative/path")
		require.Equal(t, expectedLocation, receivedLocation)
		require.Equal(t, expectedLocation, config["location"])
	})

	t.Run("KeepAbsoluteLocalPathUnchanged", func(t *testing.T) {
		var receivedLocation string

		backend := func(
			ctx context.Context,
			opts *connectors.Options,
			proto string,
			config map[string]string,
		) (xport.Exporter, error) {
			receivedLocation = config["location"]
			return MockedBackend{}, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, backend)
		appCtx := kcontext.NewKContext()
		absolutePath := filepath.Join(t.TempDir(), "some", "path")
		config := map[string]string{
			"location": "fs://" + absolutePath,
		}

		exporter, err := xport.NewExporter(appCtx, nil, config)
		require.NoError(t, err)
		require.NotNil(t, exporter)
		require.Equal(t, "fs://"+absolutePath, receivedLocation)
		require.Equal(t, "fs://"+absolutePath, config["location"])
	})

	t.Run("DoNotRewriteNonLocalLocation", func(t *testing.T) {
		var (
			receivedProto    string
			receivedLocation string
		)

		backend := func(
			ctx context.Context,
			opts *connectors.Options,
			proto string,
			config map[string]string,
		) (xport.Exporter, error) {
			receivedProto = proto
			receivedLocation = config["location"]
			return MockedBackend{}, nil
		}

		registerBackend(t, "s3", 0, backend)
		appCtx := kcontext.NewKContext()
		config := map[string]string{
			"location": "s3://bucket/path",
		}

		exporter, err := xport.NewExporter(appCtx, nil, config)
		require.NoError(t, err)
		require.NotNil(t, exporter)
		require.Equal(t, "s3", receivedProto)
		require.Equal(t, "s3://bucket/path", receivedLocation)
		require.Equal(t, "s3://bucket/path", config["location"])
	})

	t.Run("PropagateBackendError", func(t *testing.T) {
		expectedErr := errors.New("backend failure")

		backend := func(
			ctx context.Context,
			opts *connectors.Options,
			proto string,
			config map[string]string,
		) (xport.Exporter, error) {
			return nil, expectedErr
		}

		registerBackend(t, "ftp", 0, backend)
		appCtx := kcontext.NewKContext()
		config := map[string]string{
			"location": "ftp://some/path",
		}

		exporter, err := xport.NewExporter(appCtx, nil, config)
		require.Nil(t, exporter)
		require.ErrorIs(t, err, expectedErr)
	})
}
