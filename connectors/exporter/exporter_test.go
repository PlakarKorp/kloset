package exporter_test

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	xport "github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"github.com/stretchr/testify/require"
)

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
