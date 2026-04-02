package importer_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	con "github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

type MockedImporter struct{}

func (m MockedImporter) Origin() string        { return "" }
func (m MockedImporter) Type() string          { return "" }
func (m MockedImporter) Root() string          { return "" }
func (m MockedImporter) Flags() location.Flags { return 0 }

func (m MockedImporter) Ping(context.Context) error {
	return nil
}

func (m MockedImporter) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	close(records)
	return nil
}

func (m MockedImporter) NewReader(string) (io.ReadCloser, error) {
	return nil, nil
}

func (m MockedImporter) NewExtendedAttributeReader(string, string) (io.ReadCloser, error) {
	return nil, nil
}

func (m MockedImporter) Close(ctx context.Context) error {
	return nil
}

func TestRegister(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		opts *connectors.Options,
		proto string,
		config map[string]string,
	) (con.Importer, error) {
		return nil, nil
	}

	t.Run("ValidBackendRegistration", func(t *testing.T) {
		backendName := "test-register-backend"

		err := con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		t.Cleanup(func() { _ = con.Unregister(backendName) })

		require.NoError(t, err)
	})

	t.Run("FailsIfBackendAlreadyRegistered", func(t *testing.T) {
		backendName := "test-register-duplicate"

		err := con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() { _ = con.Unregister(backendName) })

		err = con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.EqualError(t, err, "importer backend 'test-register-duplicate' already registered")
	})
}

func TestUnregister(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		opts *connectors.Options,
		proto string,
		config map[string]string,
	) (con.Importer, error) {
		return nil, nil
	}

	t.Run("ValidBackendUnregistration", func(t *testing.T) {
		backendName := "test-unregister-backend"

		err := con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = con.Unregister(backendName)
		require.NoError(t, err)
	})

	t.Run("FailsIfBackendIsNotRegistered", func(t *testing.T) {
		backendName := "test-unregister-missing"

		err := con.Unregister(backendName)
		require.EqualError(t, err, "importer backend 'test-unregister-missing' not registered")
	})

	t.Run("Register->Unregister->Register", func(t *testing.T) {
		backendName := "test-unregister-backend"

		err := con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = con.Unregister(backendName)
		require.NoError(t, err)

		err = con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() { _ = con.Unregister(backendName) })
	})
}

func TestBackends(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		opts *connectors.Options,
		proto string,
		config map[string]string,
	) (con.Importer, error) {
		return nil, nil
	}

	t.Run("GetRegisteredBackends", func(t *testing.T) {
		backendName1 := "test-backends-first"
		backendName2 := "test-backends-second"

		err := con.Register(backendName1, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = con.Register(backendName2, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = con.Unregister(backendName1)
			_ = con.Unregister(backendName2)
		})

		backends := con.Backends()
		require.Contains(t, backends, backendName1)
		require.Contains(t, backends, backendName2)
	})

	t.Run("DoNotGetUnregisteredBackends", func(t *testing.T) {
		backendName := "test-backends-removed"

		err := con.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = con.Unregister(backendName)
		require.NoError(t, err)

		backends := con.Backends()
		require.NotContains(t, backends, backendName)
	})
}

func TestNewImporter(t *testing.T) {
	registerBackend := func(
		t *testing.T,
		name string,
		flags location.Flags,
		backendFn con.ImporterFn,
	) {
		t.Helper()

		err := con.Register(name, flags, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() { _ = con.Unregister(name) })
	}

	t.Run("FailsIfLocationIsMissing", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		importer, err := con.NewImporter(appCtx, nil, map[string]string{})
		require.Nil(t, importer)
		require.EqualError(t, err, "missing location")
	})

	t.Run("FailsIfProtocolIsUnsupported", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		importer, err := con.NewImporter(appCtx, nil, map[string]string{
			"location": "unsupported://some/path",
		})
		require.Nil(t, importer)
		require.EqualError(t, err, "unsupported importer protocol")
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
		) (con.Importer, error) {
			called = true
			receivedCtx = ctx
			receivedOpts = opts
			receivedProto = proto
			receivedLocation = config["location"]
			return MockedImporter{}, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, backend)

		appCtx := kcontext.NewKContext()
		appCtx.CWD = t.TempDir()

		opts := &connectors.Options{}
		config := map[string]string{
			"location": "relative/path",
		}

		importer, err := con.NewImporter(appCtx, opts, config)
		require.NoError(t, err)
		require.NotNil(t, importer)
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
		) (con.Importer, error) {
			receivedLocation = config["location"]
			return MockedImporter{}, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, backend)

		appCtx := kcontext.NewKContext()
		absolutePath := filepath.Join(t.TempDir(), "some", "path")
		config := map[string]string{
			"location": "fs://" + absolutePath,
		}

		importer, err := con.NewImporter(appCtx, nil, config)
		require.NoError(t, err)
		require.NotNil(t, importer)
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
		) (con.Importer, error) {
			receivedProto = proto
			receivedLocation = config["location"]
			return MockedImporter{}, nil
		}

		registerBackend(t, "s3", 0, backend)

		appCtx := kcontext.NewKContext()
		config := map[string]string{
			"location": "s3://bucket/path",
		}

		importer, err := con.NewImporter(appCtx, nil, config)
		require.NoError(t, err)
		require.NotNil(t, importer)
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
		) (con.Importer, error) {
			return nil, expectedErr
		}
		registerBackend(t, "ftp", 0, backend)

		appCtx := kcontext.NewKContext()
		config := map[string]string{
			"location": "ftp://some/path",
		}

		importer, err := con.NewImporter(appCtx, nil, config)
		require.Nil(t, importer)
		require.ErrorIs(t, err, expectedErr)
	})
}

func TestNewScanRecord(t *testing.T) {
	pathname := "/path/to/file"
	target := "target"
	now := time.Now().Local()

	fileinfo := objects.NewFileInfo("file", 300000, 0644, now, 1, 2, 3, 4, 5)
	xattr := []string{"attr1", "attr2"}

	record := connectors.NewRecord(pathname, target, fileinfo, xattr, nil)

	require.Equal(t, pathname, record.Pathname)
	require.Equal(t, target, record.Target)
	require.Equal(t, fileinfo, record.FileInfo)
	require.ElementsMatch(t, xattr, record.ExtendedAttributes)
}

func TestNewScanXattr(t *testing.T) {
	pathname := "/path/to/file"
	xattrname := "foo/bar"

	record := connectors.NewXattr(pathname, xattrname, objects.AttributeExtended, nil)

	require.Equal(t, pathname, record.Pathname)
	require.Equal(t, xattrname, record.XattrName)
	require.True(t, record.IsXattr)
}

func TestNewScanError(t *testing.T) {
	pathname := "/path/to/file"
	err := fmt.Errorf("some error")

	record := connectors.NewError(pathname, err)

	require.Equal(t, pathname, record.Pathname)
}
