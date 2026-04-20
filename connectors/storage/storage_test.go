package storage_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/chunking"
	"github.com/PlakarKorp/kloset/compression"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/encryption"
	"github.com/PlakarKorp/kloset/hashing"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

type mockStore struct {
	openData []byte
	openErr  error

	createCalled bool
	createInput  []byte
	createErr    error
}

func (m *mockStore) Create(_ context.Context, data []byte) error {
	m.createCalled = true
	m.createInput = append([]byte(nil), data...)
	return m.createErr
}

func (m *mockStore) Open(context.Context) ([]byte, error) {
	return m.openData, m.openErr
}

func (m *mockStore) Ping(context.Context) error                 { return nil }
func (m *mockStore) Origin() string                             { return "" }
func (m *mockStore) Type() string                               { return "" }
func (m *mockStore) Root() string                               { return "" }
func (m *mockStore) Flags() location.Flags                      { return 0 }
func (m *mockStore) Mode(context.Context) (storage.Mode, error) { return 0, nil }
func (m *mockStore) Size(context.Context) (int64, error)        { return 0, nil }

func (m *mockStore) List(context.Context, storage.StorageResource) ([]objects.MAC, error) {
	return nil, nil
}

func (m *mockStore) Put(context.Context, storage.StorageResource, objects.MAC, io.Reader) (int64, error) {
	return 0, nil
}

func (m *mockStore) Get(context.Context, storage.StorageResource, objects.MAC, *storage.Range) (io.ReadCloser, error) {
	return nil, nil
}

func (m *mockStore) Delete(context.Context, storage.StorageResource, objects.MAC) error {
	return nil
}

func (m *mockStore) Close(context.Context) error {
	return nil
}

func newMockBackend(store storage.Store, err error) storage.StoreFn {
	return func(context.Context, string, map[string]string) (storage.Store, error) {
		return store, err
	}
}

func TestStorageResourceString(t *testing.T) {
	t.Run("StorageResourceUndefined", func(t *testing.T) {
		require.Equal(t, "undefined", storage.StorageResourceUndefined.String())
	})

	t.Run("StorageResourcePackfile", func(t *testing.T) {
		require.Equal(t, "packfile", storage.StorageResourcePackfile.String())
	})

	t.Run("StorageResourceState", func(t *testing.T) {
		require.Equal(t, "state", storage.StorageResourceState.String())
	})

	t.Run("StorageResourceLock", func(t *testing.T) {
		require.Equal(t, "lock", storage.StorageResourceLock.String())
	})

	t.Run("StorageResourceECCPackfile", func(t *testing.T) {
		require.Equal(t, "ECC packfile", storage.StorageResourceECCPackfile.String())
	})

	t.Run("StorageResourceECCState", func(t *testing.T) {
		require.Equal(t, "ECC state", storage.StorageResourceECCState.String())
	})

	t.Run("UnknownStorageResource", func(t *testing.T) {
		require.Equal(t, "unknown", storage.StorageResource(999).String())
	})
}

func TestNewConfiguration(t *testing.T) {
	t.Run("ReturnsDefaultConfiguration", func(t *testing.T) {
		before := time.Now()
		cfg := storage.NewConfiguration()
		after := time.Now()

		require.NotNil(t, cfg)
		require.Equal(t, versioning.FromString(storage.VERSION), cfg.Version)

		require.False(t, cfg.Timestamp.IsZero())
		require.False(t, cfg.Timestamp.Before(before))
		require.False(t, cfg.Timestamp.After(after))

		require.NotEqual(t, uuid.Nil, cfg.RepositoryID)

		require.Equal(t, *packfile.NewDefaultConfiguration(), cfg.Packfile)
		require.Equal(t, *chunking.NewDefaultConfiguration(), cfg.Chunking)
		require.Equal(t, *hashing.NewDefaultConfiguration(), cfg.Hashing)

		require.NotNil(t, cfg.Compression)
		require.Equal(t, *compression.NewDefaultConfiguration(), *cfg.Compression)

		expectedEncryption := encryption.NewDefaultConfiguration()
		require.NotNil(t, cfg.Encryption)
		require.Equal(t, expectedEncryption.SubKeyAlgorithm, cfg.Encryption.SubKeyAlgorithm)
		require.Equal(t, expectedEncryption.DataAlgorithm, cfg.Encryption.DataAlgorithm)
		require.Equal(t, expectedEncryption.ChunkSize, cfg.Encryption.ChunkSize)
		require.Equal(t, expectedEncryption.KDFParams.KDF, cfg.Encryption.KDFParams.KDF)
		require.Len(t, cfg.Encryption.KDFParams.Salt, len(expectedEncryption.KDFParams.Salt))
		require.NotNil(t, cfg.Encryption.KDFParams.Argon2idParams)
		require.Nil(t, cfg.Encryption.KDFParams.ScryptParams)
		require.Nil(t, cfg.Encryption.KDFParams.Pbkdf2Params)
		require.Equal(t, expectedEncryption.Canary, cfg.Encryption.Canary)
	})

	t.Run("TwoNewConfigurations_DistinctRepositoryIDs", func(t *testing.T) {
		cfg1 := storage.NewConfiguration()
		cfg2 := storage.NewConfiguration()

		require.NotNil(t, cfg1)
		require.NotNil(t, cfg2)
		require.NotEqual(t, uuid.Nil, cfg1.RepositoryID)
		require.NotEqual(t, uuid.Nil, cfg2.RepositoryID)
		require.NotEqual(t, cfg1.RepositoryID, cfg2.RepositoryID)
	})
}

func TestConfigurationToBytes(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		cfg.Version = versioning.Version(42)

		data, err := cfg.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, data)

		var decoded storage.Configuration
		err = msgpack.Unmarshal(data, &decoded)
		require.NoError(t, err)

		require.True(t, cfg.Timestamp.Equal(decoded.Timestamp))
		require.Equal(t, cfg.RepositoryID, decoded.RepositoryID)
		require.Equal(t, cfg.Packfile, decoded.Packfile)
		require.Equal(t, cfg.Chunking, decoded.Chunking)
		require.Equal(t, cfg.Hashing, decoded.Hashing)
		require.Equal(t, cfg.Compression, decoded.Compression)
		require.Equal(t, cfg.Encryption, decoded.Encryption)
		require.Equal(t, versioning.Version(0), decoded.Version)
	})

	t.Run("ConfigurationsWithoutCompressionAndEncryption", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		cfg.Compression = nil
		cfg.Encryption = nil
		data, err := cfg.ToBytes()
		require.NoError(t, err)
		require.NotEmpty(t, data)

		var decoded storage.Configuration
		err = msgpack.Unmarshal(data, &decoded)
		require.NoError(t, err)
		require.Nil(t, decoded.Compression)
		require.Nil(t, decoded.Encryption)
		require.Equal(t, versioning.Version(0), decoded.Version)
	})
}

func TestNewConfigurationFromBytes(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cfg := storage.NewConfiguration()

		data, err := cfg.ToBytes()
		require.NoError(t, err)

		version := versioning.Version(42)
		decoded, err := storage.NewConfigurationFromBytes(version, data)
		require.NoError(t, err)
		require.NotNil(t, decoded)

		require.Equal(t, version, decoded.Version)
		require.True(t, cfg.Timestamp.Equal(decoded.Timestamp))
		require.Equal(t, cfg.RepositoryID, decoded.RepositoryID)
		require.Equal(t, cfg.Packfile, decoded.Packfile)
		require.Equal(t, cfg.Chunking, decoded.Chunking)
		require.Equal(t, cfg.Hashing, decoded.Hashing)
		require.Equal(t, cfg.Compression, decoded.Compression)
		require.Equal(t, cfg.Encryption, decoded.Encryption)
	})

	t.Run("UsesVersionArgument", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		cfg.Version = versioning.Version(999)
		data, err := cfg.ToBytes()
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromBytes(versioning.Version(7), data)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		require.Equal(t, versioning.Version(7), decoded.Version)
	})

	t.Run("Fails_IfDataInvalid", func(t *testing.T) {
		decoded, err := storage.NewConfigurationFromBytes(versioning.Version(1), []byte("not msgpack"))
		require.Nil(t, decoded)
		require.Error(t, err)
	})
}

func TestNewConfigurationFromWrappedBytes(t *testing.T) {
	t.Run("DeserializesWrappedConfiguration", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		payload, err := cfg.ToBytes()
		require.NoError(t, err)

		hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
		require.NotNil(t, hasher)

		wrappedReader, err := storage.Serialize(
			hasher,
			resources.RT_CONFIG,
			versioning.Version(42),
			bytes.NewReader(payload),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		require.Equal(t, versioning.Version(42), decoded.Version)
		require.True(t, cfg.Timestamp.Equal(decoded.Timestamp))
		require.Equal(t, cfg.RepositoryID, decoded.RepositoryID)
		require.Equal(t, cfg.Packfile, decoded.Packfile)
		require.Equal(t, cfg.Chunking, decoded.Chunking)
		require.Equal(t, cfg.Hashing, decoded.Hashing)
		require.Equal(t, cfg.Compression, decoded.Compression)
		require.Equal(t, cfg.Encryption, decoded.Encryption)
	})

	t.Run("UsesVersionFromHeader", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		cfg.Version = versioning.Version(999)
		payload, err := cfg.ToBytes()
		require.NoError(t, err)

		hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
		require.NotNil(t, hasher)

		wrappedReader, err := storage.Serialize(
			hasher,
			resources.RT_CONFIG,
			versioning.Version(7),
			bytes.NewReader(payload),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		require.Equal(t, versioning.Version(7), decoded.Version)
	})

	t.Run("Fails_IfPayloadIsInvalid", func(t *testing.T) {
		wrappedReader, err := storage.Serialize(
			sha256.New(),
			resources.RT_CONFIG,
			versioning.Version(1),
			bytes.NewReader([]byte("not msgpack")),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.Error(t, err)
	})

	t.Run("Fails_IfDataIsTooShort", func(t *testing.T) {
		decoded, err := storage.NewConfigurationFromWrappedBytes([]byte("short"))
		require.Nil(t, decoded)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Fails_IfMagicIsInvalid", func(t *testing.T) {
		payload := []byte{0x80}
		wrappedData := expectedSerializedData(
			t,
			"INVALID!",
			resources.RT_CONFIG,
			versioning.Version(1),
			payload,
		)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.EqualError(t, err, "invalid plakar magic: INVALID!")
	})

	t.Run("FailsIfWrappedResourceTypeIsInvalid", func(t *testing.T) {
		payload := []byte{0x80}
		wrappedData := expectedSerializedData(
			t,
			"_KLOSET_",
			resources.Type(9999),
			versioning.Version(1),
			payload,
		)

		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.EqualError(t, err, "invalid resource type")
	})

	t.Run("Fails_IfFooterIsTruncated", func(t *testing.T) {
		hasher := hashing.GetHasher(storage.DEFAULT_HASHING_ALGORITHM)
		require.NotNil(t, hasher)

		wrappedReader, err := storage.Serialize(
			hasher,
			resources.RT_CONFIG,
			versioning.Version(2),
			bytes.NewReader([]byte{}),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		wrappedData = wrappedData[:len(wrappedData)-1]
		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})

	t.Run("Fails_IfHMACIsInvalid", func(t *testing.T) {
		cfg := storage.NewConfiguration()
		require.NotNil(t, cfg)

		payload, err := cfg.ToBytes()
		require.NoError(t, err)

		wrappedReader, err := storage.Serialize(
			sha256.New(),
			resources.RT_CONFIG,
			versioning.Version(3),
			bytes.NewReader(payload),
		)
		require.NoError(t, err)

		wrappedData, err := io.ReadAll(wrappedReader)
		require.NoError(t, err)

		wrappedData[len(wrappedData)-1] ^= 0xff
		decoded, err := storage.NewConfigurationFromWrappedBytes(wrappedData)
		require.Nil(t, decoded)
		require.EqualError(t, err, "hmac mismatch")
	})
}

func TestRegister(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		proto string,
		config map[string]string,
	) (storage.Store, error) {
		return nil, nil
	}

	t.Run("ValidBackendRegistration", func(t *testing.T) {
		backendName := "test-register-backend"

		err := storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		t.Cleanup(func() { _ = storage.Unregister(backendName) })
		require.NoError(t, err)
	})

	t.Run("FailsIfBackendAlreadyRegistered", func(t *testing.T) {
		backendName := "test-register-duplicate"

		err := storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = storage.Unregister(backendName) })

		err = storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.EqualError(t, err, "storage backend 'test-register-duplicate' already registered")
	})
}

func TestUnregister(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		proto string,
		config map[string]string,
	) (storage.Store, error) {
		return nil, nil
	}

	t.Run("ValidBackendUnregistration", func(t *testing.T) {
		backendName := "test-unregister-backend"

		err := storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = storage.Unregister(backendName)
		require.NoError(t, err)
	})

	t.Run("Fails_IfBackendNotRegistered", func(t *testing.T) {
		backendName := "test-unregister-missing"

		err := storage.Unregister(backendName)
		require.EqualError(t, err, "storage backend 'test-unregister-missing' not registered")
	})

	t.Run("Register->Unregister->Register", func(t *testing.T) {
		backendName := "test-unregister-reregister"

		err := storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = storage.Unregister(backendName)
		require.NoError(t, err)

		err = storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() { _ = storage.Unregister(backendName) })
	})
}

func TestBackends(t *testing.T) {
	backendFn := func(
		ctx context.Context,
		proto string,
		config map[string]string,
	) (storage.Store, error) {
		return nil, nil
	}

	t.Run("GetRegisteredBackends", func(t *testing.T) {
		backendName1 := "test-backends-first"
		backendName2 := "test-backends-second"

		err := storage.Register(backendName1, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = storage.Register(backendName2, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = storage.Unregister(backendName1)
			_ = storage.Unregister(backendName2)
		})

		backends := storage.Backends()
		require.Contains(t, backends, backendName1)
		require.Contains(t, backends, backendName2)
	})

	t.Run("DoNotGetUnregisteredBackends", func(t *testing.T) {
		backendName := "test-backends-removed"

		err := storage.Register(backendName, location.FLAG_LOCALFS, backendFn)
		require.NoError(t, err)

		err = storage.Unregister(backendName)
		require.NoError(t, err)

		backends := storage.Backends()
		require.NotContains(t, backends, backendName)
	})
}

func TestNew(t *testing.T) {
	registerBackend := func(
		t *testing.T,
		name string,
		flags location.Flags,
		backendFn storage.StoreFn,
	) {
		t.Helper()

		err := storage.Register(name, flags, backendFn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = storage.Unregister(name) })
	}

	t.Run("Fails_IfLocationIsMissing", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		store, err := storage.New(appCtx, map[string]string{})
		require.Nil(t, store)
		require.EqualError(t, err, "missing location")
	})

	t.Run("Fails_IfBackendDoesNotExist", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		store, err := storage.New(appCtx, map[string]string{
			"location": "unknown://some/path",
		})
		require.Nil(t, store)
		require.EqualError(t, err, "backend 'unknown' does not exist")
	})

	t.Run("UseDefaultFSProtocolForRelativeLocalPath", func(t *testing.T) {
		var (
			called           bool
			receivedCtx      context.Context
			receivedProto    string
			receivedLocation string
			receivedConfig   map[string]string
		)

		backend := func(
			ctx context.Context,
			proto string,
			storeConfig map[string]string,
		) (storage.Store, error) {
			called = true
			receivedCtx = ctx
			receivedProto = proto
			receivedLocation = storeConfig["location"]
			receivedConfig = storeConfig
			return nil, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, backend)

		appCtx := kcontext.NewKContext()
		appCtx.CWD = t.TempDir()
		storeConfig := map[string]string{
			"location": "relative/path",
		}

		store, err := storage.New(appCtx, storeConfig)
		require.NoError(t, err)
		require.Nil(t, store)
		require.True(t, called)
		require.Same(t, appCtx, receivedCtx)
		require.Equal(t, "fs", receivedProto)
		expectedLocation := "fs://" + filepath.Join(appCtx.CWD, "relative/path")
		require.Equal(t, expectedLocation, receivedLocation)
		require.Equal(t, expectedLocation, storeConfig["location"])
		require.Equal(t, storeConfig, receivedConfig)
	})

	t.Run("KeepAbsoluteLocalPathUnchanged", func(t *testing.T) {
		var (
			receivedProto    string
			receivedLocation string
		)

		backend := func(
			ctx context.Context,
			proto string,
			storeConfig map[string]string,
		) (storage.Store, error) {
			receivedProto = proto
			receivedLocation = storeConfig["location"]
			return nil, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, backend)

		appCtx := kcontext.NewKContext()
		absolutePath := filepath.Join(t.TempDir(), "some", "path")
		storeConfig := map[string]string{
			"location": "fs://" + absolutePath,
		}

		store, err := storage.New(appCtx, storeConfig)
		require.NoError(t, err)
		require.Nil(t, store)
		require.Equal(t, "fs", receivedProto)
		require.Equal(t, "fs://"+absolutePath, receivedLocation)
		require.Equal(t, "fs://"+absolutePath, storeConfig["location"])
	})

	t.Run("DoNotRewriteNonLocalLocation", func(t *testing.T) {
		var (
			receivedProto    string
			receivedLocation string
		)

		backend := func(
			ctx context.Context,
			proto string,
			storeConfig map[string]string,
		) (storage.Store, error) {
			receivedProto = proto
			receivedLocation = storeConfig["location"]
			return nil, nil
		}

		registerBackend(t, "s3", 0, backend)

		appCtx := kcontext.NewKContext()
		storeConfig := map[string]string{
			"location": "s3://bucket/path",
		}

		store, err := storage.New(appCtx, storeConfig)
		require.NoError(t, err)
		require.Nil(t, store)
		require.Equal(t, "s3", receivedProto)
		require.Equal(t, "s3://bucket/path", receivedLocation)
		require.Equal(t, "s3://bucket/path", storeConfig["location"])
	})

	t.Run("RedirectFSArchiveToPTARBackend", func(t *testing.T) {
		var (
			fsCalled            bool
			ptarCalled          bool
			receivedProto       string
			receivedLocation    string
			receivedStoreConfig map[string]string
		)

		fsBackend := func(
			ctx context.Context,
			proto string,
			storeConfig map[string]string,
		) (storage.Store, error) {
			fsCalled = true
			return nil, nil
		}

		ptarBackend := func(
			ctx context.Context,
			proto string,
			storeConfig map[string]string,
		) (storage.Store, error) {
			ptarCalled = true
			receivedProto = proto
			receivedLocation = storeConfig["location"]
			receivedStoreConfig = storeConfig
			return nil, nil
		}

		registerBackend(t, "fs", location.FLAG_LOCALFS, fsBackend)
		registerBackend(t, "ptar", 0, ptarBackend)

		appCtx := kcontext.NewKContext()
		appCtx.CWD = t.TempDir()

		storeConfig := map[string]string{
			"location": "archive.ptar",
		}

		store, err := storage.New(appCtx, storeConfig)
		require.NoError(t, err)
		require.Nil(t, store)
		require.False(t, fsCalled)
		require.True(t, ptarCalled)
		require.Equal(t, "ptar", receivedProto)
		expectedLocation := "ptar://" + filepath.Join(appCtx.CWD, "archive.ptar")
		require.Equal(t, expectedLocation, receivedLocation)
		require.Equal(t, expectedLocation, storeConfig["location"])
		require.Equal(t, storeConfig, receivedStoreConfig)
	})

	t.Run("PropagateBackendError", func(t *testing.T) {
		expectedErr := errors.New("backend failure")

		backend := func(
			ctx context.Context,
			proto string,
			storeConfig map[string]string,
		) (storage.Store, error) {
			return nil, expectedErr
		}

		registerBackend(t, "s3", 0, backend)

		appCtx := kcontext.NewKContext()
		storeConfig := map[string]string{
			"location": "s3://bucket/path",
		}

		store, err := storage.New(appCtx, storeConfig)
		require.Nil(t, store)
		require.ErrorIs(t, err, expectedErr)
	})
}

func TestOpen(t *testing.T) {
	registerBackend := func(
		t *testing.T,
		name string,
		flags location.Flags,
		backendFn storage.StoreFn,
	) {
		t.Helper()

		err := storage.Register(name, flags, backendFn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = storage.Unregister(name) })
	}

	t.Run("ReturnsValidStoreAndSerializedConfig", func(t *testing.T) {
		expectedData := []byte("serialized config")
		expectedStore := mockStore{
			openData: expectedData,
		}
		registerBackend(t, "test-open-success", 0, newMockBackend(&expectedStore, nil))
		appCtx := kcontext.NewKContext()

		store, serializedConfig, err := storage.Open(appCtx, map[string]string{
			"location": "test-open-success://some/path",
		})
		require.NoError(t, err)
		require.Same(t, &expectedStore, store)
		require.Equal(t, expectedData, serializedConfig)
	})

	t.Run("PropagatesStoreOpenError", func(t *testing.T) {
		expectedErr := errors.New("opening error")
		expectedStore := &mockStore{
			openErr: expectedErr,
		}

		registerBackend(t, "test-open-error", 0, newMockBackend(expectedStore, nil))
		appCtx := kcontext.NewKContext()

		store, serializedConfig, err := storage.Open(appCtx, map[string]string{
			"location": "test-open-error://some/path",
		})
		require.Nil(t, store)
		require.Nil(t, serializedConfig)
		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("Fails_IfStoreCreationFails", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		store, serializedConfig, err := storage.Open(appCtx, map[string]string{
			"location": "unknown://some/path",
		})
		require.Nil(t, store)
		require.Nil(t, serializedConfig)
		require.EqualError(t, err, "backend 'unknown' does not exist")
	})
}

func TestCreate(t *testing.T) {
	registerBackend := func(
		t *testing.T,
		name string,
		flags location.Flags,
		backendFn storage.StoreFn,
	) {
		t.Helper()

		err := storage.Register(name, flags, backendFn)
		require.NoError(t, err)
		t.Cleanup(func() { _ = storage.Unregister(name) })
	}

	t.Run("ValidCreation", func(t *testing.T) {
		expectedConfig := []byte("serialized config")
		expectedStore := mockStore{}
		registerBackend(t, "test-create-success", 0, newMockBackend(&expectedStore, nil))
		appCtx := kcontext.NewKContext()

		store, err := storage.Create(appCtx, map[string]string{
			"location": "test-create-success://some/path",
		}, expectedConfig)

		require.NoError(t, err)
		require.Same(t, &expectedStore, store)
		require.True(t, expectedStore.createCalled)
		require.Equal(t, expectedConfig, expectedStore.createInput)
	})

	t.Run("NilConfigurationIsValid", func(t *testing.T) {
		expectedStore := mockStore{}
		registerBackend(t, "test-create-nil-config", 0, newMockBackend(&expectedStore, nil))
		appCtx := kcontext.NewKContext()

		store, err := storage.Create(appCtx, map[string]string{
			"location": "test-create-nil-config://some/path",
		}, nil)
		require.NoError(t, err)
		require.Same(t, &expectedStore, store)
		require.True(t, expectedStore.createCalled)
		require.Nil(t, expectedStore.createInput)
	})

	t.Run("PropagatesStoreCreateError", func(t *testing.T) {
		expectedErr := errors.New("creating error")
		expectedConfig := []byte("serialized config")
		expectedStore := mockStore{
			createErr: expectedErr,
		}
		registerBackend(t, "test-create-error", 0, newMockBackend(&expectedStore, nil))
		appCtx := kcontext.NewKContext()

		store, err := storage.Create(appCtx, map[string]string{
			"location": "test-create-error://some/path",
		}, expectedConfig)
		require.Nil(t, store)
		require.ErrorIs(t, err, expectedErr)
		require.True(t, expectedStore.createCalled)
		require.Equal(t, expectedConfig, expectedStore.createInput)
	})

	t.Run("FailsIfStoreCreationFails", func(t *testing.T) {
		appCtx := kcontext.NewKContext()

		store, err := storage.Create(appCtx, map[string]string{
			"location": "unknown://some/path",
		}, []byte("serialized config"))
		require.Nil(t, store)
		require.EqualError(t, err, "backend 'unknown' does not exist")
	})
}
