package storage_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	storage "github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

var errReader = errors.New("forced read error")

type errorReader struct {
	data []byte
	read bool
}

func (r *errorReader) Read(p []byte) (int, error) {
	if !r.read {
		r.read = true
		n := copy(p, r.data)
		return n, errReader
	}

	return 0, errReader
}

func TestSerialize(t *testing.T) {
	expectedSerializedData := func(
		t *testing.T,
		resourceType resources.Type,
		version versioning.Version,
		payload []byte,
	) []byte {
		t.Helper()

		header := make([]byte, storage.STORAGE_HEADER_SIZE)
		copy(header[0:8], []byte("_KLOSET_"))
		binary.LittleEndian.PutUint32(header[8:12], uint32(resourceType))
		binary.LittleEndian.PutUint32(header[12:16], uint32(version))

		hasher := sha256.New()
		_, err := hasher.Write(header)
		require.NoError(t, err)

		_, err = hasher.Write(payload)
		require.NoError(t, err)

		footer := hasher.Sum(nil)

		serialized := make([]byte, 0, len(header)+len(payload)+len(footer))
		serialized = append(serialized, header...)
		serialized = append(serialized, payload...)
		serialized = append(serialized, footer...)

		return serialized
	}

	readAllInChunks := func(t *testing.T, r io.Reader, chunkSize int) ([]byte, error) {
		t.Helper()

		var out bytes.Buffer
		buf := make([]byte, chunkSize)

		for {
			n, err := r.Read(buf)
			if n > 0 {
				_, writeErr := out.Write(buf[:n])
				require.NoError(t, writeErr)
			}

			if err != nil {
				if errors.Is(err, io.EOF) {
					return out.Bytes(), nil
				}
				return out.Bytes(), err
			}
		}
	}

	t.Run("EmptyPayload", func(t *testing.T) {
		resourceType := resources.RT_CONFIG
		version := versioning.Version(42)
		payload := []byte{}

		reader, err := storage.Serialize(sha256.New(), resourceType, version, bytes.NewReader(payload))
		require.NoError(t, err)
		require.NotNil(t, reader)

		serializedData, err := io.ReadAll(reader)
		require.NoError(t, err)

		expected := expectedSerializedData(t, resourceType, version, payload)
		require.Equal(t, expected, serializedData)
		require.Len(t, serializedData, int(storage.STORAGE_HEADER_SIZE+storage.STORAGE_FOOTER_SIZE))
		require.Equal(t, []byte("_KLOSET_"), serializedData[:8])
		require.Equal(t, uint32(resourceType), binary.LittleEndian.Uint32(serializedData[8:12]))
		require.Equal(t, uint32(version), binary.LittleEndian.Uint32(serializedData[12:16]))
	})

	t.Run("ValidPayload", func(t *testing.T) {
		resourceType := resources.RT_CONFIG
		version := versioning.Version(7)
		payload := []byte("hello serialized world")

		reader, err := storage.Serialize(sha256.New(), resourceType, version, bytes.NewReader(payload))
		require.NoError(t, err)
		require.NotNil(t, reader)

		serializedData, err := io.ReadAll(reader)
		require.NoError(t, err)

		expected := expectedSerializedData(t, resourceType, version, payload)
		require.Equal(t, expected, serializedData)
		require.Len(t, serializedData, int(storage.STORAGE_HEADER_SIZE)+len(payload)+int(storage.STORAGE_FOOTER_SIZE))
		require.Equal(t, payload, serializedData[storage.STORAGE_HEADER_SIZE:len(serializedData)-int(storage.STORAGE_FOOTER_SIZE)])
	})

	t.Run("ReadInSmallChunks", func(t *testing.T) {
		resourceType := resources.RT_CONFIG
		version := versioning.Version(9)
		payload := bytes.Repeat([]byte("abc123"), 128)

		reader, err := storage.Serialize(sha256.New(), resourceType, version, bytes.NewReader(payload))
		require.NoError(t, err)
		require.NotNil(t, reader)

		serializedData, err := readAllInChunks(t, reader, 3)
		require.NoError(t, err)

		expected := expectedSerializedData(t, resourceType, version, payload)
		require.Equal(t, expected, serializedData)
	})

	t.Run("Fails_IfReaderFailed", func(t *testing.T) {
		resourceType := resources.RT_CONFIG
		version := versioning.Version(11)
		payloadPrefix := []byte("partial payload")

		reader, err := storage.Serialize(sha256.New(), resourceType, version, &errorReader{data: payloadPrefix})
		require.NoError(t, err)
		require.NotNil(t, reader)

		serializedData, err := io.ReadAll(reader)
		require.ErrorIs(t, err, errReader)
		require.GreaterOrEqual(t, len(serializedData), int(storage.STORAGE_HEADER_SIZE))
		require.Equal(t, []byte("_KLOSET_"), serializedData[:8])
		require.Equal(t, uint32(resourceType), binary.LittleEndian.Uint32(serializedData[8:12]))
		require.Equal(t, uint32(version), binary.LittleEndian.Uint32(serializedData[12:16]))

		payloadPart := serializedData[storage.STORAGE_HEADER_SIZE:]
		require.Equal(t, payloadPrefix, payloadPart)
	})

	t.Run("Fails_IfReaderHasAnEmptyBuffer", func(t *testing.T) {
		reader, err := storage.Serialize(
			sha256.New(),
			resources.RT_CONFIG,
			versioning.Version(1),
			bytes.NewReader([]byte("payload")),
		)
		require.NoError(t, err)
		require.NotNil(t, reader)

		n, err := reader.Read(make([]byte, 0))

		require.NoError(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("HasherResetIfUsed", func(t *testing.T) {
		resourceType := resources.RT_CONFIG
		version := versioning.Version(13)
		payload := []byte("payload")

		hasher := sha256.New()
		_, err := hasher.Write([]byte("dirty state"))
		require.NoError(t, err)

		reader, err := storage.Serialize(hasher, resourceType, version, bytes.NewReader(payload))
		require.NoError(t, err)
		require.NotNil(t, reader)

		serializedData, err := io.ReadAll(reader)
		require.NoError(t, err)

		expected := expectedSerializedData(t, resourceType, version, payload)
		require.Equal(t, expected, serializedData)
	})

	t.Run("LargePayload", func(t *testing.T) {
		resourceType := resources.RT_CONFIG
		version := versioning.Version(21)
		payload := make([]byte, 2*1024*1024)
		for i := range payload {
			payload[i] = byte(i % 251)
		}

		reader, err := storage.Serialize(sha256.New(), resourceType, version, bytes.NewReader(payload))
		require.NoError(t, err)
		require.NotNil(t, reader)

		serializedData, err := io.ReadAll(reader)
		require.NoError(t, err)

		expected := expectedSerializedData(t, resourceType, version, payload)
		require.Equal(t, expected, serializedData)
	})
}
