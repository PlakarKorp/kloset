package compression_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"

	cprss "github.com/PlakarKorp/kloset/compression"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultConfiguration(t *testing.T) {
	t.Run("CheckDefaults", func(t *testing.T) {
		cfg := cprss.NewDefaultConfiguration()

		require.NotNil(t, cfg)
		require.Equal(t, "LZ4", cfg.Algorithm)
		require.Equal(t, int(lz4.Level9), cfg.Level)
		require.Equal(t, -1, cfg.WindowSize)
		require.Equal(t, -1, cfg.ChunkSize)
		require.Equal(t, -1, cfg.BlockSize)
		require.False(t, cfg.EnableCRC)
	})
}

func TestLookupDefaultConfiguration(t *testing.T) {
	t.Run("CheckLZ4Defaults", func(t *testing.T) {
		cfg, err := cprss.LookupDefaultConfiguration("LZ4")

		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "LZ4", cfg.Algorithm)
		require.Equal(t, int(lz4.Level9), cfg.Level)
		require.Equal(t, -1, cfg.WindowSize)
		require.Equal(t, -1, cfg.ChunkSize)
		require.Equal(t, -1, cfg.BlockSize)
		require.False(t, cfg.EnableCRC)
	})

	t.Run("CheckGZIPDefaults", func(t *testing.T) {
		cfg, err := cprss.LookupDefaultConfiguration("GZIP")

		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "GZIP", cfg.Algorithm)
		require.Equal(t, -1, cfg.Level)
		require.Equal(t, -1, cfg.WindowSize)
		require.Equal(t, -1, cfg.ChunkSize)
		require.Equal(t, -1, cfg.BlockSize)
		require.False(t, cfg.EnableCRC)
	})

	t.Run("CheckUnknownAlgorithm", func(t *testing.T) {
		cfg, err := cprss.LookupDefaultConfiguration("UNKNOWN")

		require.Nil(t, cfg)
		require.EqualError(t, err, "unknown hashing algorithm: UNKNOWN")
	})
}

type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("forced read error")
}

func compressDataForTest(t *testing.T, algorithm string, data []byte) []byte {
	t.Helper()

	var compressedData bytes.Buffer
	var writer io.WriteCloser

	switch algorithm {
	case "GZIP":
		writer = gzip.NewWriter(&compressedData)
	case "LZ4":
		writer = lz4.NewWriter(&compressedData)
	default:
		require.FailNow(t, "unsupported algorithm", "algorithm=%s", algorithm)
	}

	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	return compressedData.Bytes()
}

func decompressDataForTest(t *testing.T, algorithm string, r io.Reader) []byte {
	t.Helper()

	var reader io.Reader
	var closer io.Closer

	switch algorithm {
	case "GZIP":
		r, err := gzip.NewReader(r)
		require.NoError(t, err)
		reader = r
		closer = r
	case "LZ4":
		reader = lz4.NewReader(r)
		closer = nil
	default:
		require.FailNow(t, "unsupported algorithm", "algorithm=%s", algorithm)
	}

	decompressedData, err := io.ReadAll(reader)
	require.NoError(t, err)

	if closer != nil {
		require.NoError(t, closer.Close())
	}

	return decompressedData
}

func TestDeflateGzipStream(t *testing.T) {
	t.Run("CompressData", func(t *testing.T) {
		data := []byte("hello gzip")

		compressedReader, err := cprss.DeflateGzipStream(bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		decompressedData := decompressDataForTest(t, "GZIP", compressedReader)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("CompressEmptyData", func(t *testing.T) {
		data := []byte{}

		compressedReader, err := cprss.DeflateGzipStream(bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		decompressedData := decompressDataForTest(t, "GZIP", compressedReader)
		require.Empty(t, decompressedData)
	})

	t.Run("Fails_IfSourceReaderFails", func(t *testing.T) {
		compressedReader, err := cprss.DeflateGzipStream(&errorReader{})
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		_, readErr := io.ReadAll(compressedReader)
		require.ErrorContains(t, readErr, "forced read error")
	})
}

func TestInflateGzipStream(t *testing.T) {
	t.Run("InflateData", func(t *testing.T) {
		data := []byte("hello gzip")
		compressedData := compressDataForTest(t, "GZIP", data)

		decompressedReader, err := cprss.InflateGzipStream(io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		decompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("CompressEmptyData", func(t *testing.T) {
		data := []byte{}
		compressedData := compressDataForTest(t, "GZIP", data)

		decompressedReader, err := cprss.InflateGzipStream(io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		decompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.Empty(t, decompressedData)
	})

	t.Run("FailOnInvalidData", func(t *testing.T) {
		decompressedReader, err := cprss.InflateGzipStream(io.NopCloser(bytes.NewReader([]byte("not gzip"))))
		require.Error(t, err)
		require.Nil(t, decompressedReader)
	})

	t.Run("FailsOnCorruptedData", func(t *testing.T) {
		data := []byte("hello gzip")
		compressedData := compressDataForTest(t, "GZIP", data)
		corruptedData := compressedData[:len(compressedData)-4]

		decompressedReader, err := cprss.InflateGzipStream(io.NopCloser(bytes.NewReader(corruptedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		_, err = io.ReadAll(decompressedReader)
		require.Error(t, err)
	})
}

func TestDeflateLZ4Stream(t *testing.T) {
	t.Run("CompressData", func(t *testing.T) {
		data := []byte("hello lz4")

		compressedReader, err := cprss.DeflateLZ4Stream(bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		decompressedData := decompressDataForTest(t, "LZ4", compressedReader)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("CompressEmptyData", func(t *testing.T) {
		data := []byte{}

		compressedReader, err := cprss.DeflateLZ4Stream(bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		decompressedData := decompressDataForTest(t, "LZ4", compressedReader)
		require.Empty(t, decompressedData)
	})

	t.Run("Fails_IfSourceReaderFails", func(t *testing.T) {
		compressedReader, err := cprss.DeflateLZ4Stream(&errorReader{})
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		_, err = io.ReadAll(compressedReader)
		require.ErrorContains(t, err, "forced read error")
	})
}

func TestInflateLZ4Stream(t *testing.T) {
	t.Run("InflateData", func(t *testing.T) {
		data := []byte("hello lz4")
		compressedData := compressDataForTest(t, "LZ4", data)

		decompressedReader, err := cprss.InflateLZ4Stream(io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		decompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("CompressEmptyData", func(t *testing.T) {
		data := []byte{}
		compressedData := compressDataForTest(t, "LZ4", data)

		decompressedReader, err := cprss.InflateLZ4Stream(io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		decompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.Empty(t, decompressedData)
	})

	t.Run("FailOnInvalidData", func(t *testing.T) {
		decompressedReader, err := cprss.InflateLZ4Stream(io.NopCloser(bytes.NewReader([]byte("not lz4"))))

		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		_, err = io.ReadAll(decompressedReader)
		require.Error(t, err)
	})

	t.Run("FailsOnCorruptedData", func(t *testing.T) {
		data := bytes.Repeat([]byte("hello lz4"), 1024)
		compressedData := compressDataForTest(t, "LZ4", data)
		corruptedData := compressedData[:len(compressedData)-1]

		decompressedReader, err := cprss.InflateLZ4Stream(io.NopCloser(bytes.NewReader(corruptedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		_, err = io.ReadAll(decompressedReader)
		require.Error(t, err)
	})
}

func TestDeflateStream(t *testing.T) {
	t.Run("DispatchGZIP", func(t *testing.T) {
		data := []byte("hello gzip")

		compressedReader, err := cprss.DeflateStream("GZIP", bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		decompressedData := decompressDataForTest(t, "GZIP", compressedReader)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("DispatchLZ4", func(t *testing.T) {
		data := []byte("hello lz4")

		compressedReader, err := cprss.DeflateStream("LZ4", bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		decompressedData := decompressDataForTest(t, "LZ4", compressedReader)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("PartialReadOnGZIPData", func(t *testing.T) {
		data := bytes.Repeat([]byte("hello gzip"), 128)

		compressedReader, err := cprss.DeflateStream("GZIP", bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		firstChunk := make([]byte, 8)
		n, err := compressedReader.Read(firstChunk)
		require.NoError(t, err)
		require.Greater(t, n, 0)

		remainingCompressedData, err := io.ReadAll(compressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, remainingCompressedData)

		fullCompressedData := append(firstChunk[:n], remainingCompressedData...)
		decompressedData := decompressDataForTest(t, "GZIP", bytes.NewReader(fullCompressedData))
		require.NotEmpty(t, remainingCompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("PartialReadOnLZ4Data", func(t *testing.T) {
		data := bytes.Repeat([]byte("hello lz4"), 128)

		compressedReader, err := cprss.DeflateStream("LZ4", bytes.NewReader(data))
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		firstChunk := make([]byte, 8)
		n, err := compressedReader.Read(firstChunk)
		require.NoError(t, err)
		require.Greater(t, n, 0)

		remainingCompressedData, err := io.ReadAll(compressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, remainingCompressedData)

		fullCompressedData := append(firstChunk[:n], remainingCompressedData...)
		decompressedData := decompressDataForTest(t, "LZ4", bytes.NewReader(fullCompressedData))
		require.NotEmpty(t, remainingCompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("Fails_IfSourceReaderFails", func(t *testing.T) {
		compressedReader, err := cprss.DeflateStream("GZIP", &errorReader{})
		require.NoError(t, err)
		require.NotNil(t, compressedReader)

		_, err = io.ReadAll(compressedReader)
		require.ErrorContains(t, err, "forced read error")
	})

	t.Run("FailOnUnsupportedAlgorithm", func(t *testing.T) {
		compressedReader, err := cprss.DeflateStream("UNKNOWN", bytes.NewReader([]byte("test data")))
		require.Nil(t, compressedReader)
		require.EqualError(t, err, `unsupported compression method "UNKNOWN"`)
	})
}

func TestInflateStream(t *testing.T) {
	t.Run("InitializationError", func(t *testing.T) {
		decompressedReader, err := cprss.InflateStream("GZIP", io.NopCloser(bytes.NewReader([]byte("not gzip"))))
		require.Nil(t, decompressedReader)
		require.Error(t, err)
	})

	t.Run("DispatchGZIP", func(t *testing.T) {
		data := []byte("hello gzip")
		compressedData := compressDataForTest(t, "GZIP", data)

		decompressedReader, err := cprss.InflateStream("GZIP", io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		decompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("DispatchLZ4", func(t *testing.T) {
		data := []byte("hello lz4")
		compressedData := compressDataForTest(t, "LZ4", data)

		decompressedReader, err := cprss.InflateStream("LZ4", io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		decompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, decompressedData)
		require.Equal(t, data, decompressedData)
	})

	t.Run("PartialReadOnGZIPData", func(t *testing.T) {
		data := bytes.Repeat([]byte("hello gzip"), 128)
		compressedData := compressDataForTest(t, "GZIP", data)

		decompressedReader, err := cprss.InflateStream("GZIP", io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		firstChunk := make([]byte, 8)
		n, err := decompressedReader.Read(firstChunk)
		require.NoError(t, err)
		require.Greater(t, n, 0)

		remainingDecompressedData, err := io.ReadAll(decompressedReader)
		require.NotEmpty(t, remainingDecompressedData)
		require.NoError(t, err)

		fullDecompressedData := append(firstChunk[:n], remainingDecompressedData...)
		require.NotEmpty(t, fullDecompressedData)
		require.Equal(t, data, fullDecompressedData)
	})

	t.Run("PartialReadOnLZ4Data", func(t *testing.T) {
		data := bytes.Repeat([]byte("hello lz4"), 128)
		compressedData := compressDataForTest(t, "LZ4", data)

		decompressedReader, err := cprss.InflateStream("LZ4", io.NopCloser(bytes.NewReader(compressedData)))
		require.NoError(t, err)
		require.NotNil(t, decompressedReader)

		firstChunk := make([]byte, 8)
		n, err := decompressedReader.Read(firstChunk)
		require.NoError(t, err)
		require.Greater(t, n, 0)

		remainingDecompressedData, err := io.ReadAll(decompressedReader)
		require.NoError(t, err)
		require.NotEmpty(t, remainingDecompressedData)

		fullDecompressedData := append(firstChunk[:n], remainingDecompressedData...)
		require.NotEmpty(t, fullDecompressedData)
		require.Equal(t, data, fullDecompressedData)
	})

	t.Run("FailOnUnsupportedAlgorithm", func(t *testing.T) {
		decompressedReader, err := cprss.InflateStream("UNKNOWN", io.NopCloser(bytes.NewReader([]byte("test data"))))
		require.Nil(t, decompressedReader)
		require.EqualError(t, err, `unsupported compression method "UNKNOWN"`)
	})
}

func TestUnsupportedAlgorithm(t *testing.T) {
	_, err := cprss.DeflateStream("unsupported", bytes.NewReader([]byte("test data")))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}

	_, err = cprss.InflateStream("unsupported", io.NopCloser(bytes.NewReader([]byte("test data"))))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}
}

func TestDeflateStreamErrorHandling(t *testing.T) {
	_, err := cprss.DeflateStream("unsupported", bytes.NewReader([]byte("test data")))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}

	_, err = cprss.DeflateStream("gzip", &errorReader{})
	if err == nil {
		t.Error("Expected error for reader failure, got nil")
	}
}

func TestInflateStreamErrorHandling(t *testing.T) {
	_, err := cprss.InflateStream("unsupported", io.NopCloser(bytes.NewReader([]byte("test data"))))
	if err == nil {
		t.Error("Expected error for unsupported compression method, got nil")
	}

	_, err = cprss.InflateStream("gzip", io.NopCloser(&errorReader{}))
	if err == nil {
		t.Error("Expected error for reader failure, got nil")
	}
}

func TestDeflateStreamRewindLogic(t *testing.T) {
	data := []byte("test rewind logic")
	compressedReader, err := cprss.DeflateStream("GZIP", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("DeflateStream failed: %v", err)
	}

	buf := make([]byte, 1)
	n, err := compressedReader.Read(buf)
	if err != nil || n != 1 {
		t.Fatalf("Rewind logic test failed: expected 1 byte read, got %d, error: %v", n, err)
	}
}

func TestLargeDataCompression(t *testing.T) {
	largeData := make([]byte, 10*1024*1024) // 10MB of data
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	compressedReader, err := cprss.DeflateStream("LZ4", bytes.NewReader(largeData))
	if err != nil {
		t.Fatalf("DeflateStream failed for large data: %v", err)
	}

	decompressedReader, err := cprss.InflateStream("LZ4", io.NopCloser(compressedReader))
	if err != nil {
		t.Fatalf("InflateStream failed for large data: %v", err)
	}

	var decompressedData bytes.Buffer
	_, err = io.Copy(&decompressedData, decompressedReader)
	if err != nil {
		t.Fatalf("Reading decompressed data failed for large data: %v", err)
	}

	if !bytes.Equal(largeData, decompressedData.Bytes()) {
		t.Errorf("Decompressed large data does not match original. Lengths differ")
	}
}
