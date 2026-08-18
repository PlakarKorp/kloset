package compression

import (
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const DEFAULT_COMPRESSION_ALGORITHM = "LZ4"

type Configuration struct {
	Algorithm  string `json:"algorithm"`
	Level      int    `json:"level"`       // Compression level (-1 for default)
	WindowSize int    `json:"window_size"` // Window size for algorithms like zstd or Brotli
	ChunkSize  int    `json:"chunk_size"`  // Chunk size for streaming compression
	BlockSize  int    `json:"block_size"`  // Block size for block-based algorithms like bzip2
	EnableCRC  bool   `json:"enable_CRC"`  // Enable/disable checksum (e.g., gzip CRC32, zstd)
}

func NewDefaultConfiguration() *Configuration {
	configuration, _ := LookupDefaultConfiguration(DEFAULT_COMPRESSION_ALGORITHM)
	return configuration
}

func LookupDefaultConfiguration(algorithm string) (*Configuration, error) {
	switch algorithm {
	case "LZ4":
		return &Configuration{
			Algorithm:  "LZ4",
			Level:      int(lz4.Level9),
			WindowSize: -1,
			ChunkSize:  -1,
			BlockSize:  -1,
			EnableCRC:  false,
		}, nil
	case "GZIP":
		return &Configuration{
			Algorithm:  "GZIP",
			Level:      -1,
			WindowSize: -1,
			ChunkSize:  -1,
			BlockSize:  -1,
			EnableCRC:  false,
		}, nil
	case "ZSTD":
		return &Configuration{
			Algorithm:  "ZSTD",
			Level:      int(zstd.SpeedDefault),
			WindowSize: -1,
			ChunkSize:  -1,
			BlockSize:  -1,
			EnableCRC:  false,
		}, nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %s", algorithm)
	}
}

func DeflateStream(name string, r io.Reader) (io.Reader, error) {
	m := map[string]func(io.Reader) (io.Reader, error){
		"GZIP": DeflateGzipStream,
		"LZ4":  DeflateLZ4Stream,
		"ZSTD": DeflateZstdStream,
	}
	if fn, exists := m[name]; exists {
		return fn(r)
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func DeflateGzipStream(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	go func() {
		gw := gzip.NewWriter(pw)
		defer pw.Close()
		defer gw.Close()

		_, err := io.Copy(gw, r)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

func DeflateLZ4Stream(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	go func() {
		lw := lz4.NewWriter(pw)
		defer pw.Close()
		defer lw.Close()
		_, err := io.Copy(lw, r)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

func DeflateZstdStream(r io.Reader) (io.Reader, error) {
	pr, pw := io.Pipe()
	go func() {
		zw, err := zstd.NewWriter(pw)
		if err != nil {
			pw.CloseWithError(err)
			return
		}

		if _, err := io.Copy(zw, r); err != nil {
			zw.Close()
			pw.CloseWithError(err)
			return
		}

		// Close writes the final frame, so it can't be deferred
		// past pw.Close() or the reader sees a truncated stream.
		if err := zw.Close(); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	return pr, nil
}

type readCloserInternal struct {
	input  io.Closer
	reader io.ReadCloser
}

func (c *readCloserInternal) Read(p []byte) (int, error) { return c.reader.Read(p) }
func (c *readCloserInternal) Close() error               { c.reader.Close(); return c.input.Close() }

func InflateStream(name string, r io.ReadCloser) (io.ReadCloser, error) {
	m := map[string]func(io.ReadCloser) (io.ReadCloser, error){
		"GZIP": InflateGzipStream,
		"LZ4":  InflateLZ4Stream,
		"ZSTD": InflateZstdStream,
	}
	if fn, exists := m[name]; exists {
		or, err := fn(r)
		if err != nil {
			return nil, err
		}

		return &readCloserInternal{
			input:  r,
			reader: or,
		}, nil
	}
	return nil, fmt.Errorf("unsupported compression method %q", name)
}

func InflateGzipStream(r io.ReadCloser) (io.ReadCloser, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		defer gz.Close()

		_, err := io.Copy(pw, gz)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

var lz4ReaderPool = sync.Pool{
	New: func() any {
		return lz4.NewReader(nil)
	},
}

func InflateLZ4Stream(r io.ReadCloser) (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	lz := lz4.NewReader(r)
	go func() {
		defer pw.Close()

		_, err := io.Copy(pw, lz)
		if err != nil {
			pw.CloseWithError(err)
		}
	}()
	return pr, nil
}

func InflateZstdStream(r io.ReadCloser) (io.ReadCloser, error) {
	zr, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		defer zr.Close()

		if _, err := io.Copy(pw, zr.IOReadCloser()); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()
	return pr, nil
}
