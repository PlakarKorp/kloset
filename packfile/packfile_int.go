package packfile

import (
	"hash"
	"io"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
)

const VERSION = "1.0.0"

type Blob struct {
	Type    resources.Type
	Version versioning.Version
	MAC     objects.MAC
	Offset  uint64
	Length  uint32
	Flags   uint32
}

const BLOB_RECORD_SIZE = 56

type EncodingFn func(io.Reader) (io.Reader, error)
type HashFactory func() hash.Hash

// It is the responsibility of the packfile, to return a reader (through
// serialize) that will produce the expected format as documented below:
//
// - 0....N Blob bytes (encoded individually)
// - Index into blobs (encoded as a whole)
// - MAC of index
// - Footer (encoded as a whole)
// - Encoded footer length (uint32)
type Packfile interface {
	AddBlob(t resources.Type, v versioning.Version, mac objects.MAC, data []byte, flags uint32) error
	Size() uint64
	Entries() []Blob
	Serialize(EncodingFn) (io.Reader, objects.MAC, error)
	Cleanup() error
}

type PackfileCtor func(HashFactory) (Packfile, error)

type Configuration struct {
	MinSize uint64 `json:"min_size"`
	AvgSize uint64 `json:"avg_size"`
	MaxSize uint64 `json:"max_size"`
}

func NewDefaultConfiguration() *Configuration {
	return &Configuration{
		MaxSize: (20 << 10) << 10,
	}
}
