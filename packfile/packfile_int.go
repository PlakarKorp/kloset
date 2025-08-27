package packfile

import (
	"hash"

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

type Packfile interface {
	AddBlob(t resources.Type, v versioning.Version, mac objects.MAC, data []byte, flags uint32) error
	Size() uint64
	Entries() []Blob
	SerializeData() ([]byte, error)
	SerializeIndex() ([]byte, error)
	SerializeFooter() ([]byte, error)
}

type PackfileCtor func(hash.Hash) Packfile

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
