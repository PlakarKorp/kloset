package packer

import (
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
)

func init() {
	// used for padding random bytes
	versioning.Register(resources.RT_RANDOM, versioning.FromString("1.0.0"))
}

type PackerManagerInt interface {
	Run() error
	Wait()
	InsertIfNotPresent(Type resources.Type, mac objects.MAC) (bool, error)
	Put(hint int, Type resources.Type, mac objects.MAC, data []byte) error
	Exists(Type resources.Type, mac objects.MAC) (bool, error)
}

type PackerMsg struct {
	Timestamp time.Time
	Type      resources.Type
	Version   versioning.Version
	MAC       objects.MAC
	Data      []byte
	Flags     uint32
}
