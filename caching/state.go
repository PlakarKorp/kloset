package caching

import (
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

type StateCache interface {
	PutState(stateID objects.MAC, data []byte) error
	HasState(stateID objects.MAC) (bool, error)
	GetState(stateID objects.MAC) ([]byte, error)
	DelState(stateID objects.MAC) error
	GetStates() (map[objects.MAC][]byte, error)

	PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error
	GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte]
	GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte]
	GetDeltas() iter.Seq2[objects.MAC, []byte]
	DelDelta(blobType resources.Type, blobCsum objects.MAC, packfileMAC objects.MAC) error

	PutColoured(blobType resources.Type, blobCsum objects.MAC, data []byte) error
	HasColoured(blobType resources.Type, blobCsum objects.MAC) (bool, error)
	DelColoured(blobType resources.Type, blobCsum objects.MAC) error
	GetColouredEntriesByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte]
	GetColouredEntries() iter.Seq2[objects.MAC, []byte]

	PutPackfile(packfile objects.MAC, data []byte) error
	DelPackfile(packfile objects.MAC) error
	HasPackfile(packfile objects.MAC) (bool, error)
	GetPackfiles() iter.Seq2[objects.MAC, []byte]

	PutConfiguration(key string, data []byte) error
	GetConfiguration(key string) ([]byte, error)
	GetConfigurations() iter.Seq[[]byte]

	NewBatch() StateBatch
}

type StateBatch interface {
	Batch
	PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error
}
