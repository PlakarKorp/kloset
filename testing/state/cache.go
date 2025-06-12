package state

import (
	"errors"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

var unsupported = errors.ErrUnsupported

// cache implements StateCache
type cache struct {
	states map[objects.MAC][]byte
}

func newCache() *cache {
	return &cache{
		states: make(map[objects.MAC][]byte),
	}
}

func (c *cache) PutState(stateID objects.MAC, data []byte) error {
	c.states[stateID] = data
	return nil
}

func (c *cache) HasState(stateID objects.MAC) (bool, error) {
	_, ok := c.states[stateID]
	return ok, nil
}

func (c *cache) GetState(stateID objects.MAC) ([]byte, error) {
	return c.states[stateID], nil
}

func (c *cache) DelState(stateID objects.MAC) error {
	delete(c.states, stateID)
	return nil
}

func (c *cache) GetStates() (map[objects.MAC][]byte, error) {
	return c.states, nil
}

func (c *cache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return unsupported
}

func (c *cache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return nil
}

func (c *cache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return nil
}

func (c *cache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return nil
}

func (c *cache) DelDelta(blobType resources.Type, blobCsum objects.MAC, packfileMAC objects.MAC) error {
	return unsupported
}

func (c *cache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return unsupported
}

func (c *cache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return false, unsupported
}

func (c *cache) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	return unsupported
}

func (c *cache) GetDeletedsByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return nil
}

func (c *cache) GetDeleteds() iter.Seq2[objects.MAC, []byte] {
	return nil
}

func (c *cache) PutPackfile(packfile objects.MAC, data []byte) error {
	return unsupported
}

func (c *cache) DelPackfile(packfile objects.MAC) error {
	return unsupported
}

func (c *cache) HasPackfile(packfile objects.MAC) (bool, error) {
	return false, unsupported
}

func (c *cache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return nil
}

func (c *cache) PutConfiguration(key string, data []byte) error {
	return unsupported
}

func (c *cache) GetConfiguration(key string) ([]byte, error) {
	return nil, unsupported
}

func (c *cache) GetConfigurations() iter.Seq[[]byte] {
	return nil
}
