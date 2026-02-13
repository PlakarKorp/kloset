package caching

import (
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

type ScanCache struct {
	kvcache
}

type ScanBatch struct {
	Batch
}

func newScanCache(cons Constructor, snapshotID [32]byte) (*ScanCache, error) {
	cache, err := cons(CACHE_VERSION, "scan", fmt.Sprintf("%x", snapshotID), DeleteOnClose)
	if err != nil {
		return nil, err
	}

	return &ScanCache{kvcache{cache}}, nil
}

func (c *ScanCache) Copy(string) error {
	panic("Copy should never be used on the ScanCache backend")
}

func (c *ScanCache) NewScanBatch() *ScanBatch {
	return &ScanBatch{c.cache.NewBatch()}
}

func (c *ScanCache) NewBatch() StateBatch {
	return c.NewScanBatch()
}

func (c *ScanCache) PutFile(source int, file string, data []byte) error {
	return c.put("__file__", fmt.Sprintf("%d:%s", source, file), data)
}

func (c *ScanCache) GetFile(source int, file string) ([]byte, error) {
	return c.get("__file__", fmt.Sprintf("%d:%s", source, file))
}

func (c *ScanBatch) PutDirectory(source int, directory string, data []byte) error {
	return c.Put(fmt.Appendf(nil, "__directory__:%d:%s", source, directory), data)
}

func (c *ScanBatch) PutFile(source int, file string, data []byte) error {
	return c.Put(fmt.Appendf(nil, "__file__:%d:%s", source, file), data)
}

func (c *ScanBatch) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.Put(fmt.Appendf(nil, "__delta__:%d:%x:%x", blobType, blobCsum, packfile), data)
}

func (c *ScanCache) PutDirectory(source int, directory string, data []byte) error {
	return c.put("__directory__", fmt.Sprintf("%d:%s", source, directory), data)
}

func (c *ScanCache) GetDirectory(source int, directory string) ([]byte, error) {
	return c.get("__directory__", fmt.Sprintf("%d:%s", source, directory))
}

func (c *ScanCache) PutState(stateID objects.MAC, data []byte) error {
	return c.put("__state__", fmt.Sprintf("%x", stateID), data)
}

func (c *ScanCache) HasState(stateID objects.MAC) (bool, error) {
	panic("HasState should never be used on the ScanCache backend")
}

func (c *ScanCache) GetState(stateID objects.MAC) ([]byte, error) {
	panic("GetState should never be used on the ScanCache backend")
}

func (c *ScanCache) GetStates() (map[objects.MAC][]byte, error) {
	panic("GetStates should never be used on the ScanCache backend")
}

func (c *ScanCache) DelState(stateID objects.MAC) error {
	panic("DelStates should never be used on the ScanCache backend")
}

func (c *ScanCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC(fmt.Sprintf("__delta__:%d:%x:", blobType, blobCsum))
}

func (c *ScanCache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.put("__delta__", fmt.Sprintf("%d:%x:%x", blobType, blobCsum, packfile), data)
}

func (c *ScanCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC(fmt.Sprintf("__delta__:%d:", blobType))
}

func (c *ScanCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC("__delta__:")
}

func (c *ScanCache) DelDelta(blobType resources.Type, blobCsum, packfileMAC objects.MAC) error {
	return c.delete("__delta__", fmt.Sprintf("%d:%x:%x", blobType, blobCsum, packfileMAC))
}

func (c *ScanCache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.put("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *ScanCache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.has("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) GetDeleteds() iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC("__deleted__:")
}

func (c *ScanCache) GetDeletedsByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC(fmt.Sprintf("__deleted__:%d:", blobType))
}

func (c *ScanCache) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	return c.delete("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *ScanCache) PutPackfile(packfile objects.MAC, data []byte) error {
	return c.put("__packfile__", fmt.Sprintf("%x", packfile), data)
}

func (c *ScanCache) HasPackfile(packfile objects.MAC) (bool, error) {
	return c.has("__packfile__", fmt.Sprintf("%x", packfile))
}

func (c *ScanCache) DelPackfile(packfile objects.MAC) error {
	return c.delete("__packfile__", fmt.Sprintf("%x", packfile))
}

func (c *ScanCache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC("__packfile__:")
}

func (c *ScanCache) PutConfiguration(key string, data []byte) error {
	return c.put("__configuration__", key, data)
}

func (c *ScanCache) GetConfiguration(key string) ([]byte, error) {
	return c.get("__configuration__", key)
}

func (c *ScanCache) GetConfigurations() iter.Seq[[]byte] {
	return c.getObjects("__configuration__:")
}

func (c *ScanCache) EnumerateKeysWithPrefix(prefix string, reverse bool) iter.Seq2[string, []byte] {
	l := len(prefix)

	return func(yield func(string, []byte) bool) {
		for key, val := range c.cache.Scan([]byte(prefix), reverse) {
			if !yield(string(key)[l:], val) {
				return
			}
		}
	}
}
