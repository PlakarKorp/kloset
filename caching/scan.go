package caching

import (
	"fmt"
	"iter"
	"strings"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

type ScanCache struct {
	cache Cache
}

func newScanCache(cons CacheConstructor, snapshotID [32]byte) (*ScanCache, error) {
	cache, err := cons(CACHE_VERSION, "scan", fmt.Sprintf("%x", snapshotID), DeleteOnClose)
	if err != nil {
		return nil, err
	}

	return &ScanCache{
		cache: cache,
	}, nil
}

func (c *ScanCache) Close() error {
	return c.cache.Close()
}

func (c *ScanCache) PutFile(source int, file string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__file__:%d:%s", source, file)), data)
}

func (c *ScanCache) GetFile(source int, file string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__file__:%d:%s", source, file)))
}

func (c *ScanCache) PutDirectory(source int, directory string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__directory__:%d:%s", source, directory)), data)
}

func (c *ScanCache) GetDirectory(source int, directory string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__directory__:%d:%s", source, directory)))
}

func (c *ScanCache) PutSummary(source int, pathname string, data []byte) error {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	return c.cache.Put([]byte(fmt.Sprintf("__summary__:%d:%s", source, pathname)), data)
}

func (c *ScanCache) GetSummary(source int, pathname string) ([]byte, error) {
	pathname = strings.TrimSuffix(pathname, "/")
	if pathname == "" {
		pathname = "/"
	}

	return c.cache.Get([]byte(fmt.Sprintf("__summary__:%d:%s", source, pathname)))
}

func (c *ScanCache) PutState(stateID objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__state__:%x", stateID)), data)
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

func (c *ScanCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte(fmt.Sprintf("__delta__:%d:%x:", blobType, blobCsum)), false)
}

func (c *ScanCache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__delta__:%d:%x:%x", blobType, blobCsum, packfile)), data)
}

func (c *ScanCache) GetDeltasByType(blobType resources.Type) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte(fmt.Sprintf("__delta__:%d:", blobType)), false)
}

func (c *ScanCache) GetDeltas() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__delta__:"), false)
}

func (c *ScanCache) DelDelta(blobType resources.Type, blobCsum, packfileMAC objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__delta__:%d:%x:%x", blobType, blobCsum, packfileMAC)))
}

func (c *ScanCache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__deleted__:%d:%x", blobType, blobCsum)), data)
}

func (c *ScanCache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__deleted__:%d:%x", blobType, blobCsum)))
}

func (c *ScanCache) GetDeleteds() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__deleted__:"), false)
}

func (c *ScanCache) GetDeletedsByType(blobType resources.Type) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte(fmt.Sprintf("__deleted__:%d:", blobType)), false)
}

func (c *ScanCache) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__deleted__:%d:%x", blobType, blobCsum)))
}

func (c *ScanCache) PutPackfile(packfile objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__packfile__:%x", packfile)), data)
}

func (c *ScanCache) HasPackfile(packfile objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__packfile__:%x", packfile)))
}

func (c *ScanCache) DelPackfile(packfile objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__packfile__:%x", packfile)))
}

func (c *ScanCache) GetPackfiles() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__packfile__:"), false)
}

func (c *ScanCache) PutConfiguration(key string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__configuration__:%s", key)), data)
}

func (c *ScanCache) GetConfiguration(key string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__configuration__:%s", key)))
}

func (c *ScanCache) GetConfigurations() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__configuration__:"), false)
}

func (c *ScanCache) EnumerateKeysWithPrefix(prefix string, reverse bool) iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for key, val := range c.cache.Scan([]byte(prefix), reverse) {
			if !yield(string(key), val) {
				break
			}
		}

	}
}
