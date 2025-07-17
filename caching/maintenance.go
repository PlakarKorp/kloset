package caching

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"
	"log"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type MaintenanceCache struct {
	cache Cache
}

func newMaintenanceCache(cons CacheConstructor, repositoryID uuid.UUID) (*MaintenanceCache, error) {
	cache, err := cons(CACHE_VERSION, "maintenance", repositoryID.String(), None)
	if err != nil {
		return nil, err
	}

	return &MaintenanceCache{
		cache: cache,
	}, nil
}

func (c *MaintenanceCache) PutSnapshot(snapshotID objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__snapshot__:%x", snapshotID)), data)
}

func (c *MaintenanceCache) HasSnapshot(snapshotID objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__snapshot__:%x", snapshotID)))
}

func (c *MaintenanceCache) DeleteSnapshot(snapshotID objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__snapshot__:%x", snapshotID)))
}

func (c *MaintenanceCache) PutPackfile(snapshotID, packfileMAC objects.MAC) error {
	log.Printf("putting packfile %x in snap %x", packfileMAC, snapshotID)
	return c.cache.Put([]byte(fmt.Sprintf("__packfile__:%x:%x", packfileMAC, snapshotID)), packfileMAC[:])
}

func (c *MaintenanceCache) HasPackfile(packfileMAC objects.MAC) bool {
	log.Printf("checking packfile %x (%s)", packfileMAC, packfileMAC[:])
	for range c.cache.Scan([]byte(fmt.Sprintf("__packfile__:%x:", packfileMAC)), false) {
		return true
	}

	return false
}

func (c *MaintenanceCache) GetPackfiles(snapshotID objects.MAC) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__packfile__:"), false)
}

func (c *MaintenanceCache) DeleletePackfiles(snapshotID objects.MAC) error {
	for key, _ := range c.cache.Scan([]byte("__packfile__:"), false) {
		hex_mac := string(key[bytes.LastIndexByte(key, byte(':'))+1:])
		mac, err := hex.DecodeString(hex_mac)
		if err != nil {
			return err
		}

		if objects.MAC(mac) == snapshotID {
			err := c.cache.Delete(key)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *MaintenanceCache) Close() error {
	return c.cache.Close()
}
