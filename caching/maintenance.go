package caching

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type MaintenanceCache struct {
	kvcache
}

func newMaintenanceCache(cons Constructor, repositoryID uuid.UUID) (*MaintenanceCache, error) {
	cache, err := cons(CACHE_VERSION, "maintenance", repositoryID.String(), None)
	if err != nil {
		return nil, err
	}

	return &MaintenanceCache{kvcache{cache}}, nil
}

func (c *MaintenanceCache) PutSnapshot(snapshotID objects.MAC, data []byte) error {
	return c.put("__snapshot__", fmt.Sprintf("%x", snapshotID), data)
}

func (c *MaintenanceCache) HasSnapshot(snapshotID objects.MAC) (bool, error) {
	return c.has("__snapshot__", fmt.Sprintf("%x", snapshotID))
}

func (c *MaintenanceCache) DeleteSnapshot(snapshotID objects.MAC) error {
	return c.delete("__snapshot__", fmt.Sprintf("%x", snapshotID))
}

func (c *MaintenanceCache) PutPackfile(snapshotID, packfileMAC objects.MAC) error {
	return c.put("__packfile__", fmt.Sprintf("%x:%x", packfileMAC, snapshotID), packfileMAC[:])
}

func (c *MaintenanceCache) HasPackfile(packfileMAC objects.MAC) bool {
	for range c.getObjects(fmt.Sprintf("__packfile__:%x:", packfileMAC)) {
		return true
	}

	return false
}

func (c *MaintenanceCache) GetPackfiles(snapshotID objects.MAC) iter.Seq[objects.MAC] {
	return func(yield func(objects.MAC) bool) {
		for p := range c.getObjects("__packfile__:") {
			if !yield(objects.MAC(p)) {
				return
			}
		}
	}
}

func (c *MaintenanceCache) DeleletePackfiles(snapshotID objects.MAC) error {
	for key, _ := range c.cache.Scan(nil, false) {
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
