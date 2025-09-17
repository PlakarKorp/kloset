package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	// Test creating a new manager
	manager := NewManager(pebble.Constructor(tmpDir))
	require.NotNil(t, manager)
	require.Equal(t, tmpDir+"/"+CACHE_VERSION, manager.cacheDir)
	require.NotNil(t, manager.repositoryCache)
	require.NotNil(t, manager.vfsCache)
	require.NotNil(t, manager.maintenanceCache)
}

func TestManagerClose(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)

	// Test closing the manager
	err := manager.Close()
	require.NoError(t, err)

	// Test that subsequent operations return ErrClosed
	repoID := uuid.New()
	_, err = manager.Repository(repoID)
	require.Equal(t, ErrClosed, err)

	_, err = manager.VFS(repoID, "test", "origin", false)
	require.Equal(t, ErrClosed, err)

	_, err = manager.Maintenance(repoID)
	require.Equal(t, ErrClosed, err)
}

func TestManagerRepository(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	repoID := uuid.New()

	// Test getting a new repository cache
	repoCache, err := manager.Repository(repoID)
	require.NoError(t, err)
	require.NotNil(t, repoCache)

	// Test getting the same repository cache again (should return cached instance)
	repoCache2, err := manager.Repository(repoID)
	require.NoError(t, err)
	require.Equal(t, repoCache, repoCache2)
}

func TestManagerVFS(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	repoID := uuid.New()
	scheme := "test"
	origin := "test-origin"

	// Test getting a new VFS cache
	vfsCache, err := manager.VFS(repoID, scheme, origin, false)
	require.NoError(t, err)
	require.NotNil(t, vfsCache)

	// Test getting the same VFS cache again (should return cached instance)
	vfsCache2, err := manager.VFS(repoID, scheme, origin, false)
	require.NoError(t, err)
	require.Equal(t, vfsCache, vfsCache2)
}

func TestManagerMaintenance(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	repoID := uuid.New()

	// Test getting a new maintenance cache
	maintenanceCache, err := manager.Maintenance(repoID)
	require.NoError(t, err)
	require.NotNil(t, maintenanceCache)

	// Test getting the same maintenance cache again (should return cached instance)
	maintenanceCache2, err := manager.Maintenance(repoID)
	require.NoError(t, err)
	require.Equal(t, maintenanceCache, maintenanceCache2)
}

func TestManagerScan(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a test MAC
	mac := [32]byte{1, 2, 3, 4}

	// Test creating a new scan cache
	scanCache, err := manager.Scan(mac)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Clean up
	err = scanCache.Close()
	require.NoError(t, err)
}

func TestManagerCheck(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Test creating a new check cache
	checkCache, err := manager.Check()
	require.NoError(t, err)
	require.NotNil(t, checkCache)

	// Clean up
	err = checkCache.Close()
	require.NoError(t, err)
}

func TestManagerPacking(t *testing.T) {
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Test creating a new packing cache
	packingCache, err := manager.Packing()
	require.NoError(t, err)
	require.NotNil(t, packingCache)

	// Clean up
	err = packingCache.Close()
	require.NoError(t, err)
}
