package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadOrCreate(t *testing.T) {
	// Test creating a new config file
	tmpDir, err := os.MkdirTemp("", "config_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "config.yaml")
	_, err = os.Stat(configFile)
	require.ErrorContains(t, err, "no such file or directory")
	cfg, err := LoadOrCreate(configFile)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, configFile, cfg.pathname)
	require.NotNil(t, cfg.Repositories)
	require.NotNil(t, cfg.Remotes)
	_, err = os.Stat(configFile)
	require.NoError(t, err)
}

func TestLoadOrCreateNonExistentDir(t *testing.T) {
	// Test loading a file in a non-existent directory
	tmpDir, err := os.MkdirTemp("", "config_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "nonexistent", "config.yaml")
	_, err = os.Stat(configFile)
	require.ErrorContains(t, err, "no such file or directory")
	cfg, err := LoadOrCreate(configFile)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
	require.NotNil(t, cfg)
}

func TestConfigSaveAndRender(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "config_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "config.yaml")
	cfg, err := LoadOrCreate(configFile)
	require.NoError(t, err)

	// Test saving config
	cfg.DefaultRepository = "test-repo"
	cfg.Repositories["test-repo"] = RepositoryConfig{"location": "/test/path"}
	cfg.Remotes["test-remote"] = RemoteConfig{"url": "test://url"}

	err = cfg.Save()
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(configFile)
	require.NoError(t, err)

	// Test loading saved config
	cfg2, err := LoadOrCreate(configFile)
	require.NoError(t, err)
	require.Equal(t, "test-repo", cfg2.DefaultRepository)
	require.Equal(t, "/test/path", cfg2.Repositories["test-repo"]["location"])
	require.Equal(t, "test://url", cfg2.Remotes["test-remote"]["url"])
}

func TestHasRepository(t *testing.T) {
	cfg := &Config{
		Repositories: make(map[string]RepositoryConfig),
	}

	// Test non-existent repository
	require.False(t, cfg.HasRepository("test-repo"))

	// Test existing repository
	cfg.Repositories["test-repo"] = RepositoryConfig{"location": "/test/path"}
	require.True(t, cfg.HasRepository("test-repo"))
}

func TestGetRepository(t *testing.T) {
	cfg := &Config{
		Repositories: make(map[string]RepositoryConfig),
	}

	// Test direct path
	repo, err := cfg.GetRepository("/test/path")
	require.NoError(t, err)
	require.Equal(t, "/test/path", repo["location"])

	// Test non-existent repository
	_, err = cfg.GetRepository("@nonexistent")
	require.Error(t, err)

	// Test repository without location
	cfg.Repositories["test-repo"] = RepositoryConfig{"other": "value"}
	_, err = cfg.GetRepository("@test-repo")
	require.Error(t, err)

	// Test valid repository
	cfg.Repositories["test-repo"] = RepositoryConfig{"location": "/test/path"}
	repo, err = cfg.GetRepository("@test-repo")
	require.NoError(t, err)
	require.Equal(t, "/test/path", repo["location"])
}

func TestHasRemote(t *testing.T) {
	cfg := &Config{
		Remotes: make(map[string]RemoteConfig),
	}

	// Test non-existent remote
	require.False(t, cfg.HasRemote("test-remote"))

	// Test existing remote
	cfg.Remotes["test-remote"] = RemoteConfig{"url": "test://url"}
	require.True(t, cfg.HasRemote("test-remote"))
}

func TestGetRemote(t *testing.T) {
	cfg := &Config{
		Remotes: make(map[string]RemoteConfig),
	}

	// Test non-existent remote
	remote, ok := cfg.GetRemote("test-remote")
	require.False(t, ok)
	require.Nil(t, remote)

	// Test existing remote
	cfg.Remotes["test-remote"] = RemoteConfig{"url": "test://url"}
	remote, ok = cfg.GetRemote("test-remote")
	require.True(t, ok)
	require.Equal(t, "test://url", remote["url"])
}
