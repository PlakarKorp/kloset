package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"maps"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Pathname          string
	DefaultRepository string                      `yaml:"default-repo"`
	Repositories      map[string]RepositoryConfig `yaml:"repositories"`
	Remotes           map[string]RemoteConfig     `yaml:"remotes"`
}

type RepositoryConfig map[string]string
type RemoteConfig map[string]string

func LoadOrCreate(configFile string) (*Config, error) {
	f, err := os.Open(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			cfg := &Config{
				Pathname:     configFile,
				Repositories: make(map[string]RepositoryConfig),
				Remotes:      make(map[string]RemoteConfig),
			}
			return cfg, cfg.Save()
		}
		return nil, fmt.Errorf("error reading config file: %T", err)
	}
	defer f.Close()
	var config Config
	if err := yaml.NewDecoder(f).Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	config.Pathname = configFile
	if config.Repositories == nil {
		config.Repositories = make(map[string]RepositoryConfig)
	}
	if config.Remotes == nil {
		config.Remotes = make(map[string]RemoteConfig)
	}
	return &config, nil
}

func (c *Config) Render(w io.Writer) error {
	return yaml.NewEncoder(w).Encode(c)
}

func (c *Config) Save() error {
	dir := filepath.Dir(c.Pathname)
	tmpFile, err := os.CreateTemp(dir, "config.*.yaml")
	if err != nil {
		return err
	}

	err = c.Render(tmpFile)
	tmpFile.Close()
	if err != nil {
		os.Remove(tmpFile.Name())
		return err
	}
	return os.Rename(tmpFile.Name(), c.Pathname)
}

func (c *Config) HasRepository(name string) bool {
	_, ok := c.Repositories[name]
	return ok
}

func (c *Config) GetRepository(name string) (map[string]string, error) {
	if !strings.HasPrefix(name, "@") {
		return map[string]string{"location": name}, nil
	}

	kv, ok := c.Repositories[name[1:]]
	if !ok {
		return nil, fmt.Errorf("could not resolve repository: %s", name)
	}
	if _, ok := kv["location"]; !ok {
		return nil, fmt.Errorf("repository %s has no location", name)
	} else {
		res := make(map[string]string)
		maps.Copy(res, kv)
		return res, nil
	}
}

func (c *Config) HasRemote(name string) bool {
	_, ok := c.Remotes[name]
	return ok
}

func (c *Config) GetRemote(name string) (map[string]string, bool) {
	if kv, ok := c.Remotes[name]; !ok {
		return nil, false
	} else {
		res := make(map[string]string)
		maps.Copy(res, kv)
		return res, ok
	}
}
