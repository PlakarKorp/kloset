package appcontext

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/config"
	"github.com/PlakarKorp/kloset/cookies"
	"github.com/PlakarKorp/kloset/encryption/keypair"
	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/google/uuid"
)

type AppContext struct {
	events    *events.Receiver `msgpack:"-"`
	cache     *caching.Manager `msgpack:"-"`
	cookies   *cookies.Manager `msgpack:"-"`
	logger    *logging.Logger  `msgpack:"-"`
	secret    []byte           `msgpack:"-"`
	configMtx *sync.Mutex      `msgpack:"-"`
	config    *config.Config   `msgpack:"-"`

	Context context.Context    `msgpack:"-"`
	Cancel  context.CancelFunc `msgpack:"-"`

	Stdout io.Writer `msgpack:"-"`
	Stderr io.Writer `msgpack:"-"`

	NumCPU      int
	Username    string
	HomeDir     string
	Hostname    string
	CommandLine string
	MachineID   string
	KeyFromFile string
	CookiesDir  string
	CacheDir    string
	KeyringDir  string

	OperatingSystem string
	Architecture    string
	ProcessID       int

	Client string

	CWD            string
	MaxConcurrency int

	Identity uuid.UUID
	Keypair  *keypair.KeyPair
}

func NewAppContext() *AppContext {
	ctx, cancel := context.WithCancel(context.Background())

	return &AppContext{
		events:    events.New(),
		configMtx: &sync.Mutex{},
		Stdout:    os.Stdout,
		Stderr:    os.Stderr,
		Context:   ctx,
		Cancel:    cancel,
	}
}

func NewAppContextFrom(template *AppContext) *AppContext {
	ctx := *template
	ctx.events = events.New()
	ctx.configMtx = &sync.Mutex{}
	ctx.Context, ctx.Cancel = context.WithCancel(template.Context)
	return &ctx
}

func (c *AppContext) Deadline() (time.Time, bool) {
	return c.Context.Deadline()
}

func (c *AppContext) Done() <-chan struct{} {
	return c.Context.Done()
}

func (c *AppContext) Err() error {
	return c.Context.Err()
}

func (c *AppContext) Value(key any) any {
	return c.Context.Value(key)
}

func (c *AppContext) Close() {
	c.events.Close()
	c.Cancel()
}

func (c *AppContext) Events() *events.Receiver {
	return c.events
}

func (c *AppContext) SetCache(cacheManager *caching.Manager) {
	c.cache = cacheManager
}

func (c *AppContext) GetCache() *caching.Manager {
	return c.cache
}

func (c *AppContext) SetCookies(cacheManager *cookies.Manager) {
	c.cookies = cacheManager
}

func (c *AppContext) GetCookies() *cookies.Manager {
	return c.cookies
}

func (c *AppContext) SetLogger(logger *logging.Logger) {
	c.logger = logger
}

func (c *AppContext) GetLogger() *logging.Logger {
	return c.logger
}

func (c *AppContext) SetSecret(secret []byte) {
	c.secret = secret
}

func (c *AppContext) GetSecret() []byte {
	return c.secret
}

func (c *AppContext) GetAuthToken(repository uuid.UUID) (string, error) {
	if authToken, err := c.cookies.GetAuthToken(); err != nil {
		return "", err
	} else {
		return authToken, nil
	}
}

func (c *AppContext) SetConfig(cfg *config.Config) {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()
	c.config = cfg
}

// All methods after this point are simple boilerplate around config, this is a
// transition until we get appcontext out of kloset and into plakar
// You don't want to use the config methods directly because you need this to
// be locked as the pointer can be changed by another thread.
func (c *AppContext) RenderConfig(w io.Writer) error {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	return c.config.Render(w)
}

func (c *AppContext) SaveConfig() error {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	return c.config.Save()
}

func (c *AppContext) HasRepositoryConfig(name string) bool {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	return c.config.HasRepository(name)
}

// GetRepositoryConfig returns a copy of the config for the `name` repository,
// meaning once you get it it's safe to rely on it and it won't get changed
// under your feet. For long lived task most of the time what you want is to
// resolve the config at the start and then use this copy so that you are not
// affected by possible config reloads and changes.
func (c *AppContext) GetRepositoryConfig(name string) (map[string]string, error) {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()
	return c.config.GetRepository(name)
}

func (c *AppContext) HasRemoteConfig(name string) bool {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()
	return c.config.HasRemote(name)
}

func (c *AppContext) GetRemoteConfig(name string) (map[string]string, bool) {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()
	return c.config.GetRemote(name)
}

func (c *AppContext) CreateRemoteConfig(name string) error {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	if c.config.HasRemote(name) {
		return fmt.Errorf("remote %q doesn't exist", name)
	}

	c.config.Remotes[name] = make(map[string]string)
	return c.config.Save()
}

// Set an option with value, or unset it if value is an empty string
// Returns an error if the Remote `name` doesn't exist
func (c *AppContext) SetRemoteConfig(name, option, value string) error {
	if !c.config.HasRemote(name) {
		return fmt.Errorf("remote %q doesn't exist", name)
	}

	if value == "" {
		delete(c.config.Remotes[name], option)
	} else {
		c.config.Remotes[name][option] = value
	}

	return c.config.Save()
}

func (c *AppContext) CreateRepositoryConfig(name string) error {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	if c.config.HasRepository(name) {
		return fmt.Errorf("repository %q doesn't exist", name)
	}

	c.config.Repositories[name] = make(map[string]string)
	return c.config.Save()
}

func (c *AppContext) SetRepositoryConfig(name, option, value string) error {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	if !c.config.HasRepository(name) {
		return fmt.Errorf("repository %q doesn't exist", name)
	}

	if value == "" {
		delete(c.config.Repositories[name], option)
	} else {
		c.config.Repositories[name][option] = value
	}

	return c.config.Save()
}

func (c *AppContext) SetDefaultRepositoryConfig(name string) error {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	if !c.config.HasRepository(name) {
		return fmt.Errorf("repository %q doesn't exist", name)
	}

	if _, ok := c.config.Repositories[name]["location"]; !ok {
		return fmt.Errorf("repository %q doesn't have a location set", name)
	}

	c.config.DefaultRepository = name

	return c.config.Save()
}

func (c *AppContext) GetDefaultRepositoryName() string {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	return c.config.DefaultRepository
}

func (c *AppContext) GetConfigPath() string {
	c.configMtx.Lock()
	defer c.configMtx.Unlock()

	return c.config.Pathname
}
