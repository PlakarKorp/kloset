package kcontext

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/config"
	"github.com/PlakarKorp/kloset/cookies"
	"github.com/PlakarKorp/kloset/encryption/keypair"
	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/google/uuid"
)

type KContext struct {
	events  *events.Receiver `msgpack:"-"`
	cache   *caching.Manager `msgpack:"-"`
	cookies *cookies.Manager `msgpack:"-"`
	logger  *logging.Logger  `msgpack:"-"`
	secret  []byte           `msgpack:"-"`
	Config  *config.Config   `msgpack:"-"`

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

func NewKContext() *KContext {
	ctx, cancel := context.WithCancel(context.Background())

	return &KContext{
		events:  events.New(),
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Context: ctx,
		Cancel:  cancel,
	}
}

func NewKContextFrom(template *KContext) *KContext {
	ctx := *template
	ctx.events = events.New()
	ctx.Context, ctx.Cancel = context.WithCancel(template.Context)
	return &ctx
}

func (c *KContext) Deadline() (time.Time, bool) {
	return c.Context.Deadline()
}

func (c *KContext) Done() <-chan struct{} {
	return c.Context.Done()
}

func (c *KContext) Err() error {
	return c.Context.Err()
}

func (c *KContext) Value(key any) any {
	return c.Context.Value(key)
}

func (c *KContext) Close() {
	c.events.Close()
	c.Cancel()
}

func (c *KContext) Events() *events.Receiver {
	return c.events
}

func (c *KContext) SetCache(cacheManager *caching.Manager) {
	c.cache = cacheManager
}

func (c *KContext) GetCache() *caching.Manager {
	return c.cache
}

func (c *KContext) SetCookies(cacheManager *cookies.Manager) {
	c.cookies = cacheManager
}

func (c *KContext) GetCookies() *cookies.Manager {
	return c.cookies
}

func (c *KContext) SetLogger(logger *logging.Logger) {
	c.logger = logger
}

func (c *KContext) GetLogger() *logging.Logger {
	return c.logger
}

func (c *KContext) SetSecret(secret []byte) {
	c.secret = secret
}

func (c *KContext) GetSecret() []byte {
	return c.secret
}

func (c *KContext) GetAuthToken(repository uuid.UUID) (string, error) {
	if authToken, err := c.cookies.GetAuthToken(); err != nil {
		return "", err
	} else {
		return authToken, nil
	}
}
