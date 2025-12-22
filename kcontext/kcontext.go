package kcontext

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/config"
	"github.com/PlakarKorp/kloset/encryption/keypair"
	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/logging"
	"github.com/google/uuid"
)

type KContext struct {
	events *events.EventsBUS `msgpack:"-"`

	cache  *caching.Manager `msgpack:"-"`
	logger *logging.Logger  `msgpack:"-"`
	Config *config.Config   `msgpack:"-"`

	Context context.Context         `msgpack:"-"`
	Cancel  context.CancelCauseFunc `msgpack:"-"`

	Stdin  io.Reader `msgpack:"-"`
	Stdout io.Writer `msgpack:"-"`
	Stderr io.Writer `msgpack:"-"`

	Quiet  bool
	Silent bool

	Username    string
	Hostname    string
	CommandLine string
	MachineID   string
	KeyFromFile string
	CacheDir    string

	OperatingSystem string
	Architecture    string
	ProcessID       int

	Client string

	CWD            string
	MaxConcurrency int

	Identity uuid.UUID
	Keypair  *keypair.KeyPair

	//storeIOTracker      *iostat.IOTracker
	//repositoryIOTracker *iostat.IOTracker
}

func NewKContext() *KContext {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &KContext{
		events:  events.NewEventsBUS(0),
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Context: ctx,
		Cancel:  cancel,
	}
}

func NewKContextFrom(template *KContext) *KContext {
	ctx := *template
	ctx.events = events.NewEventsBUS(0)
	ctx.Context, ctx.Cancel = context.WithCancelCause(template.Context)
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
	c.Cancel(nil)
}

func (c *KContext) Events() *events.EventsBUS {
	return c.events
}

func (c *KContext) SetCache(cacheManager *caching.Manager) {
	c.cache = cacheManager
}

func (c *KContext) GetCache() *caching.Manager {
	return c.cache
}

func (c *KContext) SetLogger(logger *logging.Logger) {
	c.logger = logger
}

func (c *KContext) GetLogger() *logging.Logger {
	return c.logger
}
