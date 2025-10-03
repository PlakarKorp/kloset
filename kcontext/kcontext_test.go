package kcontext

import (
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/encryption/keypair"
	"github.com/PlakarKorp/kloset/logging/testlogger"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestContext_SettersAndGetters(t *testing.T) {
	ctx := NewKContext()

	defaultLogger := testlogger.NewLogger(nil, nil)
	defaultCachingManager := caching.NewManager("/tmp/test_plakar")
	defaultKeyPair, err := keypair.Generate()
	require.NoError(t, err)

	tests := []struct {
		name     string
		setter   func()
		getter   func() interface{}
		expected interface{}
	}{
		{
			name: "SetUsername",
			setter: func() {
				ctx.Username = "testuser"
			},
			getter:   func() interface{} { return ctx.Username },
			expected: "testuser",
		},
		{
			name: "SetHostname",
			setter: func() {
				ctx.Hostname = "testhost"
			},
			getter:   func() interface{} { return ctx.Hostname },
			expected: "testhost",
		},
		{
			name: "SetCommandLine",
			setter: func() {
				ctx.CommandLine = "test command line"
			},
			getter:   func() interface{} { return ctx.CommandLine },
			expected: "test command line",
		},
		{
			name: "SetMachineID",
			setter: func() {
				ctx.MachineID = "machine-123"
			},
			getter:   func() interface{} { return ctx.MachineID },
			expected: "machine-123",
		},
		{
			name: "SetKeyFromFile",
			setter: func() {
				ctx.KeyFromFile = "key123"
			},
			getter:   func() interface{} { return ctx.KeyFromFile },
			expected: "key123",
		},
		{
			name: "SetCacheDir",
			setter: func() {
				ctx.CacheDir = "/cache/testuser"
			},
			getter:   func() interface{} { return ctx.CacheDir },
			expected: "/cache/testuser",
		},
		{
			name: "SetOperatingSystem",
			setter: func() {
				ctx.OperatingSystem = "linux"
			},
			getter:   func() interface{} { return ctx.OperatingSystem },
			expected: "linux",
		},
		{
			name: "SetArchitecture",
			setter: func() {
				ctx.Architecture = "amd64"
			},
			getter:   func() interface{} { return ctx.Architecture },
			expected: "amd64",
		},
		{
			name: "SetProcessID",
			setter: func() {
				ctx.ProcessID = 12345
			},
			getter:   func() interface{} { return ctx.ProcessID },
			expected: 12345,
		},
		{
			name: "SetIdentity",
			setter: func() {
				ctx.Identity = uuid.MustParse("123e4567-e89b-12d3-a456-426614174000")
			},
			getter:   func() interface{} { return ctx.Identity },
			expected: uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
		},
		{
			name: "SetKeypair",
			setter: func() {
				ctx.Keypair = defaultKeyPair
			},
			getter:   func() interface{} { return ctx.Keypair },
			expected: defaultKeyPair,
		},
		{
			name: "SetPlakarClient",
			setter: func() {
				ctx.Client = "plakar-client"
			},
			getter:   func() interface{} { return ctx.Client },
			expected: "plakar-client",
		},
		{
			name: "SetMaxConcurrency",
			setter: func() {
				ctx.MaxConcurrency = 10
			},
			getter:   func() interface{} { return ctx.MaxConcurrency },
			expected: 10,
		},
		{
			name: "SetCache",
			setter: func() {
				ctx.SetCache(defaultCachingManager)
			},
			getter:   func() interface{} { return ctx.GetCache() },
			expected: defaultCachingManager,
		},
		{
			name: "SetLogger",
			setter: func() {
				ctx.SetLogger(defaultLogger)
			},
			getter:   func() interface{} { return ctx.GetLogger() },
			expected: defaultLogger,
		},
		{
			name: "SetCWD",
			setter: func() {
				ctx.CWD = "/current/working/dir"
			},
			getter:   func() interface{} { return ctx.CWD },
			expected: "/current/working/dir",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.setter()
			if result := test.getter(); result != test.expected {
				t.Errorf("%s failed: expected %#v, got %#v", test.name, test.expected, result)
			}
		})
	}
}

func TestAppContextCloseEvents(t *testing.T) {
	ctx := NewKContext()
	events := ctx.Events()
	if events == nil {
		t.Errorf("events is nil")
	}
	ctx.Close()
	// Check if events is closed
	select {
	case <-events.Listen():
		t.Errorf("events is not closed")
	default:
		// events is closed
	}
}
