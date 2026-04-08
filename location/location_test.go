package location_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/location"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("InitializeEmptyLocation", func(t *testing.T) {
		loc := location.New[string]("default")
		require.NotNil(t, loc)
		require.Empty(t, loc.Names())
	})
}

func TestParseFlag(t *testing.T) {
	tests := []struct {
		input    string
		wantFlag location.Flags
		wantErr  error
	}{
		{
			input:    "localfs",
			wantFlag: location.FLAG_LOCALFS,
		},
		{
			input:    "file",
			wantFlag: location.FLAG_FILE,
		},
		{
			input:    "stream",
			wantFlag: location.FLAG_STREAM,
		},
		{
			input:    "needack",
			wantFlag: location.FLAG_NEEDACK,
		},
		{
			input:    "nomerge",
			wantFlag: location.FLAG_NOMERGE,
		},
		{
			input:    "unknown",
			wantFlag: 0,
			wantErr:  location.ErrUnknownFlag,
		},
	}

	for _, tt := range tests {
		t.Run("_"+tt.input, func(t *testing.T) {
			flag, err := location.ParseFlag(tt.input)

			if tt.input == "unknown" {
				require.ErrorIs(t, err, tt.wantErr)
				require.Zero(t, flag)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantFlag, flag)
		})
	}
}

func TestRegister(t *testing.T) {
	t.Run("NewItem", func(t *testing.T) {
		loc := location.New[string]("default")

		ok := loc.Register("test", "value", location.FLAG_LOCALFS)
		require.True(t, ok)
	})

	t.Run("FailsIfDuplicateName", func(t *testing.T) {
		loc := location.New[string]("default")

		ok := loc.Register("test", "value", location.FLAG_LOCALFS)
		require.True(t, ok)

		ok = loc.Register("test", "other-value", location.FLAG_FILE)
		require.False(t, ok)
	})
}

func TestUnregister(t *testing.T) {
	t.Run("ExistingItem", func(t *testing.T) {
		loc := location.New[string]("default")

		ok := loc.Register("test", "value", 0)
		require.True(t, ok)

		ok = loc.Unregister("test")
		require.True(t, ok)
	})

	t.Run("FailsIfItemDoesNotExist", func(t *testing.T) {
		loc := location.New[string]("default")

		ok := loc.Unregister("missing")
		require.False(t, ok)
	})

	t.Run("Register ->Unregister->Register", func(t *testing.T) {
		loc := location.New[string]("default")

		ok := loc.Register("test", "value", 0)
		require.True(t, ok)

		ok = loc.Unregister("test")
		require.True(t, ok)

		ok = loc.Register("test", "other-value", 0)
		require.True(t, ok)
	})
}

func TestNames(t *testing.T) {
	t.Run("EmptySliceWhenNoItemsAreRegistered", func(t *testing.T) {
		loc := location.New[string]("default")

		names := loc.Names()
		require.Empty(t, names)
	})

	t.Run("ReturnSortedRegisteredNames", func(t *testing.T) {
		loc := location.New[string]("default")

		require.True(t, loc.Register("c", "value-c", 0))
		require.True(t, loc.Register("a", "value-a", 0))
		require.True(t, loc.Register("b", "value-b", 0))

		names := loc.Names()
		require.Equal(t, []string{"a", "b", "c"}, names)
	})

	t.Run("DoNotReturnUnregisteredNames", func(t *testing.T) {
		loc := location.New[string]("default")

		require.True(t, loc.Register("a", "value-a", 0))
		require.True(t, loc.Register("b", "value-b", 0))
		require.True(t, loc.Unregister("a"))

		names := loc.Names()
		require.Equal(t, []string{"b"}, names)
	})
}

func TestLookup(t *testing.T) {
	loc := location.New[string]("default")
	loc.Register("http", "http-value", 0)
	loc.Register("https", "https-value", 0)

	tests := []struct {
		name         string
		uri          string
		wantProto    string
		wantLocation string
		wantValue    string
		wantFound    bool
	}{
		{
			name:         "simple http",
			uri:          "http://example.com",
			wantProto:    "http",
			wantLocation: "example.com",
			wantValue:    "http-value",
			wantFound:    true,
		},
		{
			name:         "simple https",
			uri:          "https://example.com",
			wantProto:    "https",
			wantLocation: "example.com",
			wantValue:    "https-value",
			wantFound:    true,
		},
		{
			name:         "unknown protocol",
			uri:          "ftp://example.com",
			wantProto:    "ftp",
			wantLocation: "example.com",
			wantValue:    "",
			wantFound:    false,
		},
		{
			name:         "no protocol",
			uri:          "example.com",
			wantProto:    "default",
			wantLocation: "example.com",
			wantValue:    "",
			wantFound:    false,
		},
		{
			name:         "windows absolute path",
			uri:          "C:\\Users\\Plakup",
			wantProto:    "default",
			wantLocation: "C:\\Users\\Plakup",
			wantValue:    "",
			wantFound:    false,
		},
		{
			name:         "empty string",
			uri:          "",
			wantProto:    "default",
			wantLocation: "",
			wantValue:    "",
			wantFound:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto, location, value, _, found := loc.Lookup(tt.uri)
			if proto != tt.wantProto {
				t.Errorf("Lookup() proto = %v, want %v", proto, tt.wantProto)
			}
			if location != tt.wantLocation {
				t.Errorf("Lookup() location = %v, want %v", location, tt.wantLocation)
			}
			if value != tt.wantValue {
				t.Errorf("Lookup() value = %v, want %v", value, tt.wantValue)
			}
			if found != tt.wantFound {
				t.Errorf("Lookup() found = %v, want %v", found, tt.wantFound)
			}
		})
	}
}
