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
	t.Run("LookupRegisteredProtocolWithDoubleSlash", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("http", "http-value", location.FLAG_LOCALFS)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("http://example.com")
		require.Equal(t, "http", proto)
		require.Equal(t, "example.com", locationValue)
		require.Equal(t, "http-value", item)
		require.Equal(t, location.FLAG_LOCALFS, flags)
		require.True(t, found)
	})

	t.Run("ReturnUnknownProtocolWhenProtocolIsNotRegistered", func(t *testing.T) {
		loc := location.New[string]("default")

		proto, locationValue, item, flags, found := loc.Lookup("ftp://example.com")
		require.Equal(t, "ftp", proto)
		require.Equal(t, "example.com", locationValue)
		require.Empty(t, item)
		require.Zero(t, flags)
		require.False(t, found)
	})

	t.Run("UseFallbackWhenProtocolIsMissing", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("default", "default-value", location.FLAG_FILE)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("example.com")
		require.Equal(t, "default", proto)
		require.Equal(t, "example.com", locationValue)
		require.Equal(t, "default-value", item)
		require.Equal(t, location.FLAG_FILE, flags)
		require.True(t, found)
	})

	t.Run("UseFallbackForWindowsAbsolutePath", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("default", "default-value", location.FLAG_LOCALFS)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("C:\\Users\\Plakup")
		require.Equal(t, "default", proto)
		require.Equal(t, "C:\\Users\\Plakup", locationValue)
		require.Equal(t, "default-value", item)
		require.Equal(t, location.FLAG_LOCALFS, flags)
		require.True(t, found)
	})

	t.Run("LookupRegisteredProtocolWithoutDoubleSlash", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("http", "http-value", location.FLAG_STREAM)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("http:example.com")
		require.Equal(t, "http", proto)
		require.Equal(t, "example.com", locationValue)
		require.Equal(t, "http-value", item)
		require.Equal(t, location.FLAG_STREAM, flags)
		require.True(t, found)
	})

	t.Run("UseFallbackForEmptyString", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("default", "default-value", location.FLAG_FILE)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("")
		require.Equal(t, "default", proto)
		require.Equal(t, "", locationValue)
		require.Equal(t, "default-value", item)
		require.Equal(t, location.FLAG_FILE, flags)
		require.True(t, found)
	})

	t.Run("DoNotTreatOneCharacterPrefixAsProtocol", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("default", "default-value", location.FLAG_LOCALFS)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("a:/tmp/file")
		require.Equal(t, "default", proto)
		require.Equal(t, "a:/tmp/file", locationValue)
		require.Equal(t, "default-value", item)
		require.Equal(t, location.FLAG_LOCALFS, flags)
		require.True(t, found)
	})

	t.Run("TrimLeadingDoubleSlashFromLocation", func(t *testing.T) {
		loc := location.New[string]("default")
		ok := loc.Register("fs", "fs-value", location.FLAG_LOCALFS)
		require.True(t, ok)

		proto, locationValue, item, flags, found := loc.Lookup("fs:///tmp/file")
		require.Equal(t, "fs", proto)
		require.Equal(t, "/tmp/file", locationValue)
		require.Equal(t, "fs-value", item)
		require.Equal(t, location.FLAG_LOCALFS, flags)
		require.True(t, found)
	})
}
