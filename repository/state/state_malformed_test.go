package state

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// TestConfigurationEntryFromBytesMalformed feeds entries whose declared key,
// value or trailing timestamp run past the end of the buffer. Each must be
// reported as an error naming the field, never a panic: the 11-byte input
// "00000000000" used to index past an empty slice.
func TestConfigurationEntryFromBytesMalformed(t *testing.T) {
	validEntry := ConfigurationEntry{
		Key:       "key",
		Value:     []byte("value"),
		CreatedAt: time.Unix(0, 1234567890),
	}
	valid := validEntry.ToBytes()

	tests := []struct {
		name  string
		input []byte
		field string
	}{
		{
			name:  "fuzzer crash: key length past end",
			input: []byte("00000000000"),
			field: "key",
		},
		{
			name:  "key length is max uint8",
			input: append([]byte{0xFF}, make([]byte, 10)...),
			field: "key",
		},
		{
			name:  "key fills buffer, no value length",
			input: append([]byte{10}, make([]byte, 10)...),
			field: "value length",
		},
		{
			name:  "value length past end",
			input: append([]byte{1, 'k', 0xFF, 0xFF}, make([]byte, 7)...),
			field: "value",
		},
		{
			name:  "value fills buffer, no timestamp",
			input: []byte{1, 'k', 8, 0, 'v', 'v', 'v', 'v', 'v', 'v', 'v', 'v'},
			field: "timestamp",
		},
		{
			name:  "timestamp truncated by one byte",
			input: valid[:len(valid)-1],
			field: "timestamp",
		},
		{
			name:  "shorter than minimum",
			input: []byte{0, 0, 0},
			field: "configuration entry",
		},
		{
			name:  "empty",
			input: nil,
			field: "configuration entry",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := ConfigurationEntryFromBytes(tc.input)
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), tc.field), "error %q should name %q", err, tc.field)
			})
		})
	}

	t.Run("valid entry round-trips", func(t *testing.T) {
		got, err := ConfigurationEntryFromBytes(valid)
		require.NoError(t, err)
		require.Equal(t, validEntry.Key, got.Key)
		require.Equal(t, validEntry.Value, got.Value)
		require.True(t, validEntry.CreatedAt.Equal(got.CreatedAt))
	})

	t.Run("empty key and value round-trip", func(t *testing.T) {
		empty := ConfigurationEntry{CreatedAt: time.Unix(0, 7)}
		got, err := ConfigurationEntryFromBytes(empty.ToBytes())
		require.NoError(t, err)
		require.Equal(t, "", got.Key)
		require.Empty(t, got.Value)
		require.True(t, empty.CreatedAt.Equal(got.CreatedAt))
	})
}

// TestReadHeaderShortRead checks that a header that stops short of the
// 32-byte parent MAC is reported as an unexpected EOF, whether the reader
// hands the bytes over at once or one at a time. A short Read with a nil
// error used to produce the message "failed to read header %!w(<nil>)".
func TestReadHeaderShortRead(t *testing.T) {
	ver := versioning.FromString("1.1.0")
	parent := objects.MAC{0x11, 0x22, 0x33}

	tests := []struct {
		name    string
		rd      io.Reader
		wantErr error
	}{
		{
			name:    "empty",
			rd:      bytes.NewReader(nil),
			wantErr: io.EOF,
		},
		{
			name:    "ten bytes",
			rd:      bytes.NewReader(parent[:10]),
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "one byte short",
			rd:      bytes.NewReader(parent[:len(parent)-1]),
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "ten bytes, one per read",
			rd:      iotest.OneByteReader(bytes.NewReader(parent[:10])),
			wantErr: io.ErrUnexpectedEOF,
		},
		{
			name:    "read error",
			rd:      iotest.ErrReader(errors.New("disk gone")),
			wantErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hdr, err := ReadHeader(tc.rd, ver)
			require.Error(t, err)
			require.Nil(t, hdr)
			require.NotContains(t, err.Error(), "%!w")
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			}
		})
	}

	t.Run("full header, one byte per read", func(t *testing.T) {
		hdr, err := ReadHeader(iotest.OneByteReader(bytes.NewReader(parent[:])), ver)
		require.NoError(t, err)
		require.Equal(t, parent, hdr.Parent)
	})

	t.Run("other versions read nothing", func(t *testing.T) {
		hdr, err := ReadHeader(bytes.NewReader(nil), versioning.FromString("1.0.0"))
		require.NoError(t, err)
		require.Equal(t, objects.MAC{}, hdr.Parent)
	})
}
