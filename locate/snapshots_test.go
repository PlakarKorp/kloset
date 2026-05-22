package locate_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/locate"
	"github.com/stretchr/testify/require"
)

// TestParseSnapshotPath tests the pure string-parsing helper.
func TestParseSnapshotPath(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		wantPrefix    string
		wantPattern   string
	}{
		{
			name:        "absolute path — no prefix",
			input:       "/foo/bar",
			wantPrefix:  "",
			wantPattern: "/foo/bar",
		},
		{
			name:        "prefix only — no colon",
			input:       "abc123",
			wantPrefix:  "abc123",
			wantPattern: "",
		},
		{
			name:        "prefix with pattern",
			input:       "abc123:/some/path",
			wantPrefix:  "abc123",
			wantPattern: "/some/path",
		},
		{
			name:        "prefix with multiple colons — only first splits",
			input:       "abc123:/some:path",
			wantPrefix:  "abc123",
			wantPattern: "/some:path",
		},
		{
			name:        "empty string",
			input:       "",
			wantPrefix:  "",
			wantPattern: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPrefix, gotPattern := locate.ParseSnapshotPath(tt.input)
			require.Equal(t, tt.wantPrefix, gotPrefix)
			require.Equal(t, tt.wantPattern, gotPattern)
		})
	}
}
