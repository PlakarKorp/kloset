package locate_test

import (
	"strings"
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/stretchr/testify/require"
)

func TestParseTimeFlag(t *testing.T) {
	t.Run("EmptyTimeFlagReturnsZeroTime", func(t *testing.T) {
		got, err := loc.ParseTimeFlag("")
		require.NoError(t, err)
		require.True(t, got.IsZero())
	})

	t.Run("AbsoluteFormats", func(t *testing.T) {
		tests := []struct {
			name  string
			input string
			want  time.Time
		}{
			{
				name:  "RFC3339",
				input: "2025-08-28T12:34:56Z",
				want:  time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC),
			},
			{
				name:  "date and minute",
				input: "2025-08-28 09:10",
				want:  time.Date(2025, time.August, 28, 9, 10, 0, 0, time.UTC),
			},
			{
				name:  "date and second",
				input: "2025-08-28 09:10:11",
				want:  time.Date(2025, time.August, 28, 9, 10, 11, 0, time.UTC),
			},
			{
				name:  "dash date only",
				input: "2025-08-28",
				want:  time.Date(2025, time.August, 28, 0, 0, 0, 0, time.UTC),
			},
			{
				name:  "slash date only",
				input: "2025/08/28",
				want:  time.Date(2025, time.August, 28, 0, 0, 0, 0, time.UTC),
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				got, err := loc.ParseTimeFlag(tt.input)
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			})
		}
	})

	t.Run("RFC3339TimeFlagWithTimezoneOffset", func(t *testing.T) {
		got, err := loc.ParseTimeFlag("2025-08-28T12:34:56+02:00")
		require.NoError(t, err)

		want := time.Date(2025, time.August, 28, 10, 34, 56, 0, time.UTC)
		require.True(t, got.Equal(want), "got %v, want same instant as %v", got, want)
	})

	t.Run("RelativeDuration", func(t *testing.T) {
		before := time.Now()
		got, err := loc.ParseTimeFlag("2h")
		after := time.Now()
		require.NoError(t, err)

		lower := before.Add(-2 * time.Hour).Add(-250 * time.Millisecond)
		upper := after.Add(-2 * time.Hour).Add(250 * time.Millisecond)

		require.False(t, got.Before(lower), "got %v, want >= %v", got, lower)
		require.False(t, got.After(upper), "got %v, want <= %v", got, upper)
	})

	t.Run("ZeroRelativeDuration", func(t *testing.T) {
		before := time.Now()
		got, err := loc.ParseTimeFlag("0s")
		after := time.Now()
		require.NoError(t, err)

		require.False(t, got.Before(before.Add(-250*time.Millisecond)))
		require.False(t, got.After(after.Add(250*time.Millisecond)))
	})

	t.Run("Check_m_Interpretation", func(t *testing.T) {
		before := time.Now()
		got, err := loc.ParseTimeFlag("1m")
		require.NoError(t, err)

		minAgo := before.Add(-31 * 24 * time.Hour)
		maxAgo := before.Add(-28 * 24 * time.Hour)
		require.False(t, got.Before(minAgo), "got %v, want about one month ago", got)
		require.False(t, got.After(maxAgo), "got %v, want about one month ago", got)
	})

	t.Run("FailsIfTimeFlagHasWhitespaces", func(t *testing.T) {
		got, err := loc.ParseTimeFlag(" 2025-08-28 ")
		require.Error(t, err)
		require.True(t, got.IsZero())
	})

	t.Run("FailsIfInvalidTimeFlag", func(t *testing.T) {
		tests := []string{
			"not-a-time",
			"2025-13-01",
			"2025-02-30",
			"99wibbly",
			"2025/99/01 25:61",
		}

		for _, input := range tests {
			input := input
			t.Run(input, func(t *testing.T) {
				got, err := loc.ParseTimeFlag(input)
				require.Error(t, err)
				require.True(t, got.IsZero())
				require.Contains(t, err.Error(), "invalid time format")
			})
		}
	})
}

// helper: compare times exactly (in UTC) for absolute parses
func mustUTC(t *testing.T, y int, m time.Month, d, hh, mm, ss int) time.Time {
	t.Helper()
	return time.Date(y, m, d, hh, mm, ss, 0, time.UTC)
}

func TestTimeFlag_SetAndString(t *testing.T) {
	var dest time.Time
	tf := loc.NewTimeFlag(&dest)

	// Zero value should stringify to empty
	if s := tf.String(); s != "" {
		t.Fatalf("String on zero dest: got %q want ''", s)
	}

	// Set an RFC3339 value
	in := "2025-08-28T00:00:00Z"
	if err := tf.Set(in); err != nil {
		t.Fatalf("Set(%q) error: %v", in, err)
	}
	want := mustUTC(t, 2025, time.August, 28, 0, 0, 0)
	if !dest.Equal(want) {
		t.Fatalf("dest: got %v want %v", dest, want)
	}

	// After setting, String() should be non-empty and contain the date
	if s := tf.String(); !strings.Contains(s, "2025-08-28") {
		t.Fatalf("String after Set: got %q; expected it to include 2025-08-28", s)
	}
}

func TestTimeFlag_Set_Duration(t *testing.T) {
	var dest time.Time
	tf := loc.NewTimeFlag(&dest)

	before := time.Now()
	if err := tf.Set("30min"); err != nil { // IMPORTANT: use "min", not "m"
		t.Fatalf("Set duration error: %v", err)
	}
	after := time.Now()

	lower := before.Add(-30 * time.Minute).Add(-250 * time.Millisecond)
	upper := after.Add(-30 * time.Minute).Add(250 * time.Millisecond)
	if dest.Before(lower) || dest.After(upper) {
		t.Fatalf("Set(30min): got %v, want in [%v, %v]", dest, lower, upper)
	}
}
