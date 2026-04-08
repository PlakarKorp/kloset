package locate_test

import (
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/stretchr/testify/require"
)

var dateRef = time.Date(2023, time.March, 15, 13, 45, 59, 0, time.UTC)

// --- helpers ---

func mustParse(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tt.UTC()
}

// --- standard periods ---

func TestDays(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Days.Key(dateRef)
		require.Equal(t, "2023-03-15", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Days.Start(dateRef)
		want := time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Days.Start(dateRef)
		got := loc.Days.Prev(start)
		want := time.Date(2023, 3, 14, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Days.Start(input)
		want := time.Date(2023, 7, 5, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestMinutes(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Minutes.Key(dateRef)
		require.Equal(t, "2023-03-15-13:45", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Minutes.Start(dateRef)
		want := time.Date(2023, 3, 15, 13, 45, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Minutes.Start(dateRef)
		got := loc.Minutes.Prev(start)
		want := time.Date(2023, 3, 15, 13, 44, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Minutes.Start(input)
		want := time.Date(2023, 7, 5, 3, 59, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestHours(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Hours.Key(dateRef)
		require.Equal(t, "2023-03-15-13", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Hours.Start(dateRef)
		want := time.Date(2023, 3, 15, 13, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Hours.Start(dateRef)
		got := loc.Hours.Prev(start)
		want := time.Date(2023, 3, 15, 12, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Hours.Start(input)
		want := time.Date(2023, 7, 5, 3, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestMonths(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Months.Key(dateRef)
		require.Equal(t, "2023-03", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Months.Start(dateRef)
		want := time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Months.Start(dateRef)
		got := loc.Months.Prev(start)
		want := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Months.Start(input)
		want := time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestYears(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Years.Key(dateRef)
		require.Equal(t, "2023", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Years.Start(dateRef)
		want := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Years.Start(dateRef)
		got := loc.Years.Prev(start)
		want := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 12, 31, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Years.Start(input)
		want := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestWeeks(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		t.Run("returns ISO week for a regular date", func(t *testing.T) {
			got := loc.Weeks.Key(dateRef)
			require.Equal(t, "2023-W11", got)
		})

		t.Run("handles ISO year boundary", func(t *testing.T) {
			input := time.Date(2021, 1, 1, 10, 0, 0, 0, time.UTC)
			got := loc.Weeks.Key(input)
			require.Equal(t, "2020-W53", got)
		})
	})

	t.Run("Start", func(t *testing.T) {
		t.Run("returns monday at midnight for a weekday", func(t *testing.T) {
			got := loc.Weeks.Start(dateRef)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("returns previous monday for a sunday", func(t *testing.T) {
			input := time.Date(2023, 3, 19, 23, 59, 59, 0, time.UTC)
			got := loc.Weeks.Start(input)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("normalizes input from another timezone", func(t *testing.T) {
			input := time.Date(2023, 7, 9, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
			got := loc.Weeks.Start(input)
			want := time.Date(2023, 7, 10, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Weeks.Start(dateRef)
		got := loc.Weeks.Prev(start)
		want := time.Date(2023, 3, 6, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestWeekdayPeriod(t *testing.T) {
	t.Run("Name", func(t *testing.T) {
		got := loc.WeekdayPeriod(time.Monday)
		require.Equal(t, "monday", got.Name)
	})

	t.Run("Start", func(t *testing.T) {
		t.Run("aligns to monday of the same ISO week", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			got := monday.Start(dateRef)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("aligns to thursday of the same ISO week", func(t *testing.T) {
			thursday := loc.WeekdayPeriod(time.Thursday)
			got := thursday.Start(dateRef)
			want := time.Date(2023, 3, 16, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("normalizes input from another timezone", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			input := time.Date(2023, 7, 9, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
			got := monday.Start(input)
			want := time.Date(2023, 7, 10, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("handles sunday input when aligning to monday", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			input := time.Date(2023, 3, 19, 23, 59, 59, 0, time.UTC) // sunday

			got := monday.Start(input)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)

			require.Equal(t, want, got)
		})
	})

	t.Run("Prev", func(t *testing.T) {
		monday := loc.WeekdayPeriod(time.Monday)
		start := monday.Start(dateRef)
		got := monday.Prev(start)
		want := time.Date(2023, 3, 6, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Key", func(t *testing.T) {
		t.Run("returns ISO week key for aligned monday", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			start := monday.Start(dateRef)
			got := monday.Key(start)
			require.Equal(t, "2023-W11-monday", got)
		})

		t.Run("handles ISO year boundary for aligned thursday", func(t *testing.T) {
			thursday := loc.WeekdayPeriod(time.Thursday)
			input := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			start := thursday.Start(input)
			got := thursday.Key(start)
			require.Equal(t, "2026-W01-thursday", got)
		})
	})
}

func TestLastNKeys_Weekday_Monday(t *testing.T) {
	// Anchor on a Wednesday; the aligned Monday is 2025-08-25
	now := mustParse(t, "2025-08-27T12:00:00Z")
	keys := loc.Mondays.LastNKeys(now, 3)

	want := map[string]struct{}{
		"2025-W35-monday": {},
		"2025-W34-monday": {},
		"2025-W33-monday": {},
	}
	if len(keys) != len(want) {
		t.Fatalf("LastNKeys Monday len: got %d want %d", len(keys), len(want))
	}
	for k := range want {
		if _, ok := keys[k]; !ok {
			t.Fatalf("LastNKeys Monday missing key %q", k)
		}
	}
}

// --- general invariants ---

func TestStartAlwaysUTC(t *testing.T) {
	// Ensure Start normalizes to UTC regardless of input zone.
	ny, _ := time.LoadLocation("America/New_York")
	localTime := time.Date(2023, 7, 4, 23, 59, 59, 0, ny)

	tests := []struct {
		name   string
		p      loc.Period
		expect time.Time
	}{
		{"minute", loc.Minutes, loc.Minutes.Start(localTime.In(time.UTC))},
		{"hour", loc.Hours, loc.Hours.Start(localTime.In(time.UTC))},
		{"day", loc.Days, loc.Days.Start(localTime.In(time.UTC))},
		{"week", loc.Weeks, loc.Weeks.Start(localTime.In(time.UTC))},
		{"month", loc.Months, loc.Months.Start(localTime.In(time.UTC))},
		{"year", loc.Years, loc.Years.Start(localTime.In(time.UTC))},
		{"monday", loc.Mondays, loc.Mondays.Start(localTime.In(time.UTC))},
		{"thursday", loc.Thursdays, loc.Thursdays.Start(localTime.In(time.UTC))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.p.Start(localTime)
			if !got.Equal(tc.expect) || got.Location() != time.UTC {
				t.Fatalf("%s.Start not UTC or mismatch: got %v (loc=%v) want %v (loc=UTC)",
					tc.name, got, got.Location(), tc.expect)
			}
		})
	}
}

func TestPrevIsMonotonicBackwards(t *testing.T) {
	periods := []loc.Period{loc.Minutes, loc.Hours, loc.Days, loc.Weeks, loc.Months, loc.Years, loc.Mondays, loc.Thursdays}
	now := mustParse(t, "2024-02-29T12:34:56Z") // leap year
	for _, p := range periods {
		t.Run(p.Name, func(t *testing.T) {
			cur := p.Start(now)
			prev := p.Prev(cur)
			if !prev.Before(cur) && !prev.Equal(cur) {
				t.Fatalf("%s.Prev should be <= current start", p.Name)
			}
			// Calling Start on Prev should not jump forward past cur.
			if s := p.Start(prev); s.After(cur) {
				t.Fatalf("%s.Start(Prev(cur)) is after cur start: %v > %v", p.Name, s, cur)
			}
		})
	}
}

func TestLastNKeys_NIsZero_ReturnsEmpty(t *testing.T) {
	got := loc.Days.LastNKeys(time.Now().UTC(), 0)
	if len(got) != 0 {
		t.Fatalf("LastNKeys with n=0: got %d keys, want 0", len(got))
	}
}
