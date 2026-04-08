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
	if got, want := loc.Minutes.Key(dateRef), "2023-03-15-13:45"; got != want {
		t.Fatalf("Minutes.Key: got %q want %q", got, want)
	}
	if got, want := loc.Minutes.Start(dateRef), mustParse(t, "2023-03-15T13:45:00Z"); !got.Equal(want) {
		t.Fatalf("Minutes.Start: got %v want %v", got, want)
	}
	if got, want := loc.Minutes.Prev(loc.Minutes.Start(dateRef)), mustParse(t, "2023-03-15T13:44:00Z"); !got.Equal(want) {
		t.Fatalf("Minutes.Prev: got %v want %v", got, want)
	}
}

func TestHours(t *testing.T) {
	if got, want := loc.Hours.Key(dateRef), "2023-03-15-13"; got != want {
		t.Fatalf("Hours.Key: got %q want %q", got, want)
	}
	if got, want := loc.Hours.Start(dateRef), mustParse(t, "2023-03-15T13:00:00Z"); !got.Equal(want) {
		t.Fatalf("Hours.Start: got %v want %v", got, want)
	}
	if got, want := loc.Hours.Prev(loc.Hours.Start(dateRef)), mustParse(t, "2023-03-15T12:00:00Z"); !got.Equal(want) {
		t.Fatalf("Hours.Prev: got %v want %v", got, want)
	}
}

func TestWeeks_Key_And_Start(t *testing.T) {
	// ISO week tricky boundary: 2021-01-01 is still 2020-W53
	d1 := mustParse(t, "2020-12-31T10:00:00Z")
	if got, want := loc.Weeks.Key(d1), "2020-W53"; got != want {
		t.Fatalf("Weeks.Key 2020-12-31: got %q want %q", got, want)
	}
	d2 := mustParse(t, "2021-01-01T10:00:00Z")
	if got, want := loc.Weeks.Key(d2), "2020-W53"; got != want {
		t.Fatalf("Weeks.Key 2021-01-01: got %q want %q", got, want)
	}

	// Start is Monday 00:00:00Z of the ISO week.
	wd := mustParse(t, "2023-03-15T13:45:00Z") // Wed
	start := loc.Weeks.Start(wd)
	if got, want := start, mustParse(t, "2023-03-13T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Weeks.Start (Wed): got %v want %v", got, want)
	}

	// For a Sunday, start is the previous Monday.
	sun := mustParse(t, "2023-03-19T23:59:59Z")
	if got, want := loc.Weeks.Start(sun), mustParse(t, "2023-03-13T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Weeks.Start (Sun): got %v want %v", got, want)
	}

	// Prev goes back one week exactly.
	if got, want := loc.Weeks.Prev(start), mustParse(t, "2023-03-06T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Weeks.Prev: got %v want %v", got, want)
	}
}

func TestMonths(t *testing.T) {
	endOfMonth := mustParse(t, "2023-03-31T22:10:00Z")
	if got, want := loc.Months.Key(endOfMonth), "2023-03"; got != want {
		t.Fatalf("Months.Key: got %q want %q", got, want)
	}
	if got, want := loc.Months.Start(endOfMonth), mustParse(t, "2023-03-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Months.Start: got %v want %v", got, want)
	}
	if got, want := loc.Months.Prev(loc.Months.Start(endOfMonth)), mustParse(t, "2023-02-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Months.Prev: got %v want %v", got, want)
	}
}

func TestYears(t *testing.T) {
	now := mustParse(t, "2023-08-05T11:22:33Z")
	if got, want := loc.Years.Key(now), "2023"; got != want {
		t.Fatalf("Years.Key: got %q want %q", got, want)
	}
	if got, want := loc.Years.Start(now), mustParse(t, "2023-01-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Years.Start: got %v want %v", got, want)
	}
	if got, want := loc.Years.Prev(loc.Years.Start(now)), mustParse(t, "2022-01-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Years.Prev: got %v want %v", got, want)
	}
}

// --- weekday-aligned periods (with date+name key) ---
func TestWeekdayPeriod_MondayAlignAndPrev(t *testing.T) {
	// Wed, Aug 27, 2025 → Monday of that ISO week is Aug 25, 2025
	wed := mustParse(t, "2025-08-27T13:37:00Z")
	start := loc.Mondays.Start(wed)
	wantStart := mustParse(t, "2025-08-25T00:00:00Z")
	if !start.Equal(wantStart) {
		t.Fatalf("Mondays.Start: got %v want %v", start, wantStart)
	}
	// Prev Monday is Aug 18, 2025
	prev := loc.Mondays.Prev(start)
	wantPrev := mustParse(t, "2025-08-18T00:00:00Z")
	if !prev.Equal(wantPrev) {
		t.Fatalf("Mondays.Prev: got %v want %v", prev, wantPrev)
	}

	// Use the aligned start when checking the key
	if got, want := loc.Mondays.Key(start), "2025-W35-monday"; got != want {
		t.Fatalf("Mondays.Key: got %q want %q", got, want)
	}
}

func TestWeekdayPeriod_ThursdayWeekBoundary(t *testing.T) {
	// Thu, Jan 1, 2026 — Thursday-aligned Start is 2026-01-01 00:00Z
	thu := mustParse(t, "2026-01-01T12:00:00Z")
	start := loc.Thursdays.Start(thu)
	wantStart := mustParse(t, "2026-01-01T00:00:00Z")
	if !start.Equal(wantStart) {
		t.Fatalf("Thursdays.Start boundary: got %v want %v", start, wantStart)
	}
	// Prev should jump to the previous Thursday (−7 days).
	prev := loc.Thursdays.Prev(start)
	wantPrev := mustParse(t, "2025-12-25T00:00:00Z")
	if !prev.Equal(wantPrev) {
		t.Fatalf("Thursdays.Prev boundary: got %v want %v", prev, wantPrev)
	}
	// Use the aligned start when checking the key
	if got, want := loc.Thursdays.Key(start), "2026-W01-thursday"; got != want {
		t.Fatalf("Thursdays.Key: got %q want %q", got, want)
	}
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
