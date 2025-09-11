package locate

import (
	"testing"
	"time"
)

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

func TestMinutes(t *testing.T) {
	now := mustParse(t, "2023-03-15T13:45:59Z")
	if got, want := Minutes.Key(now), "2023-03-15-13:45"; got != want {
		t.Fatalf("Minutes.Key: got %q want %q", got, want)
	}
	if got, want := Minutes.Start(now), mustParse(t, "2023-03-15T13:45:00Z"); !got.Equal(want) {
		t.Fatalf("Minutes.Start: got %v want %v", got, want)
	}
	if got, want := Minutes.Prev(Minutes.Start(now)), mustParse(t, "2023-03-15T13:44:00Z"); !got.Equal(want) {
		t.Fatalf("Minutes.Prev: got %v want %v", got, want)
	}
}

func TestHours(t *testing.T) {
	now := mustParse(t, "2023-03-15T13:45:59Z")
	if got, want := Hours.Key(now), "2023-03-15-13"; got != want {
		t.Fatalf("Hours.Key: got %q want %q", got, want)
	}
	if got, want := Hours.Start(now), mustParse(t, "2023-03-15T13:00:00Z"); !got.Equal(want) {
		t.Fatalf("Hours.Start: got %v want %v", got, want)
	}
	if got, want := Hours.Prev(Hours.Start(now)), mustParse(t, "2023-03-15T12:00:00Z"); !got.Equal(want) {
		t.Fatalf("Hours.Prev: got %v want %v", got, want)
	}
}

func TestDays(t *testing.T) {
	now := mustParse(t, "2023-03-15T13:45:59Z")
	if got, want := Days.Key(now), "2023-03-15"; got != want {
		t.Fatalf("Days.Key: got %q want %q", got, want)
	}
	if got, want := Days.Start(now), mustParse(t, "2023-03-15T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Days.Start: got %v want %v", got, want)
	}
	if got, want := Days.Prev(Days.Start(now)), mustParse(t, "2023-03-14T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Days.Prev: got %v want %v", got, want)
	}
}

func TestWeeks_Key_And_Start(t *testing.T) {
	// ISO week tricky boundary: 2021-01-01 is still 2020-W53
	d1 := mustParse(t, "2020-12-31T10:00:00Z")
	if got, want := Weeks.Key(d1), "2020-W53"; got != want {
		t.Fatalf("Weeks.Key 2020-12-31: got %q want %q", got, want)
	}
	d2 := mustParse(t, "2021-01-01T10:00:00Z")
	if got, want := Weeks.Key(d2), "2020-W53"; got != want {
		t.Fatalf("Weeks.Key 2021-01-01: got %q want %q", got, want)
	}

	// Start is Monday 00:00:00Z of the ISO week.
	wd := mustParse(t, "2023-03-15T13:45:00Z") // Wed
	start := Weeks.Start(wd)
	if got, want := start, mustParse(t, "2023-03-13T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Weeks.Start (Wed): got %v want %v", got, want)
	}

	// For a Sunday, start is the previous Monday.
	sun := mustParse(t, "2023-03-19T23:59:59Z")
	if got, want := Weeks.Start(sun), mustParse(t, "2023-03-13T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Weeks.Start (Sun): got %v want %v", got, want)
	}

	// Prev goes back one week exactly.
	if got, want := Weeks.Prev(start), mustParse(t, "2023-03-06T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Weeks.Prev: got %v want %v", got, want)
	}
}

func TestMonths(t *testing.T) {
	endOfMonth := mustParse(t, "2023-03-31T22:10:00Z")
	if got, want := Months.Key(endOfMonth), "2023-03"; got != want {
		t.Fatalf("Months.Key: got %q want %q", got, want)
	}
	if got, want := Months.Start(endOfMonth), mustParse(t, "2023-03-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Months.Start: got %v want %v", got, want)
	}
	if got, want := Months.Prev(Months.Start(endOfMonth)), mustParse(t, "2023-02-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Months.Prev: got %v want %v", got, want)
	}
}

func TestYears(t *testing.T) {
	now := mustParse(t, "2023-08-05T11:22:33Z")
	if got, want := Years.Key(now), "2023"; got != want {
		t.Fatalf("Years.Key: got %q want %q", got, want)
	}
	if got, want := Years.Start(now), mustParse(t, "2023-01-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Years.Start: got %v want %v", got, want)
	}
	if got, want := Years.Prev(Years.Start(now)), mustParse(t, "2022-01-01T00:00:00Z"); !got.Equal(want) {
		t.Fatalf("Years.Prev: got %v want %v", got, want)
	}
}

// --- weekday-aligned periods (with date+name key) ---
func TestWeekdayPeriod_MondayAlignAndPrev(t *testing.T) {
	// Wed, Aug 27, 2025 → Monday of that ISO week is Aug 25, 2025
	wed := mustParse(t, "2025-08-27T13:37:00Z")
	start := Mondays.Start(wed)
	wantStart := mustParse(t, "2025-08-25T00:00:00Z")
	if !start.Equal(wantStart) {
		t.Fatalf("Mondays.Start: got %v want %v", start, wantStart)
	}
	// Prev Monday is Aug 18, 2025
	prev := Mondays.Prev(start)
	wantPrev := mustParse(t, "2025-08-18T00:00:00Z")
	if !prev.Equal(wantPrev) {
		t.Fatalf("Mondays.Prev: got %v want %v", prev, wantPrev)
	}

	// Use the aligned start when checking the key
	if got, want := Mondays.Key(start), "2025-W35-monday"; got != want {
		t.Fatalf("Mondays.Key: got %q want %q", got, want)
	}
}

func TestWeekdayPeriod_ThursdayWeekBoundary(t *testing.T) {
	// Thu, Jan 1, 2026 — Thursday-aligned Start is 2026-01-01 00:00Z
	thu := mustParse(t, "2026-01-01T12:00:00Z")
	start := Thursdays.Start(thu)
	wantStart := mustParse(t, "2026-01-01T00:00:00Z")
	if !start.Equal(wantStart) {
		t.Fatalf("Thursdays.Start boundary: got %v want %v", start, wantStart)
	}
	// Prev should jump to the previous Thursday (−7 days).
	prev := Thursdays.Prev(start)
	wantPrev := mustParse(t, "2025-12-25T00:00:00Z")
	if !prev.Equal(wantPrev) {
		t.Fatalf("Thursdays.Prev boundary: got %v want %v", prev, wantPrev)
	}
	// Use the aligned start when checking the key
	if got, want := Thursdays.Key(start), "2026-W01-thursday"; got != want {
		t.Fatalf("Thursdays.Key: got %q want %q", got, want)
	}
}
func TestLastNKeys_Weekday_Monday(t *testing.T) {
	// Anchor on a Wednesday; the aligned Monday is 2025-08-25
	now := mustParse(t, "2025-08-27T12:00:00Z")
	keys := Mondays.LastNKeys(now, 3)

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
	loc, _ := time.LoadLocation("America/New_York")
	localTime := time.Date(2023, 7, 4, 23, 59, 59, 0, loc)

	tests := []struct {
		name   string
		p      Period
		expect time.Time
	}{
		{"minute", Minutes, Minutes.Start(localTime.In(time.UTC))},
		{"hour", Hours, Hours.Start(localTime.In(time.UTC))},
		{"day", Days, Days.Start(localTime.In(time.UTC))},
		{"week", Weeks, Weeks.Start(localTime.In(time.UTC))},
		{"month", Months, Months.Start(localTime.In(time.UTC))},
		{"year", Years, Years.Start(localTime.In(time.UTC))},
		{"monday", Mondays, Mondays.Start(localTime.In(time.UTC))},
		{"thursday", Thursdays, Thursdays.Start(localTime.In(time.UTC))},
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
	periods := []Period{Minutes, Hours, Days, Weeks, Months, Years, Mondays, Thursdays}
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
	got := Days.LastNKeys(time.Now().UTC(), 0)
	if len(got) != 0 {
		t.Fatalf("LastNKeys with n=0: got %d keys, want 0", len(got))
	}
}
