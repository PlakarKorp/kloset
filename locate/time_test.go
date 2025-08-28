package locate

import (
	"strings"
	"testing"
	"time"
)

// helper: compare times exactly (in UTC) for absolute parses
func mustUTC(t *testing.T, y int, m time.Month, d, hh, mm, ss int) time.Time {
	t.Helper()
	return time.Date(y, m, d, hh, mm, ss, 0, time.UTC)
}

func TestParseTimeFlag_Empty(t *testing.T) {
	got, err := ParseTimeFlag("")
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !got.IsZero() {
		t.Fatalf("expected zero time, got %v", got)
	}
}

func TestParseTimeFlag_AbsoluteFormats(t *testing.T) {
	tcs := []struct {
		in   string
		want time.Time
	}{
		// RFC3339
		{"2025-08-28T12:34:56Z", mustUTC(t, 2025, time.August, 28, 12, 34, 56)},

		// "2006-01-02 15:04"
		{"2025-08-28 09:10", mustUTC(t, 2025, time.August, 28, 9, 10, 0)},

		// "2006-01-02 15:04:05"
		{"2025-08-28 09:10:11", mustUTC(t, 2025, time.August, 28, 9, 10, 11)},

		// "2006-01-02"
		{"2025-08-28", mustUTC(t, 2025, time.August, 28, 0, 0, 0)},

		// "2006/01/02"
		{"2025/08/28", mustUTC(t, 2025, time.August, 28, 0, 0, 0)},
	}

	for _, tc := range tcs {
		got, err := ParseTimeFlag(tc.in)
		if err != nil {
			t.Fatalf("ParseTimeFlag(%q) error: %v", tc.in, err)
		}
		if !got.Equal(tc.want) {
			t.Fatalf("ParseTimeFlag(%q): got %v want %v", tc.in, got, tc.want)
		}
		if got.Location() != time.UTC {
			t.Fatalf("ParseTimeFlag(%q): want UTC location, got %v", tc.in, got.Location())
		}
	}
}
func TestParseTimeFlag_DurationRelativeNow(t *testing.T) {
	// Because ParseTimeFlag uses time.Now().Add(-d),
	// we measure before/after to assert the result falls in a small window.
	type durCase struct {
		in      string
		approxD time.Duration
	}
	tcs := []durCase{
		{"2h", 2 * time.Hour},
		{"1h30min", 90 * time.Minute}, // IMPORTANT: use "min", not "m"
		{"45min", 45 * time.Minute},   // IMPORTANT: use "min", not "m"
		{"10s", 10 * time.Second},
	}

	const slack = 250 * time.Millisecond // tolerate small runtime jitter

	for _, tc := range tcs {
		before := time.Now()
		got, err := ParseTimeFlag(tc.in)
		after := time.Now()
		if err != nil {
			t.Fatalf("ParseTimeFlag(%q) error: %v", tc.in, err)
		}
		// Expected window is (before - d) .. (after - d)
		lower := before.Add(-tc.approxD).Add(-slack)
		upper := after.Add(-tc.approxD).Add(slack)

		if got.Before(lower) || got.After(upper) {
			t.Fatalf("ParseTimeFlag(%q): got %v, want in [%v, %v]", tc.in, got, lower, upper)
		}
	}
}

func TestParseTimeFlag_Invalid(t *testing.T) {
	bad := []string{
		"not-a-time",
		"2025-13-01",       // invalid month
		"2025-02-30",       // invalid day
		"99wibbly",         // bogus duration
		"2025/99/01 25:61", // broken format & values
	}
	for _, in := range bad {
		_, err := ParseTimeFlag(in)
		if err == nil {
			t.Fatalf("expected error for %q, got nil", in)
		}
		if !strings.Contains(err.Error(), "invalid time format") {
			t.Fatalf("unexpected error for %q: %v", in, err)
		}
	}
}

func TestTimeFlag_SetAndString(t *testing.T) {
	var dest time.Time
	tf := NewTimeFlag(&dest)

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
	tf := NewTimeFlag(&dest)

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

func TestParseTimeFlag_MMeansMonths(t *testing.T) {
	before := time.Now()
	got, err := ParseTimeFlag("1m") // in this lib, "m" == month
	if err != nil {
		t.Fatalf("ParseTimeFlag(1m) error: %v", err)
	}
	// Expect roughly 1 month ago (we'll use ~28..31 day window).
	minAgo := before.Add(-31 * 24 * time.Hour)
	maxAgo := before.Add(-28 * 24 * time.Hour)
	if got.Before(minAgo) || got.After(maxAgo) {
		t.Fatalf(`"1m" should be ~1 month ago, got %v (expected between %v and %v)`, got, minAgo, maxAgo)
	}
}
