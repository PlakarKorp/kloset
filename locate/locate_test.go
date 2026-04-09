package locate_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func TestLocatePeriodEmpty(t *testing.T) {
	t.Run("EmptyArgumentsReturnsTrue", func(t *testing.T) {
		lp := loc.LocatePeriod{}
		require.True(t, lp.Empty())
	})

	t.Run("ReturnsFalseIfAnyArgumentIsSet", func(t *testing.T) {
		testCases := []loc.LocatePeriod{
			{Keep: 1, Cap: 0},
			{Keep: 0, Cap: 1},
			{Keep: 1, Cap: 1},
			{Keep: 2, Cap: 3},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(fmt.Sprintf("Keep%dCap%d", tc.Keep, tc.Cap), func(t *testing.T) {
				require.False(t, tc.Empty())
			})
		}
	})
}

// NewDefaultLocateOptions is tested first with anonymous Option functions
// instead of the public With... helpers.
// This keeps the testing consistency:
// - NewDefaultLocateOptions is validated on its own contract first
// - the public With... helpers are tested afterwards as a separate API block
func TestNewDefaultLocateOptions(t *testing.T) {
	t.Run("WithoutOptionsReturnsNonNilEmptyLocateOptions", func(t *testing.T) {
		got := loc.NewDefaultLocateOptions()
		require.NotNil(t, got)
		require.Equal(t, &loc.LocateOptions{}, got)
	})

	t.Run("AppliesSingleOption", func(t *testing.T) {
		got := loc.NewDefaultLocateOptions(func(lo *loc.LocateOptions) {
			lo.Filters.Name = "daily-backup"
		})
		require.Equal(t, "daily-backup", got.Filters.Name)
	})

	t.Run("AppliesMultipleOptions", func(t *testing.T) {
		before := time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC)

		got := loc.NewDefaultLocateOptions(
			func(lo *loc.LocateOptions) {
				lo.Filters.Name = "daily-backup"
			},
			func(lo *loc.LocateOptions) {
				lo.Filters.Before = before
			},
			func(lo *loc.LocateOptions) {
				lo.Periods.Day.Keep = 7
			},
			func(lo *loc.LocateOptions) {
				lo.Periods.Day.Cap = 2
			},
		)
		require.Equal(t, "daily-backup", got.Filters.Name)
		require.Equal(t, before, got.Filters.Before)
		require.Equal(t, 7, got.Periods.Day.Keep)
		require.Equal(t, 2, got.Periods.Day.Cap)
	})

	t.Run("AppliesOptionsInOrder", func(t *testing.T) {
		got := loc.NewDefaultLocateOptions(
			func(lo *loc.LocateOptions) {
				lo.Filters.Name = "first"
			},
			func(lo *loc.LocateOptions) {
				lo.Filters.Name = "second"
			},
		)
		require.Equal(t, "second", got.Filters.Name)
	})
}

func TestWithKeepOptions(t *testing.T) {
	t.Run("SetsStandardPeriodKeepFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithKeepMinutes(1)(lo)
		loc.WithKeepHours(2)(lo)
		loc.WithKeepDays(3)(lo)
		loc.WithKeepWeeks(4)(lo)
		loc.WithKeepMonths(5)(lo)
		loc.WithKeepYears(6)(lo)

		require.Equal(t, 1, lo.Periods.Minute.Keep)
		require.Equal(t, 2, lo.Periods.Hour.Keep)
		require.Equal(t, 3, lo.Periods.Day.Keep)
		require.Equal(t, 4, lo.Periods.Week.Keep)
		require.Equal(t, 5, lo.Periods.Month.Keep)
		require.Equal(t, 6, lo.Periods.Year.Keep)
	})

	t.Run("SetsWeekdayPeriodKeepFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithKeepMondays(1)(lo)
		loc.WithKeepTuesdays(2)(lo)
		loc.WithKeepWednesdays(3)(lo)
		loc.WithKeepThursdays(4)(lo)
		loc.WithKeepFridays(5)(lo)
		loc.WithKeepSaturdays(6)(lo)
		loc.WithKeepSundays(7)(lo)

		require.Equal(t, 1, lo.Periods.Monday.Keep)
		require.Equal(t, 2, lo.Periods.Tuesday.Keep)
		require.Equal(t, 3, lo.Periods.Wednesday.Keep)
		require.Equal(t, 4, lo.Periods.Thursday.Keep)
		require.Equal(t, 5, lo.Periods.Friday.Keep)
		require.Equal(t, 6, lo.Periods.Saturday.Keep)
		require.Equal(t, 7, lo.Periods.Sunday.Keep)
	})
}

func TestWithCapOptions(t *testing.T) {
	t.Run("SetsStandardPeriodCapFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithPerMinuteCap(1)(lo)
		loc.WithPerHourCap(2)(lo)
		loc.WithPerDayCap(3)(lo)
		loc.WithPerWeekCap(4)(lo)
		loc.WithPerMonthCap(5)(lo)
		loc.WithPerYearCap(6)(lo)

		require.Equal(t, 1, lo.Periods.Minute.Cap)
		require.Equal(t, 2, lo.Periods.Hour.Cap)
		require.Equal(t, 3, lo.Periods.Day.Cap)
		require.Equal(t, 4, lo.Periods.Week.Cap)
		require.Equal(t, 5, lo.Periods.Month.Cap)
		require.Equal(t, 6, lo.Periods.Year.Cap)
	})

	t.Run("SetsWeekdayPeriodCapFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithPerMondayCap(1)(lo)
		loc.WithPerTuesdayCap(2)(lo)
		loc.WithPerWednsdayCap(3)(lo)
		loc.WithPerThursdayCap(4)(lo)
		loc.WithPerFridayCap(5)(lo)
		loc.WithPerSaturdayCap(6)(lo)
		loc.WithPerSundaysCap(7)(lo)

		require.Equal(t, 1, lo.Periods.Monday.Cap)
		require.Equal(t, 2, lo.Periods.Tuesday.Cap)
		require.Equal(t, 3, lo.Periods.Wednesday.Cap)
		require.Equal(t, 4, lo.Periods.Thursday.Cap)
		require.Equal(t, 5, lo.Periods.Friday.Cap)
		require.Equal(t, 6, lo.Periods.Saturday.Cap)
		require.Equal(t, 7, lo.Periods.Sunday.Cap)
	})
}

func TestWithFilterOptions(t *testing.T) {
	t.Run("SetsScalarFilterFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		before := time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC)
		since := time.Date(2025, time.August, 20, 8, 0, 0, 0, time.UTC)

		loc.WithBefore(before)(lo)
		loc.WithSince(since)(lo)
		loc.WithName("daily-backup")(lo)
		loc.WithCategory("database")(lo)
		loc.WithEnvironment("prod")(lo)
		loc.WithPerimeter("eu-west")(lo)
		loc.WithJob("nightly")(lo)
		loc.WithLatest(true)(lo)

		require.Equal(t, before, lo.Filters.Before)
		require.Equal(t, since, lo.Filters.Since)
		require.Equal(t, "daily-backup", lo.Filters.Name)
		require.Equal(t, "database", lo.Filters.Category)
		require.Equal(t, "prod", lo.Filters.Environment)
		require.Equal(t, "eu-west", lo.Filters.Perimeter)
		require.Equal(t, "nightly", lo.Filters.Job)
		require.True(t, lo.Filters.Latest)
	})

	t.Run("AppendsSliceBasedFilterFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithTag("important")(lo)
		loc.WithTag("daily")(lo)
		loc.WithOrigin("s3://bucket-a")(lo)
		loc.WithOrigin("s3://bucket-b")(lo)
		loc.WithID("abc123")(lo)
		loc.WithID("def456")(lo)

		require.Equal(t, []string{"important", "daily"}, lo.Filters.Tags)
		require.Equal(t, []string{"s3://bucket-a", "s3://bucket-b"}, lo.Filters.Origins)
		require.Equal(t, []string{"abc123", "def456"}, lo.Filters.IDs)
	})
}

func TestLocateOptionsHasPeriods(t *testing.T) {
	t.Run("False_WhenAllPeriodsAreEmpty", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		require.False(t, lo.HasPeriods())
	})

	t.Run("ReturnsTrueWhenAnyKeepPeriodIsConfigured", func(t *testing.T) {
		testCases := []struct {
			name  string
			apply loc.Option
		}{
			{name: "Minute", apply: loc.WithKeepMinutes(1)},
			{name: "Hour", apply: loc.WithKeepHours(1)},
			{name: "Day", apply: loc.WithKeepDays(1)},
			{name: "Week", apply: loc.WithKeepWeeks(1)},
			{name: "Month", apply: loc.WithKeepMonths(1)},
			{name: "Year", apply: loc.WithKeepYears(1)},
			{name: "Monday", apply: loc.WithKeepMondays(1)},
			{name: "Tuesday", apply: loc.WithKeepTuesdays(1)},
			{name: "Wednesday", apply: loc.WithKeepWednesdays(1)},
			{name: "Thursday", apply: loc.WithKeepThursdays(1)},
			{name: "Friday", apply: loc.WithKeepFridays(1)},
			{name: "Saturday", apply: loc.WithKeepSaturdays(1)},
			{name: "Sunday", apply: loc.WithKeepSundays(1)},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				lo := &loc.LocateOptions{}
				tc.apply(lo)
				require.True(t, lo.HasPeriods())
			})
		}
	})

	t.Run("ReturnsTrueWhenAnyCapPeriodIsConfigured", func(t *testing.T) {
		testCases := []struct {
			name  string
			apply loc.Option
		}{
			{name: "Minute", apply: loc.WithPerMinuteCap(1)},
			{name: "Hour", apply: loc.WithPerHourCap(1)},
			{name: "Day", apply: loc.WithPerDayCap(1)},
			{name: "Week", apply: loc.WithPerWeekCap(1)},
			{name: "Month", apply: loc.WithPerMonthCap(1)},
			{name: "Year", apply: loc.WithPerYearCap(1)},
			{name: "Monday", apply: loc.WithPerMondayCap(1)},
			{name: "Tuesday", apply: loc.WithPerTuesdayCap(1)},
			{name: "Wednesday", apply: loc.WithPerWednsdayCap(1)},
			{name: "Thursday", apply: loc.WithPerThursdayCap(1)},
			{name: "Friday", apply: loc.WithPerFridayCap(1)},
			{name: "Saturday", apply: loc.WithPerSaturdayCap(1)},
			{name: "Sunday", apply: loc.WithPerSundaysCap(1)},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				lo := &loc.LocateOptions{}
				tc.apply(lo)
				require.True(t, lo.HasPeriods())
			})
		}
	})
}

// ========== Utilities ==========

func mustRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return tt.UTC()
}

// mac(n) returns a unique, deterministic objects.MAC for tests by setting
// the first byte (works when MAC is a named byte array, which is typical).
// If MAC is not a byte array, this is a no-op and tests still compile,
// but uniqueness may not hold (adjust if needed).
func mac(n byte) objects.MAC {
	var m objects.MAC
	m[0] = n
	return m
}

// makeItem creates a test Item.
func makeItem(t *testing.T, id objects.MAC, ts string, name string, cat string, env string, perim string, job string, tags []string, roots []string, origins []string, types []string) loc.Item {
	t.Helper()
	return loc.Item{
		ItemID:    id,
		Timestamp: mustRFC3339(t, ts),
		Filters: loc.ItemFilters{
			Name:        name,
			Category:    cat,
			Environment: env,
			Perimeter:   perim,
			Job:         job,
			Tags:        tags,
			Roots:       roots,
			Origins:     origins,
			Types:       types,
		},
	}
}

// ========== Options wiring / HasPeriods / Empty ==========

func TestLocateOptions_Empty(t *testing.T) {
	var lo loc.LocateOptions
	if !lo.Empty() {
		t.Fatalf("Empty() should be true on zero-value")
	}
	lo.Filters.Name = "x"
	if lo.Empty() {
		t.Fatalf("Empty() should be false when a filter is set")
	}
	lo = loc.LocateOptions{}
	lo.Periods.Minute.Keep = 1
	if lo.Empty() {
		t.Fatalf("Empty() should be false when a period keep is set")
	}
}

// ========== Matches (IDs / time windows / headers / tags / roots) ==========

func TestMatches_IDPrefix(t *testing.T) {
	var lo loc.LocateOptions
	idZero := objects.NilMac
	it := loc.Item{ItemID: idZero, Timestamp: mustRFC3339(t, "2025-08-20T10:00:00Z")}
	// fmt.Sprintf("%x" , zero) will be all zeros → should match prefix "0"
	lo.Filters.IDs = []string{"0"}
	if !lo.Matches(it) {
		t.Fatalf("expected match for ID prefix '0'")
	}
	lo.Filters.IDs = []string{"f"} // zero MAC does not start with 'f'
	if lo.Matches(it) {
		t.Fatalf("expected no match for ID prefix 'f'")
	}
}

func TestMatches_TimeWindow(t *testing.T) {
	now := mustRFC3339(t, "2025-08-20T12:00:00Z")
	itemBefore := loc.Item{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T11:00:00Z")}
	itemAfter := loc.Item{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T13:00:00Z")}

	// Before: reject items strictly AFTER the 'before' instant; equal allowed.
	var lo loc.LocateOptions
	lo.Filters.Before = now
	if !lo.Matches(itemBefore) {
		t.Fatalf("itemBefore should match with Before=now")
	}
	if !lo.Matches(loc.Item{ItemID: mac(3), Timestamp: now}) {
		t.Fatalf("item at exact Before should match (not After)")
	}
	if lo.Matches(itemAfter) {
		t.Fatalf("itemAfter should not match (is After Before)")
	}

	// Since: reject items strictly BEFORE the 'since' instant; equal allowed.
	lo = loc.LocateOptions{}
	lo.Filters.Since = now
	if lo.Matches(itemBefore) {
		t.Fatalf("itemBefore should not match (Before Since)")
	}
	if !lo.Matches(loc.Item{ItemID: mac(4), Timestamp: now}) {
		t.Fatalf("item at exact Since should match")
	}
	if !lo.Matches(itemAfter) {
		t.Fatalf("itemAfter should match (After Since)")
	}
}

func TestMatches_Headers_Tags_Roots(t *testing.T) {
	it := makeItem(t, mac(1), "2025-08-20T10:00:00Z", "n1", "cat", "prod", "eu", "backup", []string{"t1", "t2"}, []string{"r1", "r2"}, []string{}, []string{})

	tests := []struct {
		name string
		cfg  func(*loc.LocateOptions)
		want bool
	}{
		{"name ok", func(lo *loc.LocateOptions) { lo.Filters.Name = "n1" }, true},
		{"name mismatch", func(lo *loc.LocateOptions) { lo.Filters.Name = "n2" }, false},
		{"category ok", func(lo *loc.LocateOptions) { lo.Filters.Category = "cat" }, true},
		{"env mismatch", func(lo *loc.LocateOptions) { lo.Filters.Environment = "stage" }, false},
		{"perimeter ok", func(lo *loc.LocateOptions) { lo.Filters.Perimeter = "eu" }, true},
		{"job mismatch", func(lo *loc.LocateOptions) { lo.Filters.Job = "restore" }, false},
		{"tags all-present", func(lo *loc.LocateOptions) { lo.Filters.Tags = []string{"t1", "t2"} }, true},
		{"tags missing-one", func(lo *loc.LocateOptions) { lo.Filters.Tags = []string{"t1", "t3"} }, false},
		{"ignore tags", func(lo *loc.LocateOptions) { lo.Filters.IgnoreTags = []string{"t2"} }, false},
		{"roots all-present", func(lo *loc.LocateOptions) { lo.Filters.Roots = []string{"r1", "r2"} }, true},
		{"roots missing-one", func(lo *loc.LocateOptions) { lo.Filters.Roots = []string{"r1", "r3"} }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var lo loc.LocateOptions
			tc.cfg(&lo)
			if got := lo.Matches(it); got != tc.want {
				t.Fatalf("got %v want %v for %s", got, tc.want, tc.name)
			}
		})
	}
}

func TestMatches_Origins(t *testing.T) {
	it := makeItem(t, mac(1), "2025-08-20T10:00:00Z", "n1", "cat", "prod", "eu", "backup", []string{"t1", "t2"}, []string{"r1", "r2"}, []string{"hosta"}, []string{})

	tests := []struct {
		name string
		cfg  func(*loc.LocateOptions)
		want bool
	}{
		{"origin match", func(lo *loc.LocateOptions) { lo.Filters.Origins = []string{"hosta"} }, true},
		{"origin multi match 1", func(lo *loc.LocateOptions) { lo.Filters.Origins = []string{"hosta", "hostb"} }, true},
		{"origin multi match 2", func(lo *loc.LocateOptions) { lo.Filters.Origins = []string{"hostb", "hosta", "hostc"} }, true},
		{"origin empty", func(lo *loc.LocateOptions) { lo.Filters.Origins = []string{""} }, true},
		{"origin not specified", func(lo *loc.LocateOptions) { lo.Filters.Tags = []string{""} }, true},
		{"origin not match", func(lo *loc.LocateOptions) { lo.Filters.Origins = []string{"hostb"} }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var lo loc.LocateOptions
			tc.cfg(&lo)
			if got := lo.Matches(it); got != tc.want {
				t.Fatalf("got %v want %v for %s", got, tc.want, tc.name)
			}
		})
	}
}

func TestMatches_Types(t *testing.T) {
	it := makeItem(t, mac(1), "2025-08-20T10:00:00Z", "n1", "cat", "prod", "eu", "backup", []string{"t1", "t2"}, []string{"r1", "r2"}, []string{}, []string{"fs"})

	tests := []struct {
		name string
		cfg  func(*loc.LocateOptions)
		want bool
	}{
		{"type match", func(lo *loc.LocateOptions) { lo.Filters.Types = []string{"fs"} }, true},
		{"type multi match 1", func(lo *loc.LocateOptions) { lo.Filters.Types = []string{"fs", "s3"} }, true},
		{"type multi match 2", func(lo *loc.LocateOptions) { lo.Filters.Types = []string{"s3", "fs", "onedrive"} }, true},
		{"type empty", func(lo *loc.LocateOptions) { lo.Filters.Types = []string{""} }, true},
		{"type not specified", func(lo *loc.LocateOptions) { lo.Filters.Tags = []string{""} }, true},
		{"type not match", func(lo *loc.LocateOptions) { lo.Filters.Types = []string{"s3"} }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var lo loc.LocateOptions
			tc.cfg(&lo)
			if got := lo.Matches(it); got != tc.want {
				t.Fatalf("got %v want %v for %s", got, tc.want, tc.name)
			}
		})
	}
}

// ========== FilterAndSort (ordering + Latest) ==========

func TestFilterAndSort_OrderingAndLatest(t *testing.T) {
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T12:00:00Z")},
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T13:00:00Z")},
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-19T23:59:59Z")},
	}
	var lo loc.LocateOptions
	sorted := lo.FilterAndSort(items)
	if len(sorted) != 3 {
		t.Fatalf("expected 3 items")
	}
	if !sorted[0].Timestamp.After(sorted[1].Timestamp) || !sorted[1].Timestamp.After(sorted[2].Timestamp) {
		t.Fatalf("not sorted desc by Timestamp")
	}
	lo.Filters.Latest = true
	one := lo.FilterAndSort(items)
	if len(one) != 1 || !one[0].Timestamp.Equal(mustRFC3339(t, "2025-08-20T13:00:00Z")) {
		t.Fatalf("Latest should return single newest item, got %#v", one)
	}
}

// ========== Match / retention logic ==========

func TestMatch_KeepOnly_LastNDays_AllInWindowKept(t *testing.T) {
	now := mustRFC3339(t, "2025-08-20T12:00:00Z")
	// Items over the last 3 days (in-window) and one older (out-of-window)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T11:00:00Z")}, // day 2025-08-20
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-19T10:00:00Z")}, // day 2025-08-19
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-18T09:00:00Z")}, // day 2025-08-18
		{ItemID: mac(4), Timestamp: mustRFC3339(t, "2025-08-17T09:00:00Z")}, // outside (if Keep=3)
	}
	lo := loc.LocateOptions{Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 3}}}
	kept, reasons := lo.Match(items, now)

	if len(kept) != 3 {
		t.Fatalf("kept len: got %d want 3", len(kept))
	}
	// The oldest one should be outside window.
	oldID := items[3].ItemID
	if r, ok := reasons[oldID]; !ok || r.Action != "delete" || r.Note != "outside retention windows" {
		t.Fatalf("expect outside-window delete for oldest, got %#v", r)
	}
	// Others kept with rule "day"
	for i := 0; i < 3; i++ {
		id := items[i].ItemID
		r, ok := reasons[id]
		if !ok || r.Action != "keep" || r.Rule != "day" || r.Bucket == "" {
			t.Fatalf("expect keep with day rule for %d, got %#v", i, r)
		}
	}
}

func TestMatch_CapOnly_PerMinute_NewestFirstWithinBucket(t *testing.T) {
	now := mustRFC3339(t, "2025-08-20T12:00:30Z")
	// 3 items in the same minute bucket "2025-08-20-12:00"
	bucket := "2025-08-20-12:00"
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T12:00:20Z")}, // newest within bucket
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T12:00:10Z")},
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-20T12:00:00Z")}, // oldest within bucket
	}
	// Ensure FilterAndSort will order desc by ts (newest first)
	lo := loc.LocateOptions{Periods: loc.LocatePeriods{Minute: loc.LocatePeriod{Cap: 2}}}
	kept, reasons := lo.Match(items, now)

	if len(kept) != 2 {
		t.Fatalf("kept len: got %d want 2", len(kept))
	}

	// Collect ranks
	ranks := make([]int, 0, 3)
	caps := make([]int, 0, 3)
	actions := make([]string, 0, 3)
	for _, it := range items {
		r := reasons[it.ItemID]
		if r.Bucket != bucket || r.Rule != "minute" {
			t.Fatalf("wrong bucket/rule: %#v", r)
		}
		ranks = append(ranks, r.Rank)
		caps = append(caps, r.Cap)
		actions = append(actions, r.Action)
	}
	// Newest ranks should be 1 and 2 → keep, oldest rank 3 → delete (exceeds cap)
	if actions[0] != "keep" || actions[1] != "keep" || actions[2] != "delete" {
		t.Fatalf("actions by rank wrong: %v", actions)
	}
	if ranks[0] != 1 || ranks[1] != 2 || ranks[2] != 3 {
		t.Fatalf("ranks wrong: %v", ranks)
	}
	if caps[0] != 2 || caps[1] != 2 || caps[2] != 2 {
		t.Fatalf("cap recorded wrong: %v", caps)
	}
}
func TestMatch_KeepAndCap_PerHour(t *testing.T) {
	now := mustRFC3339(t, "2025-08-20T17:05:00Z") // IMPORTANT: inside hour 17 so window is 17 & 16

	// Two hours: 17 and 16; multiple items in each hour
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T17:59:59Z")}, // hour 17
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T17:30:00Z")}, // hour 17
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-20T17:00:01Z")}, // hour 17
		{ItemID: mac(4), Timestamp: mustRFC3339(t, "2025-08-20T16:59:59Z")}, // hour 16
		{ItemID: mac(5), Timestamp: mustRFC3339(t, "2025-08-20T16:30:00Z")}, // hour 16
	}

	// Keep last 2 hours (current hour 17 and previous hour 16), capped at 2 per hour.
	lo := loc.LocateOptions{
		Periods: loc.LocatePeriods{
			Hour: loc.LocatePeriod{Keep: 2, Cap: 2},
		},
	}
	kept, reasons := lo.Match(items, now)

	if len(kept) != 4 {
		t.Fatalf("kept len: got %d want 4 (2 per hour)", len(kept))
	}

	// Count keeps per bucket
	perBucket := map[string]int{}
	for _, it := range items {
		r := reasons[it.ItemID]
		if r.Rule != "hour" {
			t.Fatalf("expected hour rule, got %#v", r)
		}
		perBucket[r.Bucket] += boolToInt(r.Action == "keep")
	}

	// Expect exactly 2 kept in hour-17 and 2 in hour-16.
	hours := []string{}
	for b := range perBucket {
		hours = append(hours, b)
	}
	sort.Strings(hours)
	if len(hours) != 2 {
		t.Fatalf("expected 2 hour buckets, got %v", hours)
	}
	for _, b := range hours {
		if perBucket[b] != 2 {
			t.Fatalf("bucket %s: kept=%d want 2", b, perBucket[b])
		}
	}
}

func TestMatch_MultipleRules_KeepBeatsDelete(t *testing.T) {
	now := mustRFC3339(t, "2025-08-20T12:00:00Z")
	// Two items same day & week. Cap day at 1 so the second is "delete" by day,
	// but allow week to keep more (cap=2). The item should be kept overall.
	item1 := loc.Item{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T08:00:00Z")}
	item2 := loc.Item{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T07:00:00Z")}
	items := []loc.Item{item1, item2}

	lo := loc.LocateOptions{
		Periods: loc.LocatePeriods{
			Day:  loc.LocatePeriod{Keep: 1, Cap: 1}, // keep only the newest within the day
			Week: loc.LocatePeriod{Keep: 1, Cap: 2}, // same week key, allow both
		},
	}

	kept, reasons := lo.Match(items, now)
	if len(kept) != 2 {
		t.Fatalf("expected both items kept because week rule keeps 2, got %d", len(kept))
	}
	r2 := reasons[item2.ItemID]
	if r2.Action != "keep" {
		t.Fatalf("expected keep due to week rule overriding day delete; got %#v", r2)
	}
}

func TestMatch_OutsideWindows_NoRulesKeeping(t *testing.T) {
	now := mustRFC3339(t, "2025-08-20T12:00:00Z")
	// Items far in the past; Keep last 1 day only.
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-10T00:00:00Z")},
	}
	lo := loc.LocateOptions{
		Periods: loc.LocatePeriods{
			Day: loc.LocatePeriod{Keep: 1},
		},
	}
	_, reasons := lo.Match(items, now)
	r := reasons[items[0].ItemID]
	if r.Action != "delete" || r.Note != "outside retention windows" {
		t.Fatalf("expected outside retention windows delete, got %#v", r)
	}
}

// Sanity: with no periods, Match should keep all matched-filter items and mark as "matched filters".
func TestMatch_NoPeriods_KeepsAllMatches(t *testing.T) {
	items := []loc.Item{
		makeItem(t, mac(1), "2025-08-20T11:00:00Z", "n", "c", "prod", "eu", "job", []string{"t1"}, []string{"r1"}, []string{}, []string{}),
		makeItem(t, mac(2), "2025-08-18T11:00:00Z", "n", "c", "prod", "eu", "job", []string{"t1"}, []string{"r1"}, []string{}, []string{}),
	}
	lo := loc.LocateOptions{}
	// Add restrictive filters that both items satisfy
	lo.Filters.Name = "n"
	lo.Filters.Category = "c"
	lo.Filters.Environment = "prod"
	lo.Filters.Perimeter = "eu"
	lo.Filters.Job = "job"
	lo.Filters.Tags = []string{"t1"}
	lo.Filters.Roots = []string{"r1"} // NOTE: Matches() requires all specified roots to be present; both have r1.

	kept, reasons := lo.Match(items, mustRFC3339(t, "2025-08-20T12:00:00Z"))
	if len(kept) != 2 {
		t.Fatalf("expected all matched items kept when HasPeriods=false, got %d", len(kept))
	}
	for _, it := range items {
		r := reasons[it.ItemID]
		if r.Action != "keep" || r.Note != "matched filters" {
			t.Fatalf("expected keep/matched filters, got %#v", r)
		}
	}
}

// ========== helpers ==========

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
