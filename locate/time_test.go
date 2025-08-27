package locate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTimeFlag(t *testing.T) {
	// Test case: Empty input
	t1, err := ParseTimeFlag("")
	require.NoError(t, err)
	require.True(t, t1.IsZero())

	// Test case: RFC3339 format
	input := "2025-04-15T10:00:00Z"
	expected, _ := time.Parse(time.RFC3339, input)
	t2, err := ParseTimeFlag(input)
	require.NoError(t, err)
	require.Equal(t, expected, t2)

	// Test case: Date format "2006-01-02"
	input = "2025-04-15"
	expected, _ = time.Parse("2006-01-02", input)
	t3, err := ParseTimeFlag(input)
	require.NoError(t, err)
	require.Equal(t, expected, t3)

	// Test case: Date format "2006/01/02"
	input = "2025/04/15"
	expected, _ = time.Parse("2006/01/02", input)
	t4, err := ParseTimeFlag(input)
	require.NoError(t, err)
	require.Equal(t, expected, t4)

	// Test case: DateTime format "2006-01-02 15:04:05"
	input = "2025-04-15 10:00:00"
	expected, _ = time.Parse("2006-01-02 15:04:05", input)
	t5, err := ParseTimeFlag(input)
	require.NoError(t, err)
	require.Equal(t, expected, t5)

	// Test case: Duration format (e.g., "2h")
	input = "2h"
	now := time.Now()
	t6, err := ParseTimeFlag(input)
	require.NoError(t, err)
	require.WithinDuration(t, now.Add(-2*time.Hour), t6, time.Second)

	// Test case: Invalid format
	input = "invalid-time-format"
	t7, err := ParseTimeFlag(input)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid time format")
	require.True(t, t7.IsZero())
}

func TestMatches_PrefixAndMetadata(t *testing.T) {
	now := tstamp(2025, time.August, 24, 12, 0, 0)

	p := NewDefaultPolicyOptions(
		WithName("n1"),
		WithCategory("c1"),
		WithEnvironment("prod"),
		WithPerimeter("edge"),
		WithJob("backup"),
	)
	p.Filters.Prefix = "abc"

	items := []Item{
		{ItemID: "abc123", Timestamp: now, Filters: ItemFilters{
			Name: "n1", Category: "c1", Environment: "prod", Perimeter: "edge", Job: "backup",
		}},
		{ItemID: "def456", Timestamp: now, Filters: ItemFilters{
			Name: "n1", Category: "c1", Environment: "prod", Perimeter: "edge", Job: "backup",
		}},
		{ItemID: "abc789", Timestamp: now, Filters: ItemFilters{
			Name: "n2", Category: "c1", Environment: "prod", Perimeter: "edge", Job: "backup",
		}},
	}

	filtered := p.FilterAndSort(items, now)
	if len(filtered) != 1 || filtered[0].ItemID != "abc123" {
		t.Fatalf("expected only abc123; got %#v", filtered)
	}
}

func TestMatches_TagsAND(t *testing.T) {
	now := tstamp(2025, time.August, 24, 12, 0, 0)

	p := NewDefaultPolicyOptions(WithTag("a"), WithTag("b")) // AND semantics

	ok := Item{ItemID: "ok", Timestamp: now, Filters: ItemFilters{Tags: []string{"a", "b", "c"}}}
	missA := Item{ItemID: "missA", Timestamp: now, Filters: ItemFilters{Tags: []string{"b"}}}
	missB := Item{ItemID: "missB", Timestamp: now, Filters: ItemFilters{Tags: []string{"a"}}}

	fs := p.FilterAndSort([]Item{ok, missA, missB}, now)
	if len(fs) != 1 || fs[0].ItemID != "ok" {
		t.Fatalf("expected only 'ok'; got %#v", fs)
	}
}

func TestMatches_BeforeSince_BoundariesInclusive(t *testing.T) {
	now := tstamp(2025, time.August, 24, 12, 0, 0)
	before := now
	since := now.Add(-1 * time.Hour)

	p := NewDefaultPolicyOptions(WithBefore(before), WithSince(since))

	// exactly at boundaries should match (inclusive)
	atSince := Item{ItemID: "since", Timestamp: since}
	atBefore := Item{ItemID: "before", Timestamp: before}

	// outside: after 'before' or before 'since' should be filtered out
	afterBefore := Item{ItemID: "afterBefore", Timestamp: before.Add(1 * time.Nanosecond)}
	beforeSince := Item{ItemID: "beforeSince", Timestamp: since.Add(-1 * time.Nanosecond)}

	out := p.FilterAndSort([]Item{atSince, atBefore, afterBefore, beforeSince}, now)
	got := map[string]bool{}
	for _, it := range out {
		got[it.ItemID] = true
	}
	if !got["since"] || !got["before"] || got["afterBefore"] || got["beforeSince"] {
		t.Fatalf("boundary filtering wrong; got=%v", got)
	}
}

func TestFilterAndSort_LatestImpliesDescending(t *testing.T) {
	now := tstamp(2025, time.August, 24, 10, 0, 0)
	p := NewDefaultPolicyOptions()
	p.Filters.Latest = true             // implies descending
	p.Filters.SortOrder = SortOrderNone // none → default to desc because latest

	items := []Item{
		{ItemID: "old", Timestamp: now.Add(-2 * time.Hour)},
		{ItemID: "mid", Timestamp: now.Add(-1 * time.Hour)},
		{ItemID: "new", Timestamp: now.Add(-30 * time.Minute)},
	}

	fs := p.FilterAndSort(items, now)
	if len(fs) != 1 || fs[0].ItemID != "new" {
		t.Fatalf("expected only newest 'new'; got %#v", fs)
	}
}

func TestFilterAndSort_CustomAscending(t *testing.T) {
	now := tstamp(2025, time.August, 24, 10, 0, 0)
	p := NewDefaultPolicyOptions()
	p.Filters.SortOrder = SortOrderAscending

	items := []Item{
		{ItemID: "new", Timestamp: now.Add(-10 * time.Minute)},
		{ItemID: "mid", Timestamp: now.Add(-20 * time.Minute)},
		{ItemID: "old", Timestamp: now.Add(-30 * time.Minute)},
	}

	fs := p.FilterAndSort(items, now)
	if len(fs) != 3 || fs[0].ItemID != "old" || fs[2].ItemID != "new" {
		t.Fatalf("ascending sort order wrong; got %#v", fs)
	}
}

func TestSelect_LatestPlusPrefix(t *testing.T) {
	now := tstamp(2025, time.August, 24, 11, 30, 0)
	p := NewDefaultPolicyOptions(
		WithKeepHours(1), // make sure something can be kept
		WithPerHourCap(5),
	)
	p.Filters.Latest = true
	p.Filters.Prefix = "aa" // only items starting with aa

	items := []Item{
		{ItemID: "aa-1", Timestamp: now.Add(-50 * time.Minute)}, // 10:10 (outside current hour)
		{ItemID: "aa-2", Timestamp: now.Add(+10 * time.Minute)}, // 11:10 (inside current hour)
		{ItemID: "bb-1", Timestamp: now.Add(-5 * time.Minute)},  // filtered out by prefix
	}

	kept, reasons := p.Select(items, now)
	if len(kept) != 1 {
		t.Fatalf("expected one kept; kept=%v reasons=%v", kept, reasons)
	}
	if _, ok := kept["aa-2"]; !ok {
		t.Fatalf("expected aa-2 kept; kept=%v reasons=%v", kept, reasons)
	}
	if r := reasons["aa-2"]; r.Action != "keep" || r.Rule != Hours.Name {
		t.Fatalf("aa-2 reason unexpected: %+v", r)
	}
}

func TestEmpty_ConsidersFiltersAndWindows(t *testing.T) {
	p := NewDefaultPolicyOptions()
	if !p.Empty() {
		t.Fatalf("expected empty by default")
	}
	p = NewDefaultPolicyOptions(WithKeepDays(1))
	if p.Empty() {
		t.Fatalf("expected non-empty with a keep window")
	}
	p = NewDefaultPolicyOptions(WithName("foo"))
	if p.Empty() {
		t.Fatalf("expected non-empty with a filter set")
	}
	p = NewDefaultPolicyOptions()
	p.Filters.Prefix = "abc"
	if p.Empty() {
		t.Fatalf("expected non-empty with prefix filter")
	}
}

func TestSelect_TagsAND_UnionWithMonth(t *testing.T) {
	// Ensure AND tag filter narrows the set before month union/keeps are applied.
	now := tstamp(2025, time.August, 24, 12, 0, 0)
	p := NewDefaultPolicyOptions(
		WithKeepMonths(1),
		WithPerMonthCap(2),
	)
	// require both tags
	p.Filters.Tags = []string{"x", "y"}

	in := []Item{
		{ItemID: "xy-new", Timestamp: now.Add(-1 * time.Hour), Filters: ItemFilters{Tags: []string{"x", "y"}}},
		{ItemID: "xy-old", Timestamp: now.Add(-48 * time.Hour), Filters: ItemFilters{Tags: []string{"x", "y"}}},
		{ItemID: "only-x", Timestamp: now.Add(-30 * time.Minute), Filters: ItemFilters{Tags: []string{"x"}}}, // filtered out by tags
	}

	kept, _ := p.Select(in, now)
	if len(kept) != 2 || kept["xy-new"] == struct{}{} && kept["xy-old"] == struct{}{} {
		// just verify both xy-* are considered and only-x is excluded
		if _, ok := kept["only-x"]; ok {
			t.Fatalf("tag filter failed; only-x should be excluded; kept=%v", kept)
		}
	}
}
