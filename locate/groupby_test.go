package locate_test

import (
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func mac(b byte) objects.MAC {
	var m objects.MAC
	m[0] = b
	return m
}

func TestWithGroupBy(t *testing.T) {
	got := loc.NewDefaultLocateOptions(loc.WithGroupBy(loc.GroupByName))
	require.Equal(t, loc.GroupByName, got.GroupBy)
}

func TestEmptyConsidersGroupBy(t *testing.T) {
	lo := &loc.LocateOptions{}
	require.True(t, lo.Empty())
	lo.GroupBy = loc.GroupByName
	require.False(t, lo.Empty())
}

// Without periods, GroupBy must not change which items are kept: every filtered
// item is still kept regardless of grouping.
func TestMatchGroupByWithoutPeriodsIsNoOp(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Name: "a"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Name: "b"}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{Name: "a"}},
	}

	flat, _ := (&loc.LocateOptions{}).Match(items, now)
	grouped, _ := (&loc.LocateOptions{GroupBy: loc.GroupByName}).Match(items, now)
	require.Equal(t, flat, grouped)
	require.Len(t, grouped, 3)
}

// Filters apply first, then grouping, then per-group retention.
func TestMatchGroupByFiltersFirst(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		// kept by filter, group=alpha
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour),
			Filters: loc.ItemFilters{Name: "alpha", Perimeter: "foo", Tags: []string{"bar"}}},
		// kept by filter, group=beta
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour),
			Filters: loc.ItemFilters{Name: "beta", Perimeter: "foo", Tags: []string{"bar"}}},
		// filtered out (wrong perimeter), would otherwise be newest in alpha
		{ItemID: mac(3), Timestamp: now,
			Filters: loc.ItemFilters{Name: "alpha", Perimeter: "other", Tags: []string{"bar"}}},
		// filtered out (missing tag)
		{ItemID: mac(4), Timestamp: now,
			Filters: loc.ItemFilters{Name: "beta", Perimeter: "foo"}},
	}

	lo := &loc.LocateOptions{
		Filters: loc.LocateFilters{Perimeter: "foo", Tags: []string{"bar"}},
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByName,
	}
	kept, _ := lo.Match(items, now)

	require.Len(t, kept, 2)
	require.Contains(t, kept, mac(1))
	require.Contains(t, kept, mac(2))
	require.NotContains(t, kept, mac(3))
	require.NotContains(t, kept, mac(4))
}

// Retention runs independently per group, not over the merged set.
func TestMatchGroupByRetentionIsPerGroup(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(0x11), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Name: "alpha"}},
		{ItemID: mac(0x12), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Name: "alpha"}},
		{ItemID: mac(0x21), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{Name: "beta"}},
		{ItemID: mac(0x22), Timestamp: now.Add(-4 * time.Hour), Filters: loc.ItemFilters{Name: "beta"}},
		{ItemID: mac(0x31), Timestamp: now.Add(-5 * time.Hour), Filters: loc.ItemFilters{Name: "gamma"}},
	}
	periods := loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}}

	flat, _ := (&loc.LocateOptions{Periods: periods}).Match(items, now)
	require.Len(t, flat, 1)
	require.Contains(t, flat, mac(0x11))

	grouped, reasons := (&loc.LocateOptions{Periods: periods, GroupBy: loc.GroupByName}).Match(items, now)
	require.Len(t, grouped, 3)
	require.Contains(t, grouped, mac(0x11)) // newest of alpha
	require.Contains(t, grouped, mac(0x21)) // newest of beta
	require.Contains(t, grouped, mac(0x31)) // alone in gamma

	require.Equal(t, "keep", reasons[mac(0x11)].Action)
	require.Equal(t, "keep", reasons[mac(0x21)].Action)
	require.Equal(t, "keep", reasons[mac(0x31)].Action)
	require.Equal(t, "delete", reasons[mac(0x12)].Action)
	require.Equal(t, "delete", reasons[mac(0x22)].Action)
}

// Each non-multi-valued GroupBy key buckets correctly.
func TestMatchGroupByEachKey(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	mk := func(id byte, f loc.ItemFilters) loc.Item {
		return loc.Item{ItemID: mac(id), Timestamp: now.Add(-time.Duration(id) * time.Hour), Filters: f}
	}

	cases := []struct {
		name string
		key  loc.GroupByKey
		a, b loc.ItemFilters
	}{
		{"name", loc.GroupByName, loc.ItemFilters{Name: "x"}, loc.ItemFilters{Name: "y"}},
		{"category", loc.GroupByCategory, loc.ItemFilters{Category: "x"}, loc.ItemFilters{Category: "y"}},
		{"environment", loc.GroupByEnvironment, loc.ItemFilters{Environment: "x"}, loc.ItemFilters{Environment: "y"}},
		{"perimeter", loc.GroupByPerimeter, loc.ItemFilters{Perimeter: "x"}, loc.ItemFilters{Perimeter: "y"}},
		{"job", loc.GroupByJob, loc.ItemFilters{Job: "x"}, loc.ItemFilters{Job: "y"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := []loc.Item{
				mk(1, tc.a), mk(2, tc.a), // same group
				mk(3, tc.b), mk(4, tc.b), // same group
			}
			lo := &loc.LocateOptions{
				Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
				GroupBy: tc.key,
			}
			kept, _ := lo.Match(items, now)
			require.Len(t, kept, 2)
			require.Contains(t, kept, mac(1))
			require.Contains(t, kept, mac(3))
		})
	}
}

// Items with multi-valued fields (tag, origin, type, root) fan out: they
// participate in retention in every group they belong to, and keep wins
// over delete in the flattened reasons.
func TestMatchGroupByFanout(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name  string
		key   loc.GroupByKey
		setup func(b byte, vals ...string) loc.Item
	}{
		{"tag", loc.GroupByTag, func(b byte, v ...string) loc.Item {
			return loc.Item{ItemID: mac(b), Timestamp: now.Add(-time.Duration(b) * time.Hour),
				Filters: loc.ItemFilters{Tags: v}}
		}},
		{"origin", loc.GroupByOrigin, func(b byte, v ...string) loc.Item {
			return loc.Item{ItemID: mac(b), Timestamp: now.Add(-time.Duration(b) * time.Hour),
				Filters: loc.ItemFilters{Origins: v}}
		}},
		{"type", loc.GroupByType, func(b byte, v ...string) loc.Item {
			return loc.Item{ItemID: mac(b), Timestamp: now.Add(-time.Duration(b) * time.Hour),
				Filters: loc.ItemFilters{Types: v}}
		}},
		{"root", loc.GroupByRoot, func(b byte, v ...string) loc.Item {
			return loc.Item{ItemID: mac(b), Timestamp: now.Add(-time.Duration(b) * time.Hour),
				Filters: loc.ItemFilters{Roots: v}}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := []loc.Item{
				// newest, belongs to both groups
				tc.setup(1, "red", "blue"),
				// in red only, older
				tc.setup(2, "red"),
				// in blue only, oldest
				tc.setup(3, "blue"),
			}
			lo := &loc.LocateOptions{
				Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
				GroupBy: tc.key,
			}
			kept, reasons := lo.Match(items, now)

			// item 1 is newest in both groups so it wins both buckets;
			// items 2 and 3 are each capped out by item 1.
			require.Contains(t, kept, mac(1))
			require.NotContains(t, kept, mac(2))
			require.NotContains(t, kept, mac(3))
			require.Equal(t, "keep", reasons[mac(1)].Action)
			require.Equal(t, "delete", reasons[mac(2)].Action)
			require.Equal(t, "delete", reasons[mac(3)].Action)
		})
	}
}

// An item that is dropped in one fanout group but kept in another must end
// up "keep" in the flattened result.
func TestMatchGroupByFanoutKeepWins(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		// newest in red, also tagged blue
		{ItemID: mac(1), Timestamp: now.Add(-1 * 24 * time.Hour),
			Filters: loc.ItemFilters{Tags: []string{"red", "blue"}}},
		// in red only, same day as 1 → loses the red bucket
		{ItemID: mac(2), Timestamp: now.Add(-1*24*time.Hour - 1*time.Hour),
			Filters: loc.ItemFilters{Tags: []string{"red"}}},
		// alone in its own blue day
		{ItemID: mac(3), Timestamp: now.Add(-2 * 24 * time.Hour),
			Filters: loc.ItemFilters{Tags: []string{"blue"}}},
	}
	lo := &loc.LocateOptions{
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 3, Cap: 1}},
		GroupBy: loc.GroupByTag,
	}
	kept, reasons := lo.Match(items, now)

	require.Contains(t, kept, mac(1))
	require.NotContains(t, kept, mac(2))
	require.Contains(t, kept, mac(3))
	require.Equal(t, "keep", reasons[mac(1)].Action)
	require.Equal(t, "delete", reasons[mac(2)].Action)
	require.Equal(t, "keep", reasons[mac(3)].Action)
}

// Items missing the grouping field fall into the empty "" group together.
func TestMatchGroupByMissingFieldBucketsAsEmpty(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{Name: "alpha"}},
	}
	lo := &loc.LocateOptions{
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByName,
	}
	kept, _ := lo.Match(items, now)

	require.Len(t, kept, 2)
	require.Contains(t, kept, mac(1)) // newest of "" bucket
	require.Contains(t, kept, mac(3)) // alone in alpha
	require.NotContains(t, kept, mac(2))
}

// Latest interacts with grouping at filter time: it only keeps one item
// overall (the global newest), so retention then never sees the others.
func TestMatchGroupByLatestStillGlobal(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Name: "alpha"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Name: "beta"}},
	}
	lo := &loc.LocateOptions{
		Filters: loc.LocateFilters{Latest: true},
		GroupBy: loc.GroupByName,
	}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(1))
}

func TestMatchGroupByEmptyInput(t *testing.T) {
	lo := &loc.LocateOptions{GroupBy: loc.GroupByName,
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}}}
	kept, reasons := lo.Match(nil, time.Now())
	require.Empty(t, kept)
	require.Empty(t, reasons)
}

func TestMatchGroupByUnknownKeyFallsBack(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Name: "alpha"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Name: "beta"}},
	}
	lo := &loc.LocateOptions{
		GroupBy: loc.GroupByKey("not-a-key"),
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
	}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(1))
}
