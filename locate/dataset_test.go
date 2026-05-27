package locate_test

import (
	"flag"
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/stretchr/testify/require"
)

// ---- ItemFilters helpers --------------------------------------------------

func TestHasDataClass(t *testing.T) {
	cases := []struct {
		name   string
		it     loc.ItemFilters
		class  string
		expect bool
	}{
		{"empty input matches", loc.ItemFilters{DataClasses: []string{"pii"}}, "", true},
		{"missing class", loc.ItemFilters{DataClasses: []string{"pii"}}, "phi", false},
		{"present class", loc.ItemFilters{DataClasses: []string{"pii", "phi"}}, "phi", true},
		{"nil classes", loc.ItemFilters{}, "pii", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.it.HasDataClass(tc.class))
		})
	}
}

func TestHasDataClasses(t *testing.T) {
	it := loc.ItemFilters{DataClasses: []string{"pii", "phi"}}
	require.True(t, it.HasDataClasses(nil), "no classes requested ⇒ match")
	require.True(t, it.HasDataClasses([]string{}), "empty classes requested ⇒ match")
	require.True(t, it.HasDataClasses([]string{"phi"}), "any-match semantics: present")
	require.True(t, it.HasDataClasses([]string{"phi", "secret"}), "any-match: at least one present")
	require.False(t, it.HasDataClasses([]string{"secret"}), "all absent ⇒ no match")
	require.False(t, loc.ItemFilters{}.HasDataClasses([]string{"pii"}), "nil item classes ⇒ no match")
}

// ---- Options & flag plumbing ----------------------------------------------

func TestWithDataset(t *testing.T) {
	got := loc.NewDefaultLocateOptions(loc.WithDataset("customers"))
	require.Equal(t, "customers", got.Filters.Dataset)
}

func TestWithDataClassAppends(t *testing.T) {
	got := loc.NewDefaultLocateOptions(loc.WithDataClass("pii"), loc.WithDataClass("phi"))
	require.Equal(t, []string{"pii", "phi"}, got.Filters.DataClasses)
}

func TestEmptyConsidersDatasetAndDataClass(t *testing.T) {
	lo := &loc.LocateOptions{}
	require.True(t, lo.Empty())

	lo.Filters.Dataset = "customers"
	require.False(t, lo.Empty())
	lo.Filters.Dataset = ""

	lo.Filters.DataClasses = []string{"pii"}
	require.False(t, lo.Empty())
}

func TestInstallLocateFlagsDataset(t *testing.T) {
	lo := &loc.LocateOptions{}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	lo.InstallLocateFlags(fs)
	require.NoError(t, fs.Parse([]string{"-dataset", "customers"}))
	require.Equal(t, "customers", lo.Filters.Dataset)
}

func TestInstallLocateFlagsDataClassRepeatAndCSV(t *testing.T) {
	lo := &loc.LocateOptions{}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	lo.InstallLocateFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"-data-class", "pii",
		"-data-class", "phi,secret",
	}))
	require.Equal(t, []string{"pii", "phi", "secret"}, lo.Filters.DataClasses)
}

func TestInstallLocateFlagsDataClassIgnoresBlanks(t *testing.T) {
	lo := &loc.LocateOptions{}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	lo.InstallLocateFlags(fs)
	require.NoError(t, fs.Parse([]string{"-data-class", " , pii ,  "}))
	require.Equal(t, []string{"pii"}, lo.Filters.DataClasses)
}

func TestInstallLocateFlagsGroupByDataset(t *testing.T) {
	lo := &loc.LocateOptions{}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	lo.InstallLocateFlags(fs)
	require.NoError(t, fs.Parse([]string{"-group-by", "dataset"}))
	require.Equal(t, loc.GroupByDataset, lo.GroupBy)
}

func TestInstallLocateFlagsGroupByDataClass(t *testing.T) {
	lo := &loc.LocateOptions{}
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	lo.InstallLocateFlags(fs)
	require.NoError(t, fs.Parse([]string{"-group-by", "data-class"}))
	require.Equal(t, loc.GroupByDataClass, lo.GroupBy)
}

// ---- Filtering behaviour --------------------------------------------------

func TestMatchesFilterDataset(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Dataset: "customers"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Dataset: "orders"}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{}},
	}
	lo := &loc.LocateOptions{Filters: loc.LocateFilters{Dataset: "customers"}}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(1))
}

// Dataset is single-valued: empty filter is a no-op, non-empty filter matches
// exactly and rejects items whose dataset is unset.
func TestMatchesFilterDatasetEmptyOnItem(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Dataset: ""}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Dataset: "customers"}},
	}
	// no filter ⇒ both kept
	kept, _ := (&loc.LocateOptions{}).Match(items, now)
	require.Len(t, kept, 2)
	// filter on a specific dataset ⇒ only that item
	kept, _ = (&loc.LocateOptions{Filters: loc.LocateFilters{Dataset: "customers"}}).Match(items, now)
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(2))
}

// -data-class is any-match across the requested classes (matches if the item
// has any one of them), mirroring -origin and -type. AND across classes is
// not supported, by design — match the existing convention.
func TestMatchesFilterDataClassAnyMatch(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{DataClasses: []string{"pii"}}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{DataClasses: []string{"phi", "secret"}}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{DataClasses: []string{"public"}}},
		{ItemID: mac(4), Timestamp: now.Add(-4 * time.Hour), Filters: loc.ItemFilters{}}, // no classes
	}

	// single class
	kept, _ := (&loc.LocateOptions{Filters: loc.LocateFilters{DataClasses: []string{"phi"}}}).Match(items, now)
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(2))

	// two classes ⇒ items 1 and 2 match (any-match)
	kept, _ = (&loc.LocateOptions{Filters: loc.LocateFilters{DataClasses: []string{"pii", "phi"}}}).Match(items, now)
	require.Len(t, kept, 2)
	require.Contains(t, kept, mac(1))
	require.Contains(t, kept, mac(2))

	// class no one carries
	kept, _ = (&loc.LocateOptions{Filters: loc.LocateFilters{DataClasses: []string{"nope"}}}).Match(items, now)
	require.Empty(t, kept)
}

// Combined Dataset + DataClasses are intersected with each other and with
// the rest of the filters (perimeter, tag, ...).
func TestMatchesFilterDatasetAndDataClassCombined(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour),
			Filters: loc.ItemFilters{Dataset: "customers", DataClasses: []string{"pii"}, Perimeter: "foo"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour),
			Filters: loc.ItemFilters{Dataset: "customers", DataClasses: []string{"public"}, Perimeter: "foo"}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour),
			Filters: loc.ItemFilters{Dataset: "orders", DataClasses: []string{"pii"}, Perimeter: "foo"}},
		{ItemID: mac(4), Timestamp: now.Add(-4 * time.Hour),
			Filters: loc.ItemFilters{Dataset: "customers", DataClasses: []string{"pii"}, Perimeter: "other"}},
	}
	lo := &loc.LocateOptions{Filters: loc.LocateFilters{
		Dataset:     "customers",
		DataClasses: []string{"pii"},
		Perimeter:   "foo",
	}}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(1))
}

// ---- Grouping behaviour ---------------------------------------------------

// -group-by dataset is single-valued: each snapshot lands in exactly one
// bucket; retention runs independently in each.
func TestMatchGroupByDataset(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(0x11), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Dataset: "customers"}},
		{ItemID: mac(0x12), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Dataset: "customers"}},
		{ItemID: mac(0x21), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{Dataset: "orders"}},
		{ItemID: mac(0x22), Timestamp: now.Add(-4 * time.Hour), Filters: loc.ItemFilters{Dataset: "orders"}},
	}
	periods := loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}}

	flat, _ := (&loc.LocateOptions{Periods: periods}).Match(items, now)
	require.Len(t, flat, 1, "without -group-by, retention keeps only the global newest")
	require.Contains(t, flat, mac(0x11))

	grouped, reasons := (&loc.LocateOptions{Periods: periods, GroupBy: loc.GroupByDataset}).Match(items, now)
	require.Len(t, grouped, 2)
	require.Contains(t, grouped, mac(0x11))
	require.Contains(t, grouped, mac(0x21))
	require.Equal(t, "delete", reasons[mac(0x12)].Action)
	require.Equal(t, "delete", reasons[mac(0x22)].Action)
}

// Items without a Dataset fall into the empty "" bucket together.
func TestMatchGroupByDatasetEmptyBucket(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{Dataset: "customers"}},
	}
	lo := &loc.LocateOptions{
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByDataset,
	}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 2)
	require.Contains(t, kept, mac(1)) // newest of "" bucket
	require.Contains(t, kept, mac(3)) // alone in "customers"
	require.NotContains(t, kept, mac(2))
}

// -group-by data-class is multi-valued: an item with multiple data classes
// participates in every group it belongs to (fan-out).
func TestMatchGroupByDataClassFanout(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		// newest in both pii and phi
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"pii", "phi"}}},
		// older pii
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"pii"}}},
		// older phi
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"phi"}}},
	}
	lo := &loc.LocateOptions{
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByDataClass,
	}
	kept, reasons := lo.Match(items, now)
	require.Contains(t, kept, mac(1))
	require.NotContains(t, kept, mac(2))
	require.NotContains(t, kept, mac(3))
	require.Equal(t, "keep", reasons[mac(1)].Action)
	require.Equal(t, "delete", reasons[mac(2)].Action)
	require.Equal(t, "delete", reasons[mac(3)].Action)
}

// Fan-out keep-wins: an item dropped in one class but kept in another ends
// up "keep" in the flattened result.
func TestMatchGroupByDataClassKeepWins(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		// newest in pii on day -1, also tagged phi
		{ItemID: mac(1), Timestamp: now.Add(-1 * 24 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"pii", "phi"}}},
		// in pii only, same day → loses in pii under item 1
		{ItemID: mac(2), Timestamp: now.Add(-1*24*time.Hour - 1*time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"pii"}}},
		// alone in its own phi day
		{ItemID: mac(3), Timestamp: now.Add(-2 * 24 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"phi"}}},
	}
	lo := &loc.LocateOptions{
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 3, Cap: 1}},
		GroupBy: loc.GroupByDataClass,
	}
	kept, reasons := lo.Match(items, now)
	require.Contains(t, kept, mac(1))
	require.NotContains(t, kept, mac(2))
	require.Contains(t, kept, mac(3))
	require.Equal(t, "keep", reasons[mac(1)].Action)
	require.Equal(t, "delete", reasons[mac(2)].Action)
	require.Equal(t, "keep", reasons[mac(3)].Action)
}

// Items with no data class fall into the "" bucket together when
// grouping by data-class.
func TestMatchGroupByDataClassNoClassesBucket(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{DataClasses: []string{"pii"}}},
	}
	lo := &loc.LocateOptions{
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByDataClass,
	}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 2)
	require.Contains(t, kept, mac(1)) // newest of "" bucket
	require.Contains(t, kept, mac(3))
	require.NotContains(t, kept, mac(2))
}

// Filter-then-group-then-retain ordering must hold for the new keys.
func TestMatchFiltersFirstThenGroupByDataset(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		// kept by perimeter filter
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour),
			Filters: loc.ItemFilters{Dataset: "customers", Perimeter: "foo"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour),
			Filters: loc.ItemFilters{Dataset: "orders", Perimeter: "foo"}},
		// dropped by perimeter filter (would otherwise be newest of "customers")
		{ItemID: mac(3), Timestamp: now,
			Filters: loc.ItemFilters{Dataset: "customers", Perimeter: "other"}},
	}
	lo := &loc.LocateOptions{
		Filters: loc.LocateFilters{Perimeter: "foo"},
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByDataset,
	}
	kept, _ := lo.Match(items, now)
	require.Len(t, kept, 2)
	require.Contains(t, kept, mac(1))
	require.Contains(t, kept, mac(2))
	require.NotContains(t, kept, mac(3), "filter must run before grouping")
}

// Combine -data-class filter with -group-by data-class: filter narrows
// the universe, then grouping/retention runs only over the survivors and
// only over the classes that still appear.
func TestMatchFilterDataClassThenGroupByDataClass(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		// matches the pii filter; also belongs to phi
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"pii", "phi"}}},
		// matches the pii filter, pii only
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"pii"}}},
		// dropped by filter
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour),
			Filters: loc.ItemFilters{DataClasses: []string{"public"}}},
	}
	lo := &loc.LocateOptions{
		Filters: loc.LocateFilters{DataClasses: []string{"pii"}},
		Periods: loc.LocatePeriods{Day: loc.LocatePeriod{Keep: 1, Cap: 1}},
		GroupBy: loc.GroupByDataClass,
	}
	kept, _ := lo.Match(items, now)
	// item 1 wins both pii and phi groups (newest); item 2 capped out in pii;
	// item 3 filtered out.
	require.Len(t, kept, 1)
	require.Contains(t, kept, mac(1))
}

// Without periods, group-by dataset/data-class is a no-op (all filtered
// items kept) — same property already covered for the other keys.
func TestMatchGroupByDatasetNoOpWithoutPeriods(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{Dataset: "customers"}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{Dataset: "orders"}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{}},
	}
	flat, _ := (&loc.LocateOptions{}).Match(items, now)
	grouped, _ := (&loc.LocateOptions{GroupBy: loc.GroupByDataset}).Match(items, now)
	require.Equal(t, flat, grouped)
	require.Len(t, grouped, 3)
}

func TestMatchGroupByDataClassNoOpWithoutPeriods(t *testing.T) {
	now := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	items := []loc.Item{
		{ItemID: mac(1), Timestamp: now.Add(-1 * time.Hour), Filters: loc.ItemFilters{DataClasses: []string{"pii"}}},
		{ItemID: mac(2), Timestamp: now.Add(-2 * time.Hour), Filters: loc.ItemFilters{DataClasses: []string{"phi"}}},
		{ItemID: mac(3), Timestamp: now.Add(-3 * time.Hour), Filters: loc.ItemFilters{}},
	}
	flat, _ := (&loc.LocateOptions{}).Match(items, now)
	grouped, _ := (&loc.LocateOptions{GroupBy: loc.GroupByDataClass}).Match(items, now)
	require.Equal(t, flat, grouped)
	require.Len(t, grouped, 3)
}
