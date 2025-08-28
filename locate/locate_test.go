package locate

import (
	"sort"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
)

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
	return objects.MAC([]byte{n})
}

// makeItem creates a test Item.
func makeItem(t *testing.T, id objects.MAC, ts string, name string, cat string, env string, perim string, job string, tags []string, roots []string) Item {
	t.Helper()
	return Item{
		ItemID:    id,
		Timestamp: mustRFC3339(t, ts),
		Filters: ItemFilters{
			Name:        name,
			Category:    cat,
			Environment: env,
			Perimeter:   perim,
			Job:         job,
			Tags:        tags,
			Roots:       roots,
		},
	}
}

// ========== Options wiring / HasPeriods / Empty ==========

func TestNewDefaultLocateOptions_And_WithOptions(t *testing.T) {
	now := mustRFC3339(t, "2025-08-28T12:00:00Z")
	lo := NewDefaultLocateOptions(
		WithKeepMinutes(3),
		WithPerHourCap(5),
		WithBefore(now),
		WithSince(now.Add(-24*time.Hour)),
		WithName("n"),
		WithCategory("c"),
		WithEnvironment("prod"),
		WithPerimeter("eu"),
		WithJob("backup"),
		WithTag("t1"),
		WithID("abc"),
		WithLatest(true),
	)
	if !lo.HasPeriods() {
		t.Fatalf("HasPeriods should be true when any keep/cap is set")
	}
	if lo.Minute.Keep != 3 || lo.Hour.Cap != 5 {
		t.Fatalf("options not applied: minute.keep=%d hour.cap=%d", lo.Minute.Keep, lo.Hour.Cap)
	}
	if lo.Filters.Before.IsZero() || lo.Filters.Since.IsZero() {
		t.Fatalf("before/since not set")
	}
	if lo.Filters.Name != "n" || lo.Filters.Category != "c" || lo.Filters.Environment != "prod" ||
		lo.Filters.Perimeter != "eu" || lo.Filters.Job != "backup" || !lo.Filters.Latest {
		t.Fatalf("filters not set correctly: %+v", lo.Filters)
	}
	if got := len(lo.Filters.Tags); got != 1 {
		t.Fatalf("tags len: got %d want 1", got)
	}
	if got := len(lo.Filters.IDs); got != 1 {
		t.Fatalf("IDs len: got %d want 1", got)
	}
}

func TestLocateOptions_Empty(t *testing.T) {
	var lo LocateOptions
	if !lo.Empty() {
		t.Fatalf("Empty() should be true on zero-value")
	}
	lo.Filters.Name = "x"
	if lo.Empty() {
		t.Fatalf("Empty() should be false when a filter is set")
	}
	lo = LocateOptions{}
	lo.Minute.Keep = 1
	if lo.Empty() {
		t.Fatalf("Empty() should be false when a period keep is set")
	}
}

func TestHasPeriods(t *testing.T) {
	var lo LocateOptions
	if lo.HasPeriods() {
		t.Fatalf("HasPeriods false on zero-value")
	}
	lo.Minute.Cap = 1
	if !lo.HasPeriods() {
		t.Fatalf("HasPeriods true when any cap/keep is set")
	}
}

// ========== Matches (IDs / time windows / headers / tags / roots) ==========

func TestMatches_IDPrefix(t *testing.T) {
	var lo LocateOptions
	idZero := objects.NilMac
	it := Item{ItemID: idZero, Timestamp: mustRFC3339(t, "2025-08-20T10:00:00Z")}
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
	itemBefore := Item{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T11:00:00Z")}
	itemAfter := Item{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T13:00:00Z")}

	// Before: reject items strictly AFTER the 'before' instant; equal allowed.
	var lo LocateOptions
	lo.Filters.Before = now
	if !lo.Matches(itemBefore) {
		t.Fatalf("itemBefore should match with Before=now")
	}
	if !lo.Matches(Item{ItemID: mac(3), Timestamp: now}) {
		t.Fatalf("item at exact Before should match (not After)")
	}
	if lo.Matches(itemAfter) {
		t.Fatalf("itemAfter should not match (is After Before)")
	}

	// Since: reject items strictly BEFORE the 'since' instant; equal allowed.
	lo = LocateOptions{}
	lo.Filters.Since = now
	if lo.Matches(itemBefore) {
		t.Fatalf("itemBefore should not match (Before Since)")
	}
	if !lo.Matches(Item{ItemID: mac(4), Timestamp: now}) {
		t.Fatalf("item at exact Since should match")
	}
	if !lo.Matches(itemAfter) {
		t.Fatalf("itemAfter should match (After Since)")
	}
}

func TestMatches_Headers_Tags_Roots(t *testing.T) {
	it := makeItem(t, mac(1), "2025-08-20T10:00:00Z", "n1", "cat", "prod", "eu", "backup", []string{"t1", "t2"}, []string{"r1", "r2"})

	tests := []struct {
		name string
		cfg  func(*LocateOptions)
		want bool
	}{
		{"name ok", func(lo *LocateOptions) { lo.Filters.Name = "n1" }, true},
		{"name mismatch", func(lo *LocateOptions) { lo.Filters.Name = "n2" }, false},
		{"category ok", func(lo *LocateOptions) { lo.Filters.Category = "cat" }, true},
		{"env mismatch", func(lo *LocateOptions) { lo.Filters.Environment = "stage" }, false},
		{"perimeter ok", func(lo *LocateOptions) { lo.Filters.Perimeter = "eu" }, true},
		{"job mismatch", func(lo *LocateOptions) { lo.Filters.Job = "restore" }, false},
		{"tags all-present", func(lo *LocateOptions) { lo.Filters.Tags = []string{"t1", "t2"} }, true},
		{"tags missing-one", func(lo *LocateOptions) { lo.Filters.Tags = []string{"t1", "t3"} }, false},
		{"roots all-present", func(lo *LocateOptions) { lo.Filters.Roots = []string{"r1", "r2"} }, true},
		{"roots missing-one", func(lo *LocateOptions) { lo.Filters.Roots = []string{"r1", "r3"} }, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var lo LocateOptions
			tc.cfg(&lo)
			if got := lo.Matches(it); got != tc.want {
				t.Fatalf("got %v want %v for %s", got, tc.want, tc.name)
			}
		})
	}
}

// ========== FilterAndSort (ordering + Latest) ==========

func TestFilterAndSort_OrderingAndLatest(t *testing.T) {
	items := []Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T12:00:00Z")},
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T13:00:00Z")},
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-19T23:59:59Z")},
	}
	var lo LocateOptions
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
	items := []Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T11:00:00Z")}, // day 2025-08-20
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-19T10:00:00Z")}, // day 2025-08-19
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-18T09:00:00Z")}, // day 2025-08-18
		{ItemID: mac(4), Timestamp: mustRFC3339(t, "2025-08-17T09:00:00Z")}, // outside (if Keep=3)
	}
	lo := LocateOptions{Day: LocatePeriod{Keep: 3}}
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
	items := []Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T12:00:20Z")}, // newest within bucket
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T12:00:10Z")},
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-20T12:00:00Z")}, // oldest within bucket
	}
	// Ensure FilterAndSort will order desc by ts (newest first)
	lo := LocateOptions{Minute: LocatePeriod{Cap: 2}}
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
	items := []Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T17:59:59Z")}, // hour 17
		{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T17:30:00Z")}, // hour 17
		{ItemID: mac(3), Timestamp: mustRFC3339(t, "2025-08-20T17:00:01Z")}, // hour 17
		{ItemID: mac(4), Timestamp: mustRFC3339(t, "2025-08-20T16:59:59Z")}, // hour 16
		{ItemID: mac(5), Timestamp: mustRFC3339(t, "2025-08-20T16:30:00Z")}, // hour 16
	}

	// Keep last 2 hours (current hour 17 and previous hour 16), capped at 2 per hour.
	lo := LocateOptions{Hour: LocatePeriod{Keep: 2, Cap: 2}}
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
	item1 := Item{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-20T08:00:00Z")}
	item2 := Item{ItemID: mac(2), Timestamp: mustRFC3339(t, "2025-08-20T07:00:00Z")}
	items := []Item{item1, item2}

	lo := LocateOptions{
		Day:  LocatePeriod{Keep: 1, Cap: 1}, // keep only the newest within the day
		Week: LocatePeriod{Keep: 1, Cap: 2}, // same week key, allow both
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
	items := []Item{
		{ItemID: mac(1), Timestamp: mustRFC3339(t, "2025-08-10T00:00:00Z")},
	}
	lo := LocateOptions{Day: LocatePeriod{Keep: 1}}
	_, reasons := lo.Match(items, now)
	r := reasons[items[0].ItemID]
	if r.Action != "delete" || r.Note != "outside retention windows" {
		t.Fatalf("expected outside retention windows delete, got %#v", r)
	}
}

// Sanity: with no periods, Match should keep all matched-filter items and mark as "matched filters".
func TestMatch_NoPeriods_KeepsAllMatches(t *testing.T) {
	items := []Item{
		makeItem(t, mac(1), "2025-08-20T11:00:00Z", "n", "c", "prod", "eu", "job", []string{"t1"}, []string{"r1"}),
		makeItem(t, mac(2), "2025-08-18T11:00:00Z", "n", "c", "prod", "eu", "job", []string{"t1"}, []string{"r1"}),
	}
	lo := LocateOptions{}
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
