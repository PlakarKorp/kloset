package policy

import (
	"strconv"
	"testing"
	"time"
)

func tstamp(y int, mon time.Month, d, h, m, s int) time.Time {
	return time.Date(y, mon, d, h, m, s, 0, time.UTC)
}

func TestNewDefaultPolicyOptions_DefaultCapsNormalizedToOne(t *testing.T) {
	p := NewDefaultPolicyOptions(
		WithKeepMinutes(5),
		WithPerMinuteCap(0), // should normalize to 1
	)
	if got, want := p.Minute.Cap, 1; got != want {
		t.Fatalf("cap normalization failed: got %d want %d", got, want)
	}
}

func TestSelect_MinuteBucket_CapAndRank(t *testing.T) {
	// Set now away from a minute boundary so -1s/-5s/-10s stay in the same minute.
	now := tstamp(2025, time.August, 24, 12, 30, 30)

	p := NewDefaultPolicyOptions(
		WithKeepMinutes(1),  // only this minute window
		WithPerMinuteCap(1), // keep newest 1 per minute
	)

	items := []Item{
		{ItemID: "A", Timestamp: now.Add(-10 * time.Second)},
		{ItemID: "B", Timestamp: now.Add(-5 * time.Second)},
		{ItemID: "C", Timestamp: now.Add(-1 * time.Second)}, // newest
	}

	kept, reasons := p.Select(items, now)

	if _, ok := kept["C"]; !ok {
		t.Fatalf("expected C kept; kept=%v", kept)
	}
	if _, ok := kept["A"]; ok {
		t.Fatalf("expected A not kept; kept=%v", kept)
	}
	if _, ok := kept["B"]; ok {
		t.Fatalf("expected B not kept; kept=%v", kept)
	}

	if r, ok := reasons["C"]; !ok || !(r.Action == "keep" && r.Rule == Minutes.Name && r.Rank == 1 && r.Cap == 1) {
		t.Fatalf("C reason unexpected: %+v (ok=%v)", r, ok)
	}
	if r, ok := reasons["B"]; !ok || !(r.Action == "delete" && r.Rule == Minutes.Name && r.Rank == 2 && r.Cap == 1 && r.Note == "exceeds per-bucket cap") {
		t.Fatalf("B reason unexpected: %+v (ok=%v)", r, ok)
	}
	if r, ok := reasons["A"]; !ok || !(r.Action == "delete" && r.Rule == Minutes.Name && r.Rank == 3 && r.Cap == 1 && r.Note == "exceeds per-bucket cap") {
		t.Fatalf("A reason unexpected: %+v (ok=%v)", r, ok)
	}
}

func TestSelect_HourBucket_UnionWithMinutes(t *testing.T) {
	// Set now away from an hour boundary so -30m stays within the same hour.
	now := tstamp(2025, time.August, 24, 15, 30, 0)

	p := NewDefaultPolicyOptions(
		WithKeepMinutes(0),
		WithKeepHours(1),
		WithPerHourCap(1),
	)

	item := Item{ItemID: "H", Timestamp: now.Add(-30 * time.Minute)} // same hour as 'now'
	kept, reasons := p.Select([]Item{item}, now)

	if _, ok := kept["H"]; !ok {
		t.Fatalf("expected H kept by hour rule; kept=%v", kept)
	}
	if r, ok := reasons["H"]; !ok || !(r.Action == "keep" && r.Rule == Hours.Name && r.Rank == 1) {
		t.Fatalf("H reason unexpected: %+v (ok=%v)", r, ok)
	}
}

func TestSelect_OutsideAllWindows(t *testing.T) {
	now := tstamp(2025, time.August, 24, 0, 0, 0)
	p := NewDefaultPolicyOptions() // no windows configured

	item := Item{ItemID: "Z", Timestamp: now.Add(-24 * time.Hour)}
	kept, reasons := p.Select([]Item{item}, now)

	if len(kept) != 0 {
		t.Fatalf("expected nothing kept; got %v", kept)
	}
	r, ok := reasons["Z"]
	if !ok || r.Action != "delete" || r.Rule != "" || r.Note != "outside retention windows" {
		t.Fatalf("unexpected reason for Z: %+v (ok=%v)", r, ok)
	}
}

func TestSelect_WeekBucket_CapAcrossDays(t *testing.T) {
	// ISO week window: multiple days in same ISO week share the bucket.
	now := tstamp(2025, time.August, 20, 10, 0, 0) // Wednesday
	p := NewDefaultPolicyOptions(
		WithKeepWeeks(1),
		WithPerWeekCap(1),
	)

	// Three items in the same ISO week, different days
	items := []Item{
		{ItemID: "Mon", Timestamp: Weeks.Start(now).Add(1 * time.Hour)},                // Monday 01:00
		{ItemID: "Tue", Timestamp: Weeks.Start(now).Add(24*time.Hour + 1*time.Hour)},   // Tuesday 01:00
		{ItemID: "Fri", Timestamp: Weeks.Start(now).Add(4*24*time.Hour + 1*time.Hour)}, // Friday 01:00 (newest)
	}

	kept, reasons := p.Select(items, now)

	if _, ok := kept["Fri"]; !ok {
		t.Fatalf("expected Fri kept; kept=%v", kept)
	}
	for _, id := range []string{"Mon", "Tue"} {
		if _, ok := kept[id]; ok {
			t.Fatalf("expected %s dropped by cap; kept=%v", id, kept)
		}
		if r, ok := reasons[id]; !ok || !(r.Action == "delete" && r.Rule == Weeks.Name && r.Note == "exceeds per-bucket cap") {
			t.Fatalf("%s reason unexpected: %+v (ok=%v)", id, r, ok)
		}
	}
}

func TestSelect_DayAndMonth_Union(t *testing.T) {
	// With month cap=2, the two newest items in the month are kept by the month rule.
	// We'll put two items on the same day (cap=1 for day) and one on a previous day.
	// Outcome:
	// - D1 kept by day
	// - D2 kept by month (month cap=2 keeps D1 and D2)
	// - M1 deleted by months (rank 3, exceeds cap)
	now := tstamp(2025, time.August, 24, 18, 0, 0)
	p := NewDefaultPolicyOptions(
		WithKeepDays(1),
		WithPerDayCap(1),
		WithKeepMonths(1),
		WithPerMonthCap(2),
	)

	sameDayNew := Item{ItemID: "D1", Timestamp: now.Add(-10 * time.Minute)} // newest in day/month
	sameDayOld := Item{ItemID: "D2", Timestamp: now.Add(-50 * time.Minute)} // same day, loses day cap
	prevDay := Item{ItemID: "M1", Timestamp: now.Add(-24 * time.Hour)}      // previous day, same month (oldest)

	kept, reasons := p.Select([]Item{sameDayOld, sameDayNew, prevDay}, now)

	// D1 must be kept by day
	if r, ok := reasons["D1"]; !ok || r.Action != "keep" || r.Rule != Days.Name {
		t.Fatalf("D1 should be kept by day: kept=%v reason=%+v (ok=%v)", kept, r, ok)
	}

	// D2 not kept by day (cap), BUT should be kept by month (union)
	if r, ok := reasons["D2"]; !ok || r.Action != "keep" || r.Rule != Months.Name {
		t.Fatalf("D2 should be kept by month: kept=%v reason=%+v (ok=%v)", kept, r, ok)
	}

	// M1 is 3rd-newest in the month; month cap=2 keeps D1 & D2 → M1 dropped by months.
	if r, ok := reasons["M1"]; !ok || !(r.Action == "delete" && r.Rule == Months.Name && r.Rank == 3 && r.Cap == 2 && r.Note == "exceeds per-bucket cap") {
		t.Fatalf("M1 should be deleted by months: kept=%v reason=%+v (ok=%v)", kept, r, ok)
	}
}

func TestSelect_SortingNewestFirstWithinBucket(t *testing.T) {
	// Keep 1 hour, cap=2, and ensure all items are in the same hour bucket.
	now := tstamp(2025, time.August, 24, 14, 30, 0)
	p := NewDefaultPolicyOptions(
		WithKeepHours(1),
		WithPerHourCap(2),
	)

	items := []Item{
		{ItemID: "old", Timestamp: now.Add(-20 * time.Minute)}, // 14:10
		{ItemID: "mid", Timestamp: now.Add(-10 * time.Minute)}, // 14:20
		{ItemID: "new", Timestamp: now.Add(-5 * time.Minute)},  // 14:25 (newest)
	}
	_, reasons := p.Select(items, now)

	rNew, okNew := reasons["new"]
	rMid, okMid := reasons["mid"]
	rOld, okOld := reasons["old"]
	if !okNew || !okMid || !okOld {
		t.Fatalf("missing reasons: new(%v) mid(%v) old(%v)", okNew, okMid, okOld)
	}
	if rNew.Rank != 1 || rMid.Rank != 2 || rOld.Rank != 3 {
		t.Fatalf("rank order unexpected: new=%d mid=%d old=%d", rNew.Rank, rMid.Rank, rOld.Rank)
	}
}

// Optional: quick builder for auto IDs if needed in future tests.
func autoItems(base time.Time, offsets ...time.Duration) []Item {
	res := make([]Item, len(offsets))
	for i, off := range offsets {
		res[i] = Item{
			ItemID:    "id-" + strconv.Itoa(i),
			Timestamp: base.Add(off),
		}
	}
	return res
}
