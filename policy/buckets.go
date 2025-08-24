package policy

import (
	"fmt"
	"time"
)

// given a time.Time compute unambiguous bucket names for various time windows
func minuteKey(t time.Time) string { t = t.UTC(); return t.Format("2006-01-02-15:04") }
func hourKey(t time.Time) string   { t = t.UTC(); return t.Format("2006-01-02-15") }
func dayKey(t time.Time) string    { t = t.UTC(); return t.Format("2006-01-02") }
func monthKey(t time.Time) string  { t = t.UTC(); return t.Format("2006-01") }
func yearKey(t time.Time) string   { t = t.UTC(); return t.Format("2006") }
func isoWeekKey(t time.Time) string {
	y, w := t.UTC().ISOWeek()
	return fmt.Sprintf("W%04d-%02d", y, w)
}

// set of helpers to compute the start of various time windows
func startOfMinute(t time.Time) time.Time {
	t = t.UTC()
	y, m, d := t.Date()
	return time.Date(y, m, d, t.Hour(), t.Minute(), 0, 0, time.UTC)
}

func startOfHour(t time.Time) time.Time {
	t = t.UTC()
	y, m, d := t.Date()
	return time.Date(y, m, d, t.Hour(), 0, 0, 0, time.UTC)
}

func startOfDay(t time.Time) time.Time {
	y, m, d := t.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func startOfISOWeek(t time.Time) time.Time {
	t = startOfDay(t)
	wd := int(t.Weekday())
	if wd == 0 {
		wd = 7
	}
	return t.AddDate(0, 0, -(wd - 1))
}

func startOfMonth(t time.Time) time.Time {
	y, m, _ := t.UTC().Date()
	return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
}

func startOfYear(t time.Time) time.Time {
	y := t.UTC().Year()
	return time.Date(y, time.January, 1, 0, 0, 0, 0, time.UTC)
}

type (
	startFn = func(time.Time) time.Time
	prevFn  = func(time.Time) time.Time
	keyFn   = func(time.Time) string
)

// lastNKeys computes the last N keys for a given time window.
func lastNKeys(now time.Time, n int, start startFn, prev prevFn, key keyFn) map[string]struct{} {
	keys := make(map[string]struct{}, n)
	cur := start(now.UTC())
	for range n {
		keys[key(cur)] = struct{}{}
		cur = prev(cur)
	}
	return keys
}

// for each time window, compute the last N keys
func lastNMinutesKeys(now time.Time, n int) map[string]struct{} {
	return lastNKeys(now, n, startOfMinute, func(t time.Time) time.Time { return t.Add(-time.Minute) }, minuteKey)
}
func lastNHoursKeys(now time.Time, n int) map[string]struct{} {
	return lastNKeys(now, n, startOfHour, func(t time.Time) time.Time { return t.Add(-time.Hour) }, hourKey)
}
func lastNDaysKeys(now time.Time, n int) map[string]struct{} {
	return lastNKeys(now, n, startOfDay, func(t time.Time) time.Time { return t.AddDate(0, 0, -1) }, dayKey)
}
func lastNISOWeeksKeys(now time.Time, n int) map[string]struct{} {
	return lastNKeys(now, n, startOfISOWeek, func(t time.Time) time.Time { return t.AddDate(0, 0, -7) }, isoWeekKey)
}
func lastNMonthsKeys(now time.Time, n int) map[string]struct{} {
	return lastNKeys(now, n, startOfMonth, func(t time.Time) time.Time { return t.AddDate(0, -1, 0) }, monthKey)
}
func lastNYearsKeys(now time.Time, n int) map[string]struct{} {
	return lastNKeys(now, n, startOfYear, func(t time.Time) time.Time { return t.AddDate(-1, 0, 0) }, yearKey)
}
