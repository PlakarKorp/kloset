package locate

import (
	"fmt"
	"strings"
	"time"
)

type Period struct {
	Name  string
	Key   func(time.Time) string    // given a time.Time compute unambiguous bucket names for various time windows
	Start func(time.Time) time.Time // set of helpers to compute the start of various time windows
	Prev  func(time.Time) time.Time // lastNKeys computes the last N keys for a given time window.
}

var Minutes = Period{
	Name: "minute",
	Key:  func(t time.Time) string { return t.UTC().Format("2006-01-02-15:04") },
	Start: func(t time.Time) time.Time {
		ut := t.UTC()
		y, m, d := ut.Date()
		h, min, _ := ut.Clock()
		return time.Date(y, m, d, h, min, 0, 0, time.UTC)
	},
	Prev: func(t time.Time) time.Time { return t.Add(-time.Minute) },
}

var Hours = Period{
	Name: "hour",
	Key:  func(t time.Time) string { return t.UTC().Format("2006-01-02-15") },
	Start: func(t time.Time) time.Time {
		ut := t.UTC()
		y, m, d := ut.Date()
		h, _, _ := ut.Clock()
		return time.Date(y, m, d, h, 0, 0, 0, time.UTC)
	},
	Prev: func(t time.Time) time.Time { return t.Add(-time.Hour) },
}

var Days = Period{
	Name: "day",
	Key:  func(t time.Time) string { return t.UTC().Format("2006-01-02") },
	Start: func(t time.Time) time.Time {
		y, m, d := t.UTC().Date()
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	},
	Prev: func(t time.Time) time.Time { return t.AddDate(0, 0, -1) },
}

var Weeks = Period{
	Name: "week",
	Key: func(t time.Time) string {
		y, w := t.UTC().ISOWeek()
		return fmt.Sprintf("%04d-W%02d", y, w)
	},
	Start: func(t time.Time) time.Time {
		t = Days.Start(t)
		wd := int(t.Weekday())
		if wd == 0 {
			wd = 7
		}
		return t.AddDate(0, 0, -(wd - 1))
	},
	Prev: func(t time.Time) time.Time { return t.AddDate(0, 0, -7) },
}

var Months = Period{
	Name: "month",
	Key:  func(t time.Time) string { return t.UTC().Format("2006-01") },
	Start: func(t time.Time) time.Time {
		y, m, _ := t.UTC().Date()
		return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	},
	Prev: func(t time.Time) time.Time { return t.AddDate(0, -1, 0) },
}

var Years = Period{
	Name: "year",
	Key:  func(t time.Time) string { return t.UTC().Format("2006") },
	Start: func(t time.Time) time.Time {
		y := t.UTC().Year()
		return time.Date(y, time.January, 1, 0, 0, 0, 0, time.UTC)
	},
	Prev: func(t time.Time) time.Time { return t.AddDate(-1, 0, 0) },
}

func (period Period) LastNKeys(now time.Time, n int) map[string]any {
	keys := make(map[string]any)
	cur := period.Start(now)
	if cur.After(now) {
		cur = period.Prev(cur)
	}
	for range n {
		keys[period.Key(cur)] = nil
		cur = period.Prev(cur)
	}
	return keys
}

func WeekdayPeriod(target time.Weekday) Period {
	name := strings.ToLower(target.String())
	return Period{
		Name: name,
		Key: func(t time.Time) string {
			y, w := t.UTC().ISOWeek()
			weekday := strings.ToLower(t.UTC().Weekday().String())
			return fmt.Sprintf("%04d-W%02d-%s", y, w, weekday)
		},
		Start: func(t time.Time) time.Time {
			ut := Days.Start(t) // 00:00:00Z of the day
			wd := int(ut.Weekday())
			if wd == 0 {
				wd = 7 // Sunday -> 7
			}
			tw := int(target)
			if tw == 0 {
				tw = 7 // Sunday -> 7
			}
			// Monday start of the ISO week, then offset to the target weekday
			monday := ut.AddDate(0, 0, -(wd - 1))
			return monday.AddDate(0, 0, tw-1)
		},
		Prev: func(t time.Time) time.Time {
			// assumes t is at Start; still safe for any t because it’s a fixed step back
			return t.AddDate(0, 0, -7)
		},
	}
}

var (
	Mondays    = WeekdayPeriod(time.Monday)
	Tuesdays   = WeekdayPeriod(time.Tuesday)
	Wednesdays = WeekdayPeriod(time.Wednesday)
	Thursdays  = WeekdayPeriod(time.Thursday)
	Fridays    = WeekdayPeriod(time.Friday)
	Saturdays  = WeekdayPeriod(time.Saturday)
	Sundays    = WeekdayPeriod(time.Sunday)
)
