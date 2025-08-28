package locate

import (
	"fmt"
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
		y, m, d := t.UTC().Date()
		return time.Date(y, m, d, t.Hour(), t.Minute(), 0, 0, time.UTC)
	},
	Prev: func(t time.Time) time.Time { return t.Add(-time.Minute) },
}

var Hours = Period{
	Name: "hour",
	Key:  func(t time.Time) string { return t.UTC().Format("2006-01-02-15") },
	Start: func(t time.Time) time.Time {
		y, m, d := t.UTC().Date()
		return time.Date(y, m, d, t.Hour(), 0, 0, 0, time.UTC)
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
	for range n {
		keys[period.Key(cur)] = nil
		cur = period.Prev(cur)
	}
	return keys
}
