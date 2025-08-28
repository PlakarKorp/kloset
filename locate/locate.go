package locate

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/objects"
)

type SortOrder int

const (
	SortOrderNone       SortOrder = 0
	SortOrderAscending  SortOrder = 1
	SortOrderDescending SortOrder = -1
)

type Reason struct {
	Action string // "keep" or "delete"
	Rule   string // minutes/hours/days/weeks/months/years; empty if outside windows
	Bucket string // "2025-08-20" or "2025-08-20-14" or "W2025-34"
	Rank   int    // position within the bucket, newest-first, used for cap
	Cap    int    // cap applied for that bucket in the rule (max items per bucket)
	Note   string // human message: "outside retention windows"
}

type ItemFilters struct {
	Name        string
	Category    string
	Environment string
	Perimeter   string
	Job         string
	Tags        []string
	Roots       []string
}

func (it ItemFilters) HasTag(tag string) bool {
	if tag == "" {
		return true
	}
	for _, t := range it.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (it ItemFilters) HasRoot(root string) bool {
	if root == "" {
		return true
	}
	for _, t := range it.Roots {
		if t == root {
			return true
		}
	}
	return false
}

type Item struct {
	ItemID    objects.MAC
	Timestamp time.Time
	Filters   ItemFilters
}

type LocatePeriod struct {
	Keep int
	Cap  int
}

func (lp *LocatePeriod) Empty() bool {
	return lp.Keep == 0 && lp.Cap == 0
}

type LocateFilters struct {
	Before time.Time
	Since  time.Time

	Name        string
	Category    string
	Environment string
	Perimeter   string
	Job         string
	Tags        []string

	Latest bool
	IDs    []string
	Roots  []string
}

type LocateOptions struct {
	Filters LocateFilters

	Minute LocatePeriod
	Hour   LocatePeriod
	Day    LocatePeriod
	Week   LocatePeriod
	Month  LocatePeriod
	Year   LocatePeriod
}

func (lo *LocateOptions) HasPeriods() bool {
	return !lo.Minute.Empty() || !lo.Hour.Empty() || !lo.Day.Empty() ||
		!lo.Week.Empty() || !lo.Month.Empty() || !lo.Year.Empty()
}

type Option func(*LocateOptions)

func NewDefaultLocateOptions(opts ...Option) *LocateOptions {
	p := &LocateOptions{}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func WithKeepMinutes(n int) Option { return func(p *LocateOptions) { p.Minute.Keep = n } }
func WithKeepHours(n int) Option   { return func(p *LocateOptions) { p.Hour.Keep = n } }
func WithKeepDays(n int) Option    { return func(p *LocateOptions) { p.Day.Keep = n } }
func WithKeepWeeks(n int) Option   { return func(p *LocateOptions) { p.Week.Keep = n } }
func WithKeepMonths(n int) Option  { return func(p *LocateOptions) { p.Month.Keep = n } }
func WithKeepYears(n int) Option   { return func(p *LocateOptions) { p.Year.Keep = n } }

func WithPerMinuteCap(n int) Option { return func(p *LocateOptions) { p.Minute.Cap = n } }
func WithPerHourCap(n int) Option   { return func(p *LocateOptions) { p.Hour.Cap = n } }
func WithPerDayCap(n int) Option    { return func(p *LocateOptions) { p.Day.Cap = n } }
func WithPerWeekCap(n int) Option   { return func(p *LocateOptions) { p.Week.Cap = n } }
func WithPerMonthCap(n int) Option  { return func(p *LocateOptions) { p.Month.Cap = n } }
func WithPerYearCap(n int) Option   { return func(p *LocateOptions) { p.Year.Cap = n } }

func WithBefore(t time.Time) Option {
	return func(p *LocateOptions) { p.Filters.Before = t }
}
func WithSince(t time.Time) Option {
	return func(p *LocateOptions) { p.Filters.Since = t }
}
func WithName(name string) Option {
	return func(p *LocateOptions) { p.Filters.Name = name }
}
func WithCategory(category string) Option {
	return func(p *LocateOptions) { p.Filters.Category = category }
}
func WithEnvironment(env string) Option {
	return func(p *LocateOptions) { p.Filters.Environment = env }
}
func WithPerimeter(perimeter string) Option {
	return func(p *LocateOptions) { p.Filters.Perimeter = perimeter }
}
func WithJob(job string) Option {
	return func(p *LocateOptions) { p.Filters.Job = job }
}
func WithTag(tag string) Option {
	return func(p *LocateOptions) { p.Filters.Tags = append(p.Filters.Tags, tag) }
}
func WithID(id string) Option {
	return func(p *LocateOptions) { p.Filters.IDs = append(p.Filters.IDs, id) }
}
func WithLatest(latest bool) Option { return func(p *LocateOptions) { p.Filters.Latest = latest } }

func (lo *LocateOptions) Matches(it Item) bool {
	if len(lo.Filters.IDs) > 0 {
		found := false
		for _, id := range lo.Filters.IDs {
			if strings.HasPrefix(fmt.Sprintf("%x", it.ItemID), id) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// time window
	if !lo.Filters.Before.IsZero() && it.Timestamp.After(lo.Filters.Before.UTC()) {
		return false
	}
	if !lo.Filters.Since.IsZero() && it.Timestamp.Before(lo.Filters.Since.UTC()) {
		return false
	}

	// header fields
	if lo.Filters.Name != "" && it.Filters.Name != lo.Filters.Name {
		return false
	}
	if lo.Filters.Category != "" && it.Filters.Category != lo.Filters.Category {
		return false
	}
	if lo.Filters.Environment != "" && it.Filters.Environment != lo.Filters.Environment {
		return false
	}
	if lo.Filters.Perimeter != "" && it.Filters.Perimeter != lo.Filters.Perimeter {
		return false
	}
	if lo.Filters.Job != "" && it.Filters.Job != lo.Filters.Job {
		return false
	}
	if len(lo.Filters.Tags) > 0 {
		for _, tag := range lo.Filters.Tags {
			if !it.Filters.HasTag(tag) {
				return false
			}
		}
	}
	if len(lo.Filters.Roots) > 0 {
		for _, root := range lo.Filters.Roots {
			if !it.Filters.HasRoot(root) {
				return false
			}
		}
	}
	return true
}

func (lo *LocateOptions) FilterAndSort(items []Item) []Item {
	out := make([]Item, 0, len(items))
	for i := range items {
		it := items[i]
		it.Timestamp = it.Timestamp.UTC()
		if lo.Matches(it) {
			out = append(out, it)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	if lo.Filters.Latest && len(out) > 1 {
		return out[:1]
	}
	return out
}

func (lo *LocateOptions) Match(items []Item, now time.Time) (map[objects.MAC]struct{}, map[objects.MAC]Reason) {
	now = now.UTC()

	filtered := lo.FilterAndSort(items)

	kept := make(map[objects.MAC]struct{}, len(filtered))
	reasons := make(map[objects.MAC]Reason, len(filtered))

	// nothing matched, no need to go further
	if len(filtered) == 0 {
		return kept, reasons
	}

	// we won't group by periods
	if !lo.HasPeriods() {
		for _, s := range filtered {
			id := s.ItemID
			kept[id] = struct{}{}
			reasons[id] = Reason{
				Action: "keep", Note: "matched filters",
			}
		}
		return kept, reasons
	}

	ruleKeepReasons := make(map[objects.MAC]Reason, len(filtered)) // per-snapshot best keep reason
	ruleDropReasons := make(map[objects.MAC]Reason, len(filtered)) // per-snapshot best delete reason
	processRule := func(period Period, pp LocatePeriod) {
		if pp.Keep == 0 && pp.Cap == 0 {
			return
		}

		// 2) Keep==0 && Cap>0: consider ALL time buckets, keep up to Cap per bucket (since oldest)
		// 3) Keep>0  && Cap==0: consider last Keep buckets, keep ALL items in-window (no per-bucket cap)
		// 4) Keep>0  && Cap>0: consider last Keep buckets, keep up to Cap per bucket

		var windowKeys map[string]any
		if pp.Keep == 0 {
			windowKeys = make(map[string]any)
			for _, s := range filtered {
				windowKeys[period.Key(s.Timestamp)] = struct{}{}
			}
		} else {
			windowKeys = period.LastNKeys(now, pp.Keep)
		}

		buckets := make(map[string][]int)
		for idx, s := range filtered {
			k := period.Key(s.Timestamp)
			if _, ok := windowKeys[k]; ok {
				buckets[k] = append(buckets[k], idx)
			}
		}

		for bkey, idxs := range buckets {
			for rank, idx := range idxs {
				s := filtered[idx]
				id := s.ItemID
				if pp.Cap == 0 || rank < pp.Cap {
					r := Reason{
						Action: "keep",
						Rule:   period.Name,
						Bucket: bkey,
						Rank:   rank + 1,
						Cap:    pp.Cap, // 0 = unlimited
						Note:   "within bucket",
					}
					if prev, ok := ruleKeepReasons[id]; !ok || r.Rank < prev.Rank {
						ruleKeepReasons[id] = r
					}
				} else {
					r := Reason{
						Action: "delete",
						Rule:   period.Name,
						Bucket: bkey,
						Rank:   rank + 1,
						Cap:    pp.Cap,
						Note:   "exceeds per-bucket cap",
					}
					if prev, ok := ruleDropReasons[id]; !ok || r.Rank < prev.Rank {
						ruleDropReasons[id] = r
					}
				}
			}
		}
	}

	processRule(Minutes, lo.Minute)
	processRule(Hours, lo.Hour)
	processRule(Days, lo.Day)
	processRule(Weeks, lo.Week)
	processRule(Months, lo.Month)
	processRule(Years, lo.Year)

	// finalize decision for each snapshot
	for _, s := range filtered {
		id := s.ItemID
		if kr, ok := ruleKeepReasons[id]; ok {
			kept[id] = struct{}{}
			reasons[id] = kr
		} else if dr, ok := ruleDropReasons[id]; ok {
			reasons[id] = dr
		} else {
			reasons[id] = Reason{
				Action: "delete", Note: "outside retention windows",
			}
		}
	}

	return kept, reasons
}

func (lo *LocateOptions) Empty() bool {
	return lo.Minute.Keep == 0 && lo.Hour.Keep == 0 && lo.Day.Keep == 0 &&
		lo.Week.Keep == 0 && lo.Month.Keep == 0 && lo.Year.Keep == 0 &&
		lo.Minute.Cap == 0 && lo.Hour.Cap == 0 && lo.Day.Cap == 0 &&
		lo.Week.Cap == 0 && lo.Month.Cap == 0 && lo.Year.Cap == 0 &&
		lo.Filters.Name == "" && lo.Filters.Category == "" && lo.Filters.Environment == "" &&
		lo.Filters.Perimeter == "" && lo.Filters.Job == "" && len(lo.Filters.Tags) == 0 && len(lo.Filters.IDs) == 0 &&
		lo.Filters.Before.IsZero() && lo.Filters.Since.IsZero() && !lo.Filters.Latest
}
