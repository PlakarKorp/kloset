package locate

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"
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

type Item struct {
	ItemID    string
	Timestamp time.Time
	Filters   ItemFilters
}

type LocatePeriod struct {
	Keep int
	Cap  int
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

	SortOrder SortOrder
	Latest    bool
	Prefix    string // snapshot/item id prefix

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
func WithPrefix(prefix string) Option  { return func(p *LocateOptions) { p.Filters.Prefix = prefix } }
func WithLatest(latest bool) Option    { return func(p *LocateOptions) { p.Filters.Latest = latest } }
func WithSortOrder(o SortOrder) Option { return func(p *LocateOptions) { p.Filters.SortOrder = o } }

func (po *LocateOptions) Matches(it Item) bool {
	if po.Filters.Prefix != "" && !strings.HasPrefix(it.ItemID, po.Filters.Prefix) {
		return false
	}

	// time window
	if !po.Filters.Before.IsZero() && it.Timestamp.After(po.Filters.Before.UTC()) {
		return false
	}
	if !po.Filters.Since.IsZero() && it.Timestamp.Before(po.Filters.Since.UTC()) {
		return false
	}

	// header fields
	if po.Filters.Name != "" && it.Filters.Name != po.Filters.Name {
		return false
	}
	if po.Filters.Category != "" && it.Filters.Category != po.Filters.Category {
		return false
	}
	if po.Filters.Environment != "" && it.Filters.Environment != po.Filters.Environment {
		return false
	}
	if po.Filters.Perimeter != "" && it.Filters.Perimeter != po.Filters.Perimeter {
		return false
	}
	if po.Filters.Job != "" && it.Filters.Job != po.Filters.Job {
		return false
	}
	if len(po.Filters.Tags) > 0 {
		for _, tag := range po.Filters.Tags {
			if !it.Filters.HasTag(tag) {
				return false
			}
		}
	}
	return true
}

func (po *LocateOptions) FilterAndSort(items []Item, now time.Time) []Item {
	now = now.UTC()
	out := make([]Item, 0, len(items))
	for i := range items {
		it := items[i]
		it.Timestamp = it.Timestamp.UTC()
		if po.Matches(it) {
			out = append(out, it)
		}
	}
	sortOrder := po.Filters.SortOrder
	if po.Filters.Latest && sortOrder == SortOrderNone {
		sortOrder = SortOrderDescending
	}
	switch sortOrder {
	case SortOrderAscending:
		sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	case SortOrderDescending:
		sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	default:
		sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.After(out[j].Timestamp) })
	}
	if po.Filters.Latest && len(out) > 1 {
		return out[:1]
	}
	return out
}

func (po *LocateOptions) Select(items []Item, now time.Time) (map[string]struct{}, map[string]Reason) {
	now = now.UTC()

	filtered := po.FilterAndSort(items, now)

	kept := make(map[string]struct{}, len(filtered))
	reasons := make(map[string]Reason, len(filtered))
	ruleKeepReasons := make(map[string]Reason, len(filtered)) // per-snapshot best keep reason
	ruleDropReasons := make(map[string]Reason, len(filtered)) // per-snapshot best delete reason

	processRule := func(period Period, pp LocatePeriod) {
		if pp.Keep <= 0 {
			return
		}
		capPer := max(pp.Cap, 1)

		windowKeys := period.LastNKeys(now, pp.Keep)

		// bucket indices (items already newest-first)
		buckets := make(map[string][]int)
		for idx, s := range filtered {
			k := period.Key(s.Timestamp)
			if _, ok := windowKeys[k]; ok {
				buckets[k] = append(buckets[k], idx)
			}
		}

		// assign reasons (top K kept; rest exceed cap)
		for bkey, idxs := range buckets {
			for rank, idx := range idxs {
				s := filtered[idx]
				id := s.ItemID
				if rank < capPer {
					r := Reason{
						Action: "keep", Rule: period.Name, Bucket: bkey,
						Rank: rank + 1, Cap: capPer,
					}
					if prev, ok := ruleKeepReasons[id]; !ok || r.Rank < prev.Rank {
						ruleKeepReasons[id] = r
					}
				} else {
					r := Reason{
						Action: "delete", Rule: period.Name, Bucket: bkey,
						Rank: rank + 1, Cap: capPer, Note: "exceeds per-bucket cap",
					}
					if prev, ok := ruleDropReasons[id]; !ok || r.Rank < prev.Rank {
						ruleDropReasons[id] = r
					}
				}
			}
		}
	}

	// process for each period
	processRule(Minutes, po.Minute)
	processRule(Hours, po.Hour)
	processRule(Days, po.Day)
	processRule(Weeks, po.Week)
	processRule(Months, po.Month)
	processRule(Years, po.Year)

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

func (po *LocateOptions) InstallFlags(flags *flag.FlagSet) {
	flags.IntVar(&po.Minute.Keep, "keep-minutes", 0, "keep snapshots for the last N minutes")
	flags.IntVar(&po.Hour.Keep, "keep-hours", 0, "keep snapshots for the last N hours")
	flags.IntVar(&po.Day.Keep, "keep-days", 0, "keep snapshots for the last N days")
	flags.IntVar(&po.Week.Keep, "keep-weeks", 0, "keep snapshots for the last N weeks")
	flags.IntVar(&po.Month.Keep, "keep-months", 0, "keep snapshots for the last N months")
	flags.IntVar(&po.Year.Keep, "keep-years", 0, "keep snapshots for the last N years")
	flags.IntVar(&po.Minute.Cap, "keep-per-minute", 0, "cap the number of kept snapshots per minute")
	flags.IntVar(&po.Hour.Cap, "keep-per-hour", 0, "cap the number of kept snapshots per hour")
	flags.IntVar(&po.Day.Cap, "keep-per-day", 0, "cap the number of kept snapshots per day")
	flags.IntVar(&po.Week.Cap, "keep-per-week", 0, "cap the number of kept snapshots per week")
	flags.IntVar(&po.Month.Cap, "keep-per-month", 0, "cap the number of kept snapshots per month")
	flags.IntVar(&po.Year.Cap, "keep-per-year", 0, "cap the number of kept snapshots per year")

	flags.Var(NewTimeFlag(&po.Filters.Before), "before", "filter by date")
	flags.Var(NewTimeFlag(&po.Filters.Since), "since", "filter by date")

	flags.StringVar(&po.Filters.Name, "name", "", "filter by name")
	flags.StringVar(&po.Filters.Category, "category", "", "filter by category")
	flags.StringVar(&po.Filters.Environment, "environment", "", "filter by environment")
	flags.StringVar(&po.Filters.Perimeter, "perimeter", "", "filter by perimeter")
	flags.StringVar(&po.Filters.Job, "job", "", "filter by job")

	flags.Func("tag", "filter by tag (repeat or comma-separated). All specified tags must be present.",
		func(v string) error {
			for _, t := range strings.Split(v, ",") {
				t = strings.TrimSpace(t)
				if t != "" {
					po.Filters.Tags = append(po.Filters.Tags, t)
				}
			}
			return nil
		})

	flags.Func("order", "0=none, 1=asc (oldest-first), -1=desc (newest-first)",
		func(v string) error {
			switch strings.TrimSpace(v) {
			case "0", "none":
				po.Filters.SortOrder = SortOrderNone
			case "1", "asc", "ascending":
				po.Filters.SortOrder = SortOrderAscending
			case "-1", "desc", "descending":
				po.Filters.SortOrder = SortOrderDescending
			default:
				return fmt.Errorf("invalid sort-order: %q", v)
			}
			return nil
		})

	flags.BoolVar(&po.Filters.Latest, "latest", false, "consider only the latest matching item")
	flags.StringVar(&po.Filters.Prefix, "prefix", "", "filter by item ID prefix (hex)")
}

func (po *LocateOptions) Empty() bool {
	return po.Minute.Keep == 0 && po.Hour.Keep == 0 && po.Day.Keep == 0 &&
		po.Week.Keep == 0 && po.Month.Keep == 0 && po.Year.Keep == 0 &&
		po.Filters.Name == "" && po.Filters.Category == "" && po.Filters.Environment == "" &&
		po.Filters.Perimeter == "" && po.Filters.Job == "" && len(po.Filters.Tags) == 0 &&
		po.Filters.Prefix == "" && po.Filters.Before.IsZero() && po.Filters.Since.IsZero()
}
