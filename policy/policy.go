package policy

import (
	"flag"
	"sort"
	"time"
)

type Reason struct {
	Action string // "keep" or "delete"
	Rule   string // minutes/hours/days/weeks/months/years; empty if outside windows
	Bucket string // "2025-08-20" or "2025-08-20-14" or "W2025-34"
	Rank   int    // position within the bucket, newest-first, used for cap
	Cap    int    // cap applied for that bucket in the rule (max items per bucket)
	Note   string // human message: "outside retention windows"
}

type Item struct {
	ItemID    string
	Timestamp time.Time
}

type PeriodPolicy struct {
	Keep int
	Cap  int
}

type PolicyOptions struct {
	Minute PeriodPolicy
	Hour   PeriodPolicy
	Day    PeriodPolicy
	Week   PeriodPolicy
	Month  PeriodPolicy
	Year   PeriodPolicy
}

type Option func(*PolicyOptions)

func NewDefaultPolicyOptions(opts ...Option) *PolicyOptions {
	p := &PolicyOptions{}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func WithKeepMinutes(n int) Option { return func(p *PolicyOptions) { p.Minute.Keep = n } }
func WithKeepHours(n int) Option   { return func(p *PolicyOptions) { p.Hour.Keep = n } }
func WithKeepDays(n int) Option    { return func(p *PolicyOptions) { p.Day.Keep = n } }
func WithKeepWeeks(n int) Option   { return func(p *PolicyOptions) { p.Week.Keep = n } }
func WithKeepMonths(n int) Option  { return func(p *PolicyOptions) { p.Month.Keep = n } }
func WithKeepYears(n int) Option   { return func(p *PolicyOptions) { p.Year.Keep = n } }

func WithPerMinuteCap(n int) Option { return func(p *PolicyOptions) { p.Minute.Cap = n } }
func WithPerHourCap(n int) Option   { return func(p *PolicyOptions) { p.Hour.Cap = n } }
func WithPerDayCap(n int) Option    { return func(p *PolicyOptions) { p.Day.Cap = n } }
func WithPerWeekCap(n int) Option   { return func(p *PolicyOptions) { p.Week.Cap = n } }
func WithPerMonthCap(n int) Option  { return func(p *PolicyOptions) { p.Month.Cap = n } }
func WithPerYearCap(n int) Option   { return func(p *PolicyOptions) { p.Year.Cap = n } }

func (po *PolicyOptions) Select(items []Item, now time.Time) (map[string]struct{}, map[string]Reason) {
	now = now.UTC()

	// copy + normalize timestamps + sort newest-first
	itemsCopy := make([]Item, len(items))
	for i := range items {
		itemsCopy[i] = items[i]
		itemsCopy[i].Timestamp = items[i].Timestamp.UTC()
	}
	sort.Slice(itemsCopy, func(i, j int) bool { return itemsCopy[i].Timestamp.After(itemsCopy[j].Timestamp) })

	kept := make(map[string]struct{}, len(itemsCopy))
	reasons := make(map[string]Reason, len(itemsCopy))
	ruleKeepReasons := make(map[string]Reason, len(itemsCopy)) // per-snapshot best keep reason
	ruleDropReasons := make(map[string]Reason, len(itemsCopy)) // per-snapshot best delete reason

	processRule := func(period Period, pp PeriodPolicy) {
		if pp.Keep <= 0 {
			return
		}
		capPer := max(pp.Cap, 1)

		windowKeys := period.LastNKeys(now, pp.Keep)

		// bucket indices (items already newest-first)
		buckets := make(map[string][]int)
		for idx, s := range itemsCopy {
			k := period.Key(s.Timestamp)
			if _, ok := windowKeys[k]; ok {
				buckets[k] = append(buckets[k], idx)
			}
		}

		// assign reasons (top K kept; rest exceed cap)
		for bkey, idxs := range buckets {
			for rank, idx := range idxs {
				s := itemsCopy[idx]
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
	for _, s := range itemsCopy {
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

func (po *PolicyOptions) InstallFlags(flags *flag.FlagSet) {
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
}

func (po *PolicyOptions) Empty() bool {
	return po.Minute.Keep == 0 && po.Hour.Keep == 0 && po.Day.Keep == 0 &&
		po.Week.Keep == 0 && po.Month.Keep == 0 && po.Year.Keep == 0
}
