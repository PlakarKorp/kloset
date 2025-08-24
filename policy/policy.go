package policy

import (
	"flag"
	"sort"
	"time"
)

const (
	RuleMinutes = "minutes"
	RuleHours   = "hours"
	RuleDays    = "days"
	RuleWeeks   = "weeks"
	RuleMonths  = "months"
	RuleYears   = "years"
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

type PolicyOptions struct {
	// buckets
	KeepMinutes, KeepHours, KeepDays, KeepWeeks, KeepMonths, KeepYears int

	// caps
	KeepPerMinute, KeepPerHour, KeepPerDay, KeepPerWeek, KeepPerMonth, KeepPerYear int
}

type Option func(*PolicyOptions)

func NewDefaultPolicyOptions(opts ...Option) *PolicyOptions {
	p := &PolicyOptions{}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func WithKeepMinutes(n int) Option { return func(p *PolicyOptions) { p.KeepMinutes = n } }
func WithKeepHours(n int) Option   { return func(p *PolicyOptions) { p.KeepHours = n } }
func WithKeepDays(n int) Option    { return func(p *PolicyOptions) { p.KeepDays = n } }
func WithKeepWeeks(n int) Option   { return func(p *PolicyOptions) { p.KeepWeeks = n } }
func WithKeepMonths(n int) Option  { return func(p *PolicyOptions) { p.KeepMonths = n } }
func WithKeepYears(n int) Option   { return func(p *PolicyOptions) { p.KeepYears = n } }

func WithPerMinuteCap(n int) Option { return func(p *PolicyOptions) { p.KeepPerMinute = n } }
func WithPerHourCap(n int) Option   { return func(p *PolicyOptions) { p.KeepPerHour = n } }
func WithPerDayCap(n int) Option    { return func(p *PolicyOptions) { p.KeepPerDay = n } }
func WithPerWeekCap(n int) Option   { return func(p *PolicyOptions) { p.KeepPerWeek = n } }
func WithPerMonthCap(n int) Option  { return func(p *PolicyOptions) { p.KeepPerMonth = n } }
func WithPerYearCap(n int) Option   { return func(p *PolicyOptions) { p.KeepPerYear = n } }

func (p *PolicyOptions) Select(items []Item, now time.Time) (map[string]struct{}, map[string]Reason) {
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

	processRule := func(rule string, windowKeys map[string]struct{}, keyFn func(time.Time) string, capPer int) {
		if len(windowKeys) == 0 || capPer < 1 {
			return
		}

		// bucket indices (items already newest-first)
		buckets := make(map[string][]int)
		for idx, s := range itemsCopy {
			k := keyFn(s.Timestamp)
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
						Action: "keep", Rule: rule, Bucket: bkey,
						Rank: rank + 1, Cap: capPer,
					}
					if prev, ok := ruleKeepReasons[id]; !ok || r.Rank < prev.Rank {
						ruleKeepReasons[id] = r
					}
				} else {
					r := Reason{
						Action: "delete", Rule: rule, Bucket: bkey,
						Rank: rank + 1, Cap: capPer, Note: "exceeds per-bucket cap",
					}
					if prev, ok := ruleDropReasons[id]; !ok || r.Rank < prev.Rank {
						ruleDropReasons[id] = r
					}
				}
			}
		}
	}

	// windows
	if p.KeepMinutes > 0 {
		processRule(RuleMinutes, lastNMinutesKeys(now, p.KeepMinutes), minuteKey, capOr1(p.KeepPerMinute))
	}
	if p.KeepHours > 0 {
		processRule(RuleHours, lastNHoursKeys(now, p.KeepHours), hourKey, capOr1(p.KeepPerHour))
	}
	if p.KeepDays > 0 {
		processRule(RuleDays, lastNDaysKeys(now, p.KeepDays), dayKey, capOr1(p.KeepPerDay))
	}
	if p.KeepWeeks > 0 {
		processRule(RuleWeeks, lastNISOWeeksKeys(now, p.KeepWeeks), isoWeekKey, capOr1(p.KeepPerWeek))
	}
	if p.KeepMonths > 0 {
		processRule(RuleMonths, lastNMonthsKeys(now, p.KeepMonths), monthKey, capOr1(p.KeepPerMonth))
	}
	if p.KeepYears > 0 {
		processRule(RuleYears, lastNYearsKeys(now, p.KeepYears), yearKey, capOr1(p.KeepPerYear))
	}

	// finalize decision for each snapshot
	for _, s := range itemsCopy {
		id := s.ItemID
		if kr, ok := ruleKeepReasons[id]; ok {
			kept[id] = struct{}{}
			reasons[id] = kr
			continue
		}
		if dr, ok := ruleDropReasons[id]; ok {
			reasons[id] = dr
		} else {
			reasons[id] = Reason{
				Action: "delete", Note: "outside retention windows",
			}
		}
	}

	return kept, reasons
}

func capOr1(v int) int {
	if v < 1 {
		return 1
	}
	return v
}

func (po *PolicyOptions) InstallFlags(flags *flag.FlagSet) {
	flags.IntVar(&po.KeepMinutes, "keep-minutes", 0, "keep snapshots for the last N minutes")
	flags.IntVar(&po.KeepHours, "keep-hours", 0, "keep snapshots for the last N hours")
	flags.IntVar(&po.KeepDays, "keep-days", 0, "keep snapshots for the last N days")
	flags.IntVar(&po.KeepWeeks, "keep-weeks", 0, "keep snapshots for the last N weeks")
	flags.IntVar(&po.KeepMonths, "keep-months", 0, "keep snapshots for the last N months")
	flags.IntVar(&po.KeepYears, "keep-years", 0, "keep snapshots for the last N years")
	flags.IntVar(&po.KeepPerMinute, "keep-per-minute", 1, "cap the number of kept snapshots per minute")
	flags.IntVar(&po.KeepPerHour, "keep-per-hour", 1, "cap the number of kept snapshots per hour")
	flags.IntVar(&po.KeepPerDay, "keep-per-day", 1, "cap the number of kept snapshots per day")
	flags.IntVar(&po.KeepPerWeek, "keep-per-week", 1, "cap the number of kept snapshots per week")
	flags.IntVar(&po.KeepPerMonth, "keep-per-month", 1, "cap the number of kept snapshots per month")
	flags.IntVar(&po.KeepPerYear, "keep-per-year", 1, "cap the number of kept snapshots per year")
}

func (po *PolicyOptions) Empty() bool {
	return po.KeepMinutes == 0 && po.KeepHours == 0 && po.KeepDays == 0 &&
		po.KeepWeeks == 0 && po.KeepMonths == 0 && po.KeepYears == 0 &&
		po.KeepPerMinute <= 1 && po.KeepPerHour <= 1 && po.KeepPerDay <= 1 &&
		po.KeepPerWeek <= 1 && po.KeepPerMonth <= 1 && po.KeepPerYear <= 1
}
