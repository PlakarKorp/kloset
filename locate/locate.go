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
	Dataset     string
	Tags        []string
	Types       []string
	Origins     []string
	Roots       []string
	DataClasses []string
}

func (it *ItemFilters) HasTag(tag string) bool {
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

func (it ItemFilters) HasType(typ string) bool {
	if typ == "" {
		return true
	}
	for _, t := range it.Types {
		if t == typ {
			return true
		}
	}
	return false
}

func (it ItemFilters) HasOrigin(origin string) bool {
	if origin == "" {
		return true
	}
	for _, t := range it.Origins {
		if t == origin {
			return true
		}
	}
	return false
}

func (it ItemFilters) HasOrigins(origins []string) bool {
	if len(origins) == 0 {
		return true
	}
	for _, origin := range origins {
		if it.HasOrigin(origin) {
			return true
		}
	}
	return false
}

func (it ItemFilters) HasTypes(types []string) bool {
	if len(types) == 0 {
		return true
	}
	for _, t := range types {
		if it.HasType(t) {
			return true
		}
	}
	return false
}

func (it ItemFilters) HasDataClass(class string) bool {
	if class == "" {
		return true
	}
	for _, c := range it.DataClasses {
		if c == class {
			return true
		}
	}
	return false
}

func (it ItemFilters) HasDataClasses(classes []string) bool {
	if len(classes) == 0 {
		return true
	}
	for _, c := range classes {
		if it.HasDataClass(c) {
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
	Keep int `json:"keep,omitempty" yaml:"keep,omitempty"` // number of time buckets to keep, 0 = unlimited
	Cap  int `json:"cap,omitempty" yaml:"cap,omitempty"`   // max items to keep per time bucket, 0 = unlimited
}

func (lp *LocatePeriod) Empty() bool {
	return lp.Keep == 0 && lp.Cap == 0
}

type LocateFilters struct {
	Before time.Time `json:"before,omitzero" yaml:"before,omitempty"`
	Since  time.Time `json:"since,omitzero" yaml:"since,omitempty"`

	Name        string   `json:"name,omitempty" yaml:"name,omitempty"`
	Category    string   `json:"category,omitempty" yaml:"category,omitempty"`
	Environment string   `json:"environment,omitempty" yaml:"environment,omitempty"`
	Perimeter   string   `json:"perimeter,omitempty" yaml:"perimeter,omitempty"`
	Job         string   `json:"job,omitempty" yaml:"job,omitempty"`
	Dataset     string   `json:"dataset,omitempty" yaml:"dataset,omitempty"`
	Tags        []string `json:"tags,omitempty" yaml:"tags,omitempty"`
	IgnoreTags  []string `json:"ignore_tags,omitempty" yaml:"ignore_tags,omitempty"`

	Latest      bool     `json:"latest,omitempty" yaml:"latest,omitempty"` // if true, consider only the latest matching item
	IDs         []string `json:"ids,omitempty" yaml:"ids,omitempty"`
	Types       []string `json:"types,omitempty" yaml:"types,omitempty"`
	Origins     []string `json:"origins,omitempty" yaml:"origins,omitempty"`
	Roots       []string `json:"roots,omitempty" yaml:"roots,omitempty"`
	DataClasses []string `json:"data_classes,omitempty" yaml:"data_classes,omitempty"`
}

type LocatePeriods struct {
	Minute LocatePeriod `json:"minute,omitzero" yaml:"minute,omitempty"`
	Hour   LocatePeriod `json:"hour,omitzero" yaml:"hour,omitempty"`
	Day    LocatePeriod `json:"day,omitzero" yaml:"day,omitempty"`
	Week   LocatePeriod `json:"week,omitzero" yaml:"week,omitempty"`
	Month  LocatePeriod `json:"month,omitzero" yaml:"month,omitempty"`
	Year   LocatePeriod `json:"year,omitzero" yaml:"year,omitempty"`

	Monday    LocatePeriod `json:"monday,omitzero" yaml:"monday,omitempty"`
	Tuesday   LocatePeriod `json:"tuesday,omitzero" yaml:"tuesday,omitempty"`
	Wednesday LocatePeriod `json:"wednesday,omitzero" yaml:"wednesday,omitempty"`
	Thursday  LocatePeriod `json:"thursday,omitzero" yaml:"thursday,omitempty"`
	Friday    LocatePeriod `json:"friday,omitzero" yaml:"friday,omitempty"`
	Saturday  LocatePeriod `json:"saturday,omitzero" yaml:"saturday,omitempty"`
	Sunday    LocatePeriod `json:"sunday,omitzero" yaml:"sunday,omitempty"`
}

type GroupByKey string

const (
	GroupByNone        GroupByKey = ""
	GroupByName        GroupByKey = "name"
	GroupByCategory    GroupByKey = "category"
	GroupByEnvironment GroupByKey = "environment"
	GroupByPerimeter   GroupByKey = "perimeter"
	GroupByJob         GroupByKey = "job"
	GroupByDataset     GroupByKey = "dataset"
	GroupByDataClass   GroupByKey = "data-class"
	GroupByTag         GroupByKey = "tag"
	GroupByOrigin      GroupByKey = "origin"
	GroupByType        GroupByKey = "type"
	GroupByRoot        GroupByKey = "root"
)

type LocateOptions struct {
	Filters LocateFilters `json:"filters,omitzero" yaml:"filters,omitempty"`
	Periods LocatePeriods `json:"periods,omitzero" yaml:"periods,omitempty"`
	GroupBy GroupByKey    `json:"group_by,omitempty" yaml:"group_by,omitempty"`
}

func (lo *LocateOptions) HasPeriods() bool {
	return !lo.Periods.Minute.Empty() || !lo.Periods.Hour.Empty() || !lo.Periods.Day.Empty() ||
		!lo.Periods.Week.Empty() || !lo.Periods.Month.Empty() || !lo.Periods.Year.Empty() ||
		!lo.Periods.Monday.Empty() || !lo.Periods.Tuesday.Empty() || !lo.Periods.Wednesday.Empty() ||
		!lo.Periods.Thursday.Empty() || !lo.Periods.Friday.Empty() || !lo.Periods.Saturday.Empty() || !lo.Periods.Sunday.Empty()
}

type Option func(*LocateOptions)

func NewDefaultLocateOptions(opts ...Option) *LocateOptions {
	p := &LocateOptions{}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func WithKeepMinutes(n int) Option { return func(p *LocateOptions) { p.Periods.Minute.Keep = n } }
func WithKeepHours(n int) Option   { return func(p *LocateOptions) { p.Periods.Hour.Keep = n } }
func WithKeepDays(n int) Option    { return func(p *LocateOptions) { p.Periods.Day.Keep = n } }
func WithKeepWeeks(n int) Option   { return func(p *LocateOptions) { p.Periods.Week.Keep = n } }
func WithKeepMonths(n int) Option  { return func(p *LocateOptions) { p.Periods.Month.Keep = n } }
func WithKeepYears(n int) Option   { return func(p *LocateOptions) { p.Periods.Year.Keep = n } }

func WithKeepMondays(n int) Option    { return func(p *LocateOptions) { p.Periods.Monday.Keep = n } }
func WithKeepTuesdays(n int) Option   { return func(p *LocateOptions) { p.Periods.Tuesday.Keep = n } }
func WithKeepWednesdays(n int) Option { return func(p *LocateOptions) { p.Periods.Wednesday.Keep = n } }
func WithKeepThursdays(n int) Option  { return func(p *LocateOptions) { p.Periods.Thursday.Keep = n } }
func WithKeepFridays(n int) Option    { return func(p *LocateOptions) { p.Periods.Friday.Keep = n } }
func WithKeepSaturdays(n int) Option  { return func(p *LocateOptions) { p.Periods.Saturday.Keep = n } }
func WithKeepSundays(n int) Option    { return func(p *LocateOptions) { p.Periods.Sunday.Keep = n } }

func WithPerMinuteCap(n int) Option { return func(p *LocateOptions) { p.Periods.Minute.Cap = n } }
func WithPerHourCap(n int) Option   { return func(p *LocateOptions) { p.Periods.Hour.Cap = n } }
func WithPerDayCap(n int) Option    { return func(p *LocateOptions) { p.Periods.Day.Cap = n } }
func WithPerWeekCap(n int) Option   { return func(p *LocateOptions) { p.Periods.Week.Cap = n } }
func WithPerMonthCap(n int) Option  { return func(p *LocateOptions) { p.Periods.Month.Cap = n } }
func WithPerYearCap(n int) Option   { return func(p *LocateOptions) { p.Periods.Year.Cap = n } }

func WithPerMondayCap(n int) Option   { return func(p *LocateOptions) { p.Periods.Monday.Cap = n } }
func WithPerTuesdayCap(n int) Option  { return func(p *LocateOptions) { p.Periods.Tuesday.Cap = n } }
func WithPerWednsdayCap(n int) Option { return func(p *LocateOptions) { p.Periods.Wednesday.Cap = n } }
func WithPerThursdayCap(n int) Option { return func(p *LocateOptions) { p.Periods.Thursday.Cap = n } }
func WithPerFridayCap(n int) Option   { return func(p *LocateOptions) { p.Periods.Friday.Cap = n } }
func WithPerSaturdayCap(n int) Option { return func(p *LocateOptions) { p.Periods.Saturday.Cap = n } }
func WithPerSundaysCap(n int) Option  { return func(p *LocateOptions) { p.Periods.Sunday.Cap = n } }

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
func WithDataset(dataset string) Option {
	return func(p *LocateOptions) { p.Filters.Dataset = dataset }
}
func WithDataClass(class string) Option {
	return func(p *LocateOptions) { p.Filters.DataClasses = append(p.Filters.DataClasses, class) }
}
func WithTag(tag string) Option {
	return func(p *LocateOptions) { p.Filters.Tags = append(p.Filters.Tags, tag) }
}
func WithIgnoreTag(tag string) Option {
	return func(p *LocateOptions) { p.Filters.IgnoreTags = append(p.Filters.IgnoreTags, tag) }
}
func WithType(id string) Option {
	return func(p *LocateOptions) { p.Filters.Types = append(p.Filters.Types, id) }
}
func WithRoot(id string) Option {
	return func(p *LocateOptions) { p.Filters.Roots = append(p.Filters.Roots, id) }
}
func WithOrigin(origin string) Option {
	return func(p *LocateOptions) { p.Filters.Origins = append(p.Filters.Origins, origin) }
}
func WithID(id string) Option {
	return func(p *LocateOptions) { p.Filters.IDs = append(p.Filters.IDs, id) }
}
func WithLatest(latest bool) Option { return func(p *LocateOptions) { p.Filters.Latest = latest } }
func WithGroupBy(key GroupByKey) Option {
	return func(p *LocateOptions) { p.GroupBy = key }
}

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
	if lo.Filters.Dataset != "" && it.Filters.Dataset != lo.Filters.Dataset {
		return false
	}
	if !it.Filters.HasDataClasses(lo.Filters.DataClasses) {
		return false
	}
	for _, tag := range lo.Filters.IgnoreTags {
		if it.Filters.HasTag(tag) {
			return false
		}
	}
	for _, tag := range lo.Filters.Tags {
		if !it.Filters.HasTag(tag) {
			return false
		}
	}
	if !it.Filters.HasTypes(lo.Filters.Types) {
		return false
	}
	if !it.Filters.HasOrigins(lo.Filters.Origins) {
		return false
	}
	for _, root := range lo.Filters.Roots {
		if !it.Filters.HasRoot(root) {
			return false
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

	if lo.GroupBy == GroupByNone {
		return lo.applyPeriods(filtered, now)
	}

	groups := make(map[string][]Item)
	for _, it := range filtered {
		for _, k := range lo.groupKeys(it) {
			groups[k] = append(groups[k], it)
		}
	}

	kept := make(map[objects.MAC]struct{}, len(filtered))
	reasons := make(map[objects.MAC]Reason, len(filtered))
	for _, gitems := range groups {
		gk, gr := lo.applyPeriods(gitems, now)
		for id := range gk {
			kept[id] = struct{}{}
		}
		for id, r := range gr {
			if prev, ok := reasons[id]; !ok || (prev.Action == "delete" && r.Action == "keep") {
				reasons[id] = r
			}
		}
	}
	return kept, reasons
}

func (lo *LocateOptions) groupKeys(it Item) []string {
	switch lo.GroupBy {
	case GroupByName:
		return []string{it.Filters.Name}
	case GroupByCategory:
		return []string{it.Filters.Category}
	case GroupByEnvironment:
		return []string{it.Filters.Environment}
	case GroupByPerimeter:
		return []string{it.Filters.Perimeter}
	case GroupByJob:
		return []string{it.Filters.Job}
	case GroupByDataset:
		return []string{it.Filters.Dataset}
	case GroupByDataClass:
		if len(it.Filters.DataClasses) == 0 {
			return []string{""}
		}
		return it.Filters.DataClasses
	case GroupByTag:
		if len(it.Filters.Tags) == 0 {
			return []string{""}
		}
		return it.Filters.Tags
	case GroupByOrigin:
		if len(it.Filters.Origins) == 0 {
			return []string{""}
		}
		return it.Filters.Origins
	case GroupByType:
		if len(it.Filters.Types) == 0 {
			return []string{""}
		}
		return it.Filters.Types
	case GroupByRoot:
		if len(it.Filters.Roots) == 0 {
			return []string{""}
		}
		return it.Filters.Roots
	default:
		return []string{""}
	}
}

func (lo *LocateOptions) applyPeriods(filtered []Item, now time.Time) (map[objects.MAC]struct{}, map[objects.MAC]Reason) {
	kept := make(map[objects.MAC]struct{}, len(filtered))
	reasons := make(map[objects.MAC]Reason, len(filtered))

	if len(filtered) == 0 {
		return kept, reasons
	}

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
				if period.Key(period.Start(s.Timestamp)) == period.Key(s.Timestamp) {
					windowKeys[period.Key(s.Timestamp)] = struct{}{}
				}
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

	processRule(Minutes, lo.Periods.Minute)
	processRule(Hours, lo.Periods.Hour)
	processRule(Days, lo.Periods.Day)

	processRule(Mondays, lo.Periods.Monday)
	processRule(Tuesdays, lo.Periods.Tuesday)
	processRule(Wednesdays, lo.Periods.Wednesday)
	processRule(Thursdays, lo.Periods.Thursday)
	processRule(Fridays, lo.Periods.Friday)
	processRule(Saturdays, lo.Periods.Saturday)
	processRule(Sundays, lo.Periods.Sunday)

	processRule(Weeks, lo.Periods.Week)
	processRule(Months, lo.Periods.Month)
	processRule(Years, lo.Periods.Year)

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
	return !lo.HasPeriods() &&
		lo.Filters.Name == "" && lo.Filters.Category == "" && lo.Filters.Environment == "" &&
		lo.Filters.Perimeter == "" && lo.Filters.Job == "" && lo.Filters.Dataset == "" &&
		len(lo.Filters.Tags) == 0 && len(lo.Filters.IDs) == 0 && len(lo.Filters.Roots) == 0 &&
		len(lo.Filters.DataClasses) == 0 &&
		lo.Filters.Before.IsZero() && lo.Filters.Since.IsZero() && !lo.Filters.Latest &&
		lo.GroupBy == GroupByNone
}
