package locate

import (
	"flag"
	"strings"
)

func (po *LocateOptions) installGenericFlags(flags *flag.FlagSet) {
	flags.Var(NewTimeFlag(&po.Filters.Before), "before", "filter by date")
	flags.Var(NewTimeFlag(&po.Filters.Since), "since", "filter by date")

	flags.StringVar(&po.Filters.Name, "name", "", "filter by name")
	flags.StringVar(&po.Filters.Category, "category", "", "filter by category")
	flags.StringVar(&po.Filters.Environment, "environment", "", "filter by environment")
	flags.StringVar(&po.Filters.Perimeter, "perimeter", "", "filter by perimeter")
	flags.StringVar(&po.Filters.Job, "job", "", "filter by job")
	flags.BoolVar(&po.Filters.Latest, "latest", false, "consider only the latest matching item")

	flags.Func("root", "filter by root (repeat).",
		func(v string) error {
			for _, t := range strings.Split(v, ",") {
				t = strings.TrimSpace(t)
				if t != "" {
					po.Filters.Roots = append(po.Filters.Roots, t)
				}
			}
			return nil
		})

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
}

func (po *LocateOptions) InstallLocateFlags(flags *flag.FlagSet) {
	po.installGenericFlags(flags)
	flags.IntVar(&po.Periods.Minute.Keep, "minutes", 0, "keep snapshots for the last N minutes")
	flags.IntVar(&po.Periods.Hour.Keep, "hours", 0, "keep snapshots for the last N hours")
	flags.IntVar(&po.Periods.Day.Keep, "days", 0, "keep snapshots for the last N days")
	flags.IntVar(&po.Periods.Week.Keep, "weeks", 0, "keep snapshots for the last N weeks")
	flags.IntVar(&po.Periods.Month.Keep, "months", 0, "keep snapshots for the last N months")
	flags.IntVar(&po.Periods.Year.Keep, "years", 0, "keep snapshots for the last N years")

	flags.IntVar(&po.Periods.Minute.Cap, "per-minute", 0, "cap the number of kept snapshots per minute")
	flags.IntVar(&po.Periods.Hour.Cap, "per-hour", 0, "cap the number of kept snapshots per hour")
	flags.IntVar(&po.Periods.Day.Cap, "per-day", 0, "cap the number of kept snapshots per day")
	flags.IntVar(&po.Periods.Week.Cap, "per-week", 0, "cap the number of kept snapshots per week")
	flags.IntVar(&po.Periods.Month.Cap, "per-month", 0, "cap the number of kept snapshots per month")
	flags.IntVar(&po.Periods.Year.Cap, "per-year", 0, "cap the number of kept snapshots per year")

	flags.IntVar(&po.Periods.Monday.Keep, "mondays", 0, "keep snapshots for the last N Mondays")
	flags.IntVar(&po.Periods.Tuesday.Keep, "tuesdays", 0, "keep snapshots for the last N Tuesdays")
	flags.IntVar(&po.Periods.Wednesday.Keep, "wednesdays", 0, "keep snapshots for the last N Wednesdays")
	flags.IntVar(&po.Periods.Thursday.Keep, "thursdays", 0, "keep snapshots for the last N Thursdays")
	flags.IntVar(&po.Periods.Friday.Keep, "fridays", 0, "keep snapshots for the last N Fridays")
	flags.IntVar(&po.Periods.Saturday.Keep, "saturdays", 0, "keep snapshots for the last N Saturdays")
	flags.IntVar(&po.Periods.Sunday.Keep, "sundays", 0, "keep snapshots for the last N Sundays")

	flags.IntVar(&po.Periods.Monday.Cap, "per-monday", 0, "cap the number of kept snapshots per Monday bucket")
	flags.IntVar(&po.Periods.Tuesday.Cap, "per-tuesday", 0, "cap the number of kept snapshots per Tuesday bucket")
	flags.IntVar(&po.Periods.Wednesday.Cap, "per-wednesday", 0, "cap the number of kept snapshots per Wednesday bucket")
	flags.IntVar(&po.Periods.Thursday.Cap, "per-thursday", 0, "cap the number of kept snapshots per Thursday bucket")
	flags.IntVar(&po.Periods.Friday.Cap, "per-friday", 0, "cap the number of kept snapshots per Friday bucket")
	flags.IntVar(&po.Periods.Saturday.Cap, "per-saturday", 0, "cap the number of kept snapshots per Saturday bucket")
	flags.IntVar(&po.Periods.Sunday.Cap, "per-sunday", 0, "cap the number of kept snapshots per Sunday bucket")
}

func (po *LocateOptions) InstallDeletionFlags(flags *flag.FlagSet) {
	po.installGenericFlags(flags)
}
