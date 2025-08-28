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

	flags.Func("source", "filter by root (repeat).",
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
	flags.IntVar(&po.Minute.Keep, "minutes", 0, "keep snapshots for the last N minutes")
	flags.IntVar(&po.Hour.Keep, "hours", 0, "keep snapshots for the last N hours")
	flags.IntVar(&po.Day.Keep, "days", 0, "keep snapshots for the last N days")
	flags.IntVar(&po.Week.Keep, "weeks", 0, "keep snapshots for the last N weeks")
	flags.IntVar(&po.Month.Keep, "months", 0, "keep snapshots for the last N months")
	flags.IntVar(&po.Year.Keep, "years", 0, "keep snapshots for the last N years")
	flags.IntVar(&po.Minute.Cap, "per-minute", 0, "cap the number of kept snapshots per minute")
	flags.IntVar(&po.Hour.Cap, "per-hour", 0, "cap the number of kept snapshots per hour")
	flags.IntVar(&po.Day.Cap, "per-day", 0, "cap the number of kept snapshots per day")
	flags.IntVar(&po.Week.Cap, "per-week", 0, "cap the number of kept snapshots per week")
	flags.IntVar(&po.Month.Cap, "per-month", 0, "cap the number of kept snapshots per month")
	flags.IntVar(&po.Year.Cap, "per-year", 0, "cap the number of kept snapshots per year")
}

func (po *LocateOptions) InstallDeletionFlags(flags *flag.FlagSet) {
	po.installGenericFlags(flags)
}
