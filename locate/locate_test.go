package locate_test

import (
	"fmt"
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func TestLocatePeriodEmpty(t *testing.T) {
	t.Run("EmptyArgumentsReturnsTrue", func(t *testing.T) {
		lp := loc.LocatePeriod{}
		require.True(t, lp.Empty())
	})

	t.Run("ReturnsFalseIfAnyArgumentIsSet", func(t *testing.T) {
		testCases := []loc.LocatePeriod{
			{Keep: 1, Cap: 0},
			{Keep: 0, Cap: 1},
			{Keep: 1, Cap: 1},
			{Keep: 2, Cap: 3},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(fmt.Sprintf("Keep%dCap%d", tc.Keep, tc.Cap), func(t *testing.T) {
				require.False(t, tc.Empty())
			})
		}
	})
}

// NewDefaultLocateOptions is tested first with anonymous Option functions
// instead of the public With... helpers.
// This keeps the testing consistency:
// - NewDefaultLocateOptions is validated on its own contract first
// - the public With... helpers are tested afterwards as a separate API block
func TestNewDefaultLocateOptions(t *testing.T) {
	t.Run("WithoutOptionsReturnsNonNilEmptyLocateOptions", func(t *testing.T) {
		got := loc.NewDefaultLocateOptions()
		require.NotNil(t, got)
		require.Equal(t, &loc.LocateOptions{}, got)
	})

	t.Run("AppliesSingleOption", func(t *testing.T) {
		got := loc.NewDefaultLocateOptions(func(lo *loc.LocateOptions) {
			lo.Filters.Name = "daily-backup"
		})
		require.Equal(t, "daily-backup", got.Filters.Name)
	})

	t.Run("AppliesMultipleOptions", func(t *testing.T) {
		before := time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC)

		got := loc.NewDefaultLocateOptions(
			func(lo *loc.LocateOptions) {
				lo.Filters.Name = "daily-backup"
			},
			func(lo *loc.LocateOptions) {
				lo.Filters.Before = before
			},
			func(lo *loc.LocateOptions) {
				lo.Periods.Day.Keep = 7
			},
			func(lo *loc.LocateOptions) {
				lo.Periods.Day.Cap = 2
			},
		)
		require.Equal(t, "daily-backup", got.Filters.Name)
		require.Equal(t, before, got.Filters.Before)
		require.Equal(t, 7, got.Periods.Day.Keep)
		require.Equal(t, 2, got.Periods.Day.Cap)
	})

	t.Run("AppliesOptionsInOrder", func(t *testing.T) {
		got := loc.NewDefaultLocateOptions(
			func(lo *loc.LocateOptions) {
				lo.Filters.Name = "first"
			},
			func(lo *loc.LocateOptions) {
				lo.Filters.Name = "second"
			},
		)
		require.Equal(t, "second", got.Filters.Name)
	})
}

func TestWithKeepOptions(t *testing.T) {
	t.Run("SetsStandardPeriodKeepFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithKeepMinutes(1)(lo)
		loc.WithKeepHours(2)(lo)
		loc.WithKeepDays(3)(lo)
		loc.WithKeepWeeks(4)(lo)
		loc.WithKeepMonths(5)(lo)
		loc.WithKeepYears(6)(lo)

		require.Equal(t, 1, lo.Periods.Minute.Keep)
		require.Equal(t, 2, lo.Periods.Hour.Keep)
		require.Equal(t, 3, lo.Periods.Day.Keep)
		require.Equal(t, 4, lo.Periods.Week.Keep)
		require.Equal(t, 5, lo.Periods.Month.Keep)
		require.Equal(t, 6, lo.Periods.Year.Keep)
	})

	t.Run("SetsWeekdayPeriodKeepFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithKeepMondays(1)(lo)
		loc.WithKeepTuesdays(2)(lo)
		loc.WithKeepWednesdays(3)(lo)
		loc.WithKeepThursdays(4)(lo)
		loc.WithKeepFridays(5)(lo)
		loc.WithKeepSaturdays(6)(lo)
		loc.WithKeepSundays(7)(lo)

		require.Equal(t, 1, lo.Periods.Monday.Keep)
		require.Equal(t, 2, lo.Periods.Tuesday.Keep)
		require.Equal(t, 3, lo.Periods.Wednesday.Keep)
		require.Equal(t, 4, lo.Periods.Thursday.Keep)
		require.Equal(t, 5, lo.Periods.Friday.Keep)
		require.Equal(t, 6, lo.Periods.Saturday.Keep)
		require.Equal(t, 7, lo.Periods.Sunday.Keep)
	})
}

func TestWithCapOptions(t *testing.T) {
	t.Run("SetsStandardPeriodCapFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithPerMinuteCap(1)(lo)
		loc.WithPerHourCap(2)(lo)
		loc.WithPerDayCap(3)(lo)
		loc.WithPerWeekCap(4)(lo)
		loc.WithPerMonthCap(5)(lo)
		loc.WithPerYearCap(6)(lo)

		require.Equal(t, 1, lo.Periods.Minute.Cap)
		require.Equal(t, 2, lo.Periods.Hour.Cap)
		require.Equal(t, 3, lo.Periods.Day.Cap)
		require.Equal(t, 4, lo.Periods.Week.Cap)
		require.Equal(t, 5, lo.Periods.Month.Cap)
		require.Equal(t, 6, lo.Periods.Year.Cap)
	})

	t.Run("SetsWeekdayPeriodCapFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithPerMondayCap(1)(lo)
		loc.WithPerTuesdayCap(2)(lo)
		loc.WithPerWednsdayCap(3)(lo)
		loc.WithPerThursdayCap(4)(lo)
		loc.WithPerFridayCap(5)(lo)
		loc.WithPerSaturdayCap(6)(lo)
		loc.WithPerSundaysCap(7)(lo)

		require.Equal(t, 1, lo.Periods.Monday.Cap)
		require.Equal(t, 2, lo.Periods.Tuesday.Cap)
		require.Equal(t, 3, lo.Periods.Wednesday.Cap)
		require.Equal(t, 4, lo.Periods.Thursday.Cap)
		require.Equal(t, 5, lo.Periods.Friday.Cap)
		require.Equal(t, 6, lo.Periods.Saturday.Cap)
		require.Equal(t, 7, lo.Periods.Sunday.Cap)
	})
}

func TestWithFilterOptions(t *testing.T) {
	t.Run("SetsScalarFilterFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		before := time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC)
		since := time.Date(2025, time.August, 20, 8, 0, 0, 0, time.UTC)

		loc.WithBefore(before)(lo)
		loc.WithSince(since)(lo)
		loc.WithName("daily-backup")(lo)
		loc.WithCategory("database")(lo)
		loc.WithEnvironment("prod")(lo)
		loc.WithPerimeter("eu-west")(lo)
		loc.WithJob("nightly")(lo)
		loc.WithLatest(true)(lo)

		require.Equal(t, before, lo.Filters.Before)
		require.Equal(t, since, lo.Filters.Since)
		require.Equal(t, "daily-backup", lo.Filters.Name)
		require.Equal(t, "database", lo.Filters.Category)
		require.Equal(t, "prod", lo.Filters.Environment)
		require.Equal(t, "eu-west", lo.Filters.Perimeter)
		require.Equal(t, "nightly", lo.Filters.Job)
		require.True(t, lo.Filters.Latest)
	})

	t.Run("AppendsSliceBasedFilterFields", func(t *testing.T) {
		lo := &loc.LocateOptions{}

		loc.WithTag("important")(lo)
		loc.WithTag("daily")(lo)
		loc.WithOrigin("s3://bucket-a")(lo)
		loc.WithOrigin("s3://bucket-b")(lo)
		loc.WithID("abc123")(lo)
		loc.WithID("def456")(lo)

		require.Equal(t, []string{"important", "daily"}, lo.Filters.Tags)
		require.Equal(t, []string{"s3://bucket-a", "s3://bucket-b"}, lo.Filters.Origins)
		require.Equal(t, []string{"abc123", "def456"}, lo.Filters.IDs)
	})
}

func TestLocateOptionsHasPeriods(t *testing.T) {
	t.Run("False_WhenAllPeriodsAreEmpty", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		require.False(t, lo.HasPeriods())
	})

	t.Run("ReturnsTrueWhenAnyKeepPeriodIsConfigured", func(t *testing.T) {
		testCases := []struct {
			name  string
			apply loc.Option
		}{
			{name: "Minute", apply: loc.WithKeepMinutes(1)},
			{name: "Hour", apply: loc.WithKeepHours(1)},
			{name: "Day", apply: loc.WithKeepDays(1)},
			{name: "Week", apply: loc.WithKeepWeeks(1)},
			{name: "Month", apply: loc.WithKeepMonths(1)},
			{name: "Year", apply: loc.WithKeepYears(1)},
			{name: "Monday", apply: loc.WithKeepMondays(1)},
			{name: "Tuesday", apply: loc.WithKeepTuesdays(1)},
			{name: "Wednesday", apply: loc.WithKeepWednesdays(1)},
			{name: "Thursday", apply: loc.WithKeepThursdays(1)},
			{name: "Friday", apply: loc.WithKeepFridays(1)},
			{name: "Saturday", apply: loc.WithKeepSaturdays(1)},
			{name: "Sunday", apply: loc.WithKeepSundays(1)},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				lo := &loc.LocateOptions{}
				tc.apply(lo)
				require.True(t, lo.HasPeriods())
			})
		}
	})

	t.Run("ReturnsTrueWhenAnyCapPeriodIsConfigured", func(t *testing.T) {
		testCases := []struct {
			name  string
			apply loc.Option
		}{
			{name: "Minute", apply: loc.WithPerMinuteCap(1)},
			{name: "Hour", apply: loc.WithPerHourCap(1)},
			{name: "Day", apply: loc.WithPerDayCap(1)},
			{name: "Week", apply: loc.WithPerWeekCap(1)},
			{name: "Month", apply: loc.WithPerMonthCap(1)},
			{name: "Year", apply: loc.WithPerYearCap(1)},
			{name: "Monday", apply: loc.WithPerMondayCap(1)},
			{name: "Tuesday", apply: loc.WithPerTuesdayCap(1)},
			{name: "Wednesday", apply: loc.WithPerWednsdayCap(1)},
			{name: "Thursday", apply: loc.WithPerThursdayCap(1)},
			{name: "Friday", apply: loc.WithPerFridayCap(1)},
			{name: "Saturday", apply: loc.WithPerSaturdayCap(1)},
			{name: "Sunday", apply: loc.WithPerSundaysCap(1)},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				lo := &loc.LocateOptions{}
				tc.apply(lo)
				require.True(t, lo.HasPeriods())
			})
		}
	})
}

func TestLocateOptionsEmpty(t *testing.T) {
	t.Run("True_WhenNoPeriodsOrSupportedFiltersAreConfigured", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		require.True(t, lo.Empty())
	})

	t.Run("False_WhenAnyPeriodIsConfigured", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		loc.WithKeepDays(1)(lo)
		require.False(t, lo.Empty())
	})

	t.Run("False_WhenScalarFilterIsConfigured", func(t *testing.T) {
		testCases := []struct {
			name  string
			apply loc.Option
		}{
			{
				name:  "Name",
				apply: loc.WithName("daily-backup"),
			},
			{
				name:  "Category",
				apply: loc.WithCategory("database"),
			},
			{
				name:  "Environment",
				apply: loc.WithEnvironment("prod"),
			},
			{
				name:  "Perimeter",
				apply: loc.WithPerimeter("eu-west"),
			},
			{
				name:  "Job",
				apply: loc.WithJob("nightly"),
			},
			{
				name:  "Before",
				apply: loc.WithBefore(time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC)),
			},
			{
				name:  "Since",
				apply: loc.WithSince(time.Date(2025, time.August, 20, 8, 0, 0, 0, time.UTC)),
			},
			{
				name:  "Latest",
				apply: loc.WithLatest(true),
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				lo := &loc.LocateOptions{}
				tc.apply(lo)
				require.False(t, lo.Empty())
			})
		}
	})

	t.Run("False_WhenSliceFilterIsConfigured", func(t *testing.T) {
		testCases := []struct {
			name  string
			apply loc.Option
		}{
			{
				name:  "Tags",
				apply: loc.WithTag("important"),
			},
			{
				name:  "IDs",
				apply: loc.WithID("abc123"),
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				lo := &loc.LocateOptions{}
				tc.apply(lo)
				require.False(t, lo.Empty())
			})
		}
	})

	t.Run("False_WhenRootsFilterIsConfiguredDirectly", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Roots: []string{"/var/backups"},
			},
		}
		require.False(t, lo.Empty())
	})

	t.Run("ReturnsTrueWhenOnlyOriginsAreConfigured", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Origins: []string{"s3://bucket-a"},
			},
		}
		require.True(t, lo.Empty())
	})

	t.Run("ReturnsTrueWhenOnlyTypesAreConfigured", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Types: []string{"s3"},
			},
		}
		require.True(t, lo.Empty())
	})

	t.Run("ReturnsTrueWhenOnlyIgnoreTagsAreConfigured", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				IgnoreTags: []string{"temporary"},
			},
		}
		require.True(t, lo.Empty())
	})

	t.Run("ReturnsTrueWhenOnlyIgnoredFiltersAreCombined", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Origins:    []string{"s3://bucket-a"},
				Types:      []string{"s3"},
				IgnoreTags: []string{"temporary"},
			},
		}
		require.True(t, lo.Empty())
	})

	t.Run("ReturnsFalseWhenIgnoredAndSupportedFiltersAreCombined", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Origins:    []string{"s3://bucket-a"},
				Types:      []string{"s3"},
				IgnoreTags: []string{"temporary"},
				Name:       "daily-backup",
			},
		}
		require.False(t, lo.Empty())
	})
}

func TestItemFiltersHasTag(t *testing.T) {
	t.Run("EmptyTagReturnsTrue", func(t *testing.T) {
		filters := loc.ItemFilters{
			Tags: []string{"daily", "important"},
		}
		require.True(t, filters.HasTag(""))
	})

	t.Run("ReturnsTrueWhenTagExists", func(t *testing.T) {
		filters := loc.ItemFilters{
			Tags: []string{"daily", "important"},
		}
		require.True(t, filters.HasTag("important"))
	})

	t.Run("ReturnsFalseWhenTagDoesNotExist", func(t *testing.T) {
		filters := loc.ItemFilters{
			Tags: []string{"daily", "important"},
		}
		require.False(t, filters.HasTag("weekly"))
	})
}

func TestItemFiltersHasType(t *testing.T) {
	t.Run("EmptyTypeReturnsTrue", func(t *testing.T) {
		filters := loc.ItemFilters{
			Types: []string{"fs", "s3"},
		}

		require.True(t, filters.HasType(""))
	})

	t.Run("ReturnsTrueWhenTypeExists", func(t *testing.T) {
		filters := loc.ItemFilters{
			Types: []string{"fs", "s3"},
		}
		require.True(t, filters.HasType("s3"))
	})

	t.Run("ReturnsFalseWhenTypeDoesNotExist", func(t *testing.T) {
		filters := loc.ItemFilters{
			Types: []string{"fs", "s3"},
		}
		require.False(t, filters.HasType("swift"))
	})
}

func TestItemFiltersHasOrigin(t *testing.T) {
	t.Run("EmptyOriginReturnsTrue", func(t *testing.T) {
		filters := loc.ItemFilters{
			Origins: []string{"s3://bucket-a", "s3://bucket-b"},
		}
		require.True(t, filters.HasOrigin(""))
	})

	t.Run("ReturnsTrueWhenOriginExists", func(t *testing.T) {
		filters := loc.ItemFilters{
			Origins: []string{"s3://bucket-a", "s3://bucket-b"},
		}

		require.True(t, filters.HasOrigin("s3://bucket-b"))
	})

	t.Run("ReturnsFalseWhenOriginDoesNotExist", func(t *testing.T) {
		filters := loc.ItemFilters{
			Origins: []string{"s3://bucket-a", "s3://bucket-b"},
		}
		require.False(t, filters.HasOrigin("s3://bucket-c"))
	})
}

func TestItemFiltersHasOrigins(t *testing.T) {
	t.Run("EmptyOriginsReturnsTrue", func(t *testing.T) {
		filters := loc.ItemFilters{
			Origins: []string{"s3://bucket-a", "s3://bucket-b"},
		}
		require.True(t, filters.HasOrigins(nil))
		require.True(t, filters.HasOrigins([]string{}))
	})

	t.Run("ReturnsTrueWhenAnyOriginMatches", func(t *testing.T) {
		filters := loc.ItemFilters{
			Origins: []string{"s3://bucket-a", "s3://bucket-b"},
		}

		require.True(t, filters.HasOrigins([]string{"s3://bucket-x", "s3://bucket-b"}))
	})

	t.Run("ReturnsFalseWhenNoOriginMatches", func(t *testing.T) {
		filters := loc.ItemFilters{
			Origins: []string{"s3://bucket-a", "s3://bucket-b"},
		}
		require.False(t, filters.HasOrigins([]string{"s3://bucket-x", "s3://bucket-y"}))
	})
}

func TestItemFiltersHasTypes(t *testing.T) {
	t.Run("EmptyTypesReturnsTrue", func(t *testing.T) {
		filters := loc.ItemFilters{
			Types: []string{"fs", "s3"},
		}
		require.True(t, filters.HasTypes(nil))
		require.True(t, filters.HasTypes([]string{}))
	})

	t.Run("ReturnsTrueWhenAnyTypeMatches", func(t *testing.T) {
		filters := loc.ItemFilters{
			Types: []string{"fs", "s3"},
		}
		require.True(t, filters.HasTypes([]string{"swift", "s3"}))
	})

	t.Run("ReturnsFalseWhenNoTypeMatches", func(t *testing.T) {
		filters := loc.ItemFilters{
			Types: []string{"fs", "s3"},
		}
		require.False(t, filters.HasTypes([]string{"swift", "b2"}))
	})
}

func TestItemFiltersHasRoot(t *testing.T) {
	t.Run("EmptyRootReturnsTrue", func(t *testing.T) {
		filters := loc.ItemFilters{
			Roots: []string{"/var/backups", "/srv/data"},
		}
		require.True(t, filters.HasRoot(""))
	})

	t.Run("ReturnsTrueWhenRootExists", func(t *testing.T) {
		filters := loc.ItemFilters{
			Roots: []string{"/var/backups", "/srv/data"},
		}
		require.True(t, filters.HasRoot("/srv/data"))
	})

	t.Run("ReturnsFalseWhenRootDoesNotExist", func(t *testing.T) {
		filters := loc.ItemFilters{
			Roots: []string{"/var/backups", "/srv/data"},
		}
		require.False(t, filters.HasRoot("/tmp"))
	})
}

func TestLocateOptionsMatches(t *testing.T) {
	baseTime := time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC)

	var zeroID objects.MAC
	baseItem := loc.Item{
		ItemID:    zeroID,
		Timestamp: baseTime,
		Filters: loc.ItemFilters{
			Name:        "daily-backup",
			Category:    "database",
			Environment: "prod",
			Perimeter:   "eu-west",
			Job:         "nightly",
			Tags:        []string{"daily", "important"},
			Types:       []string{"fs", "s3"},
			Origins:     []string{"s3://bucket-a", "s3://bucket-b"},
			Roots:       []string{"/var/backups", "/srv/data"},
		},
	}

	t.Run("ReturnsTrueWhenNoFilterIsConfigured", func(t *testing.T) {
		lo := &loc.LocateOptions{}
		require.True(t, lo.Matches(baseItem))
	})

	t.Run("MatchesIDsByPrefix", func(t *testing.T) {
		t.Run("ReturnsTrueWhenAnyPrefixMatches", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					IDs: []string{"f", "0"},
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenNoPrefixMatches", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					IDs: []string{"a", "b"},
				},
			}
			require.False(t, lo.Matches(baseItem))
		})
	})

	t.Run("AppliesBeforeInclusively", func(t *testing.T) {
		t.Run("ReturnsTrueWhenTimestampIsBeforeBefore", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Before: baseTime,
				},
			}
			item := baseItem
			item.Timestamp = baseTime.Add(-time.Second)
			require.True(t, lo.Matches(item))
		})

		t.Run("ReturnsTrueWhenTimestampEqualsBefore", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Before: baseTime,
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenTimestampIsAfterBefore", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Before: baseTime.Add(-time.Second),
				},
			}
			require.False(t, lo.Matches(baseItem))
		})
	})

	t.Run("AppliesSinceInclusively", func(t *testing.T) {
		t.Run("ReturnsTrueWhenTimestampIsAfterSince", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Since: baseTime,
				},
			}

			item := baseItem
			item.Timestamp = baseTime.Add(time.Second)
			require.True(t, lo.Matches(item))
		})

		t.Run("ReturnsTrueWhenTimestampEqualsSince", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Since: baseTime,
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenTimestampIsBeforeSince", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Since: baseTime.Add(time.Second),
				},
			}
			require.False(t, lo.Matches(baseItem))
		})
	})

	t.Run("MatchesScalarHeaderFieldsByExactEquality", func(t *testing.T) {
		testCases := []struct {
			name               string
			withMatchingFilter func() *loc.LocateOptions
			withOtherFilter    func() *loc.LocateOptions
		}{
			{
				name: "Name",
				withMatchingFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Name: "daily-backup"},
					}
				},
				withOtherFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Name: "weekly-backup"},
					}
				},
			},
			{
				name: "Category",
				withMatchingFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Category: "database"},
					}
				},
				withOtherFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Category: "filesystem"},
					}
				},
			},
			{
				name: "Environment",
				withMatchingFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Environment: "prod"},
					}
				},
				withOtherFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Environment: "staging"},
					}
				},
			},
			{
				name: "Perimeter",
				withMatchingFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Perimeter: "eu-west"},
					}
				},
				withOtherFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Perimeter: "us-east"},
					}
				},
			},
			{
				name: "Job",
				withMatchingFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Job: "nightly"},
					}
				},
				withOtherFilter: func() *loc.LocateOptions {
					return &loc.LocateOptions{
						Filters: loc.LocateFilters{Job: "hourly"},
					}
				},
			},
		}

		for _, tc := range testCases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				require.True(t, tc.withMatchingFilter().Matches(baseItem))
				require.False(t, tc.withOtherFilter().Matches(baseItem))
			})
		}
	})

	t.Run("RejectsItemWhenAnyIgnoredTagIsPresent", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				IgnoreTags: []string{"important"},
			},
		}
		require.False(t, lo.Matches(baseItem))
	})

	t.Run("RequiresAllRequestedTags", func(t *testing.T) {
		t.Run("ReturnsTrueWhenAllTagsArePresent", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Tags: []string{"daily", "important"},
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenOneTagIsMissing", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Tags: []string{"daily", "missing"},
				},
			}
			require.False(t, lo.Matches(baseItem))
		})
	})

	t.Run("MatchesTypesWithAnyOfSemantics", func(t *testing.T) {
		t.Run("ReturnsTrueWhenAnyRequestedTypeMatches", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Types: []string{"swift", "s3"},
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenNoRequestedTypeMatches", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Types: []string{"swift", "b2"},
				},
			}
			require.False(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsTrueWhenRequestedTypesContainsEmptyString", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Types: []string{""},
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsTrueWhenRequestedTypesContainsEmptyStringAmongOtherValues", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Types: []string{"swift", ""},
				},
			}

			require.True(t, lo.Matches(baseItem))
		})
	})

	t.Run("MatchesOriginsWithAnyOfSemantics", func(t *testing.T) {
		t.Run("ReturnsTrueWhenAnyRequestedOriginMatches", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Origins: []string{"s3://bucket-x", "s3://bucket-b"},
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenNoRequestedOriginMatches", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Origins: []string{"s3://bucket-x", "s3://bucket-y"},
				},
			}
			require.False(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsTrueWhenRequestedOriginsContainsEmptyStringAmongOtherValues", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Origins: []string{"s3://missing-bucket", ""},
				},
			}

			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsTrueWhenRequestedOriginsContainsEmptyString", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Origins: []string{""},
				},
			}

			require.True(t, lo.Matches(baseItem))
		})
	})

	t.Run("RequiresAllRequestedRoots", func(t *testing.T) {
		t.Run("ReturnsTrueWhenAllRequestedRootsArePresent", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Roots: []string{"/var/backups", "/srv/data"},
				},
			}
			require.True(t, lo.Matches(baseItem))
		})

		t.Run("ReturnsFalseWhenOneRequestedRootIsMissing", func(t *testing.T) {
			lo := &loc.LocateOptions{
				Filters: loc.LocateFilters{
					Roots: []string{"/var/backups", "/tmp"},
				},
			}
			require.False(t, lo.Matches(baseItem))
		})
	})
}

func TestLocateOptionsFilterAndSort(t *testing.T) {
	paris := time.FixedZone("CEST", 2*60*60)

	items := []loc.Item{
		{
			Timestamp: time.Date(2025, time.August, 28, 14, 34, 56, 0, paris), // 12:34:56 UTC
			Filters: loc.ItemFilters{
				Name: "daily-backup",
			},
		},
		{
			Timestamp: time.Date(2025, time.August, 28, 12, 50, 0, 0, time.UTC),
			Filters: loc.ItemFilters{
				Name: "weekly-backup",
			},
		},
		{
			Timestamp: time.Date(2025, time.August, 28, 13, 0, 0, 0, time.UTC),
			Filters: loc.ItemFilters{
				Name: "daily-backup",
			},
		},
	}

	t.Run("FiltersMatchingItemsAndSortsThemByDescendingTimestamp", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Name: "daily-backup",
			},
		}

		got := lo.FilterAndSort(items)
		require.Len(t, got, 2)
		require.Equal(t, "daily-backup", got[0].Filters.Name)
		require.Equal(t, "daily-backup", got[1].Filters.Name)
		require.True(t, got[0].Timestamp.After(got[1].Timestamp))
		require.Equal(t, time.Date(2025, time.August, 28, 13, 0, 0, 0, time.UTC), got[0].Timestamp)
		require.Equal(t, time.Date(2025, time.August, 28, 12, 34, 56, 0, time.UTC), got[1].Timestamp)
		require.Equal(t, time.UTC, got[0].Timestamp.Location())
		require.Equal(t, time.UTC, got[1].Timestamp.Location())
	})

	t.Run("ReturnsOnlyLatestMatchingItemWhenLatestIsEnabled", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Name:   "daily-backup",
				Latest: true,
			},
		}

		got := lo.FilterAndSort(items)
		require.Len(t, got, 1)
		require.Equal(t, "daily-backup", got[0].Filters.Name)
		require.Equal(t, time.Date(2025, time.August, 28, 13, 0, 0, 0, time.UTC), got[0].Timestamp)
	})

	t.Run("ReturnsEmptySliceWhenNothingMatches", func(t *testing.T) {
		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Name: "missing-backup",
			},
		}

		got := lo.FilterAndSort(items)
		require.Empty(t, got)
	})
}

func TestLocateOptionsMatch(t *testing.T) {
	now := time.Date(2025, time.August, 28, 12, 0, 0, 0, time.UTC)

	t.Run("ReturnsEmptyMapsWhenNothingMatches", func(t *testing.T) {
		id := objects.MAC{1}
		items := []loc.Item{
			{
				ItemID:    id,
				Timestamp: now,
				Filters: loc.ItemFilters{
					Name: "daily-backup",
				},
			},
		}

		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Name: "missing-backup",
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Empty(t, kept)
		require.Empty(t, reasons)
	})

	t.Run("KeepsAllFilteredItemsWhenNoPeriodsAreConfigured", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}
		id3 := objects.MAC{3}

		items := []loc.Item{
			{
				ItemID:    id1,
				Timestamp: time.Date(2025, time.August, 28, 11, 0, 0, 0, time.UTC),
				Filters: loc.ItemFilters{
					Name: "daily-backup",
				},
			},
			{
				ItemID:    id2,
				Timestamp: time.Date(2025, time.August, 27, 11, 0, 0, 0, time.UTC),
				Filters: loc.ItemFilters{
					Name: "daily-backup",
				},
			},
			{
				ItemID:    id3,
				Timestamp: time.Date(2025, time.August, 26, 11, 0, 0, 0, time.UTC),
				Filters: loc.ItemFilters{
					Name: "weekly-backup",
				},
			},
		}

		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Name: "daily-backup",
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Len(t, kept, 2)
		require.Contains(t, kept, id1)
		require.Contains(t, kept, id2)
		require.NotContains(t, kept, id3)
		require.Len(t, reasons, 2)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Note:   "matched filters",
		}, reasons[id1])
		require.Equal(t, loc.Reason{
			Action: "keep",
			Note:   "matched filters",
		}, reasons[id2])
	})

	t.Run("KeepsOnlyItemsWithinConfiguredWindows", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}
		id3 := objects.MAC{3}

		ts1 := time.Date(2025, time.August, 28, 11, 0, 0, 0, time.UTC)
		ts2 := time.Date(2025, time.August, 27, 11, 0, 0, 0, time.UTC)
		ts3 := time.Date(2025, time.August, 26, 11, 0, 0, 0, time.UTC)

		items := []loc.Item{
			{ItemID: id1, Timestamp: ts1},
			{ItemID: id2, Timestamp: ts2},
			{ItemID: id3, Timestamp: ts3},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Day: loc.LocatePeriod{
					Keep: 2,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Len(t, kept, 2)
		require.Contains(t, kept, id1)
		require.Contains(t, kept, id2)
		require.NotContains(t, kept, id3)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "day",
			Bucket: loc.Days.Key(ts1),
			Rank:   1,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[id1])
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "day",
			Bucket: loc.Days.Key(ts2),
			Rank:   1,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[id2])
		require.Equal(t, loc.Reason{
			Action: "delete",
			Note:   "outside retention windows",
		}, reasons[id3])
	})

	t.Run("AppliesPerBucketCapWhenKeepIsZero", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}
		id3 := objects.MAC{3}

		ts1 := time.Date(2025, time.August, 28, 12, 0, 0, 0, time.UTC)
		ts2 := time.Date(2025, time.August, 28, 11, 0, 0, 0, time.UTC)
		ts3 := time.Date(2025, time.August, 27, 10, 0, 0, 0, time.UTC)

		items := []loc.Item{
			{ItemID: id1, Timestamp: ts1},
			{ItemID: id2, Timestamp: ts2},
			{ItemID: id3, Timestamp: ts3},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Day: loc.LocatePeriod{
					Cap: 1,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Len(t, kept, 2)
		require.Contains(t, kept, id1)
		require.Contains(t, kept, id3)
		require.NotContains(t, kept, id2)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "day",
			Bucket: loc.Days.Key(ts1),
			Rank:   1,
			Cap:    1,
			Note:   "within bucket",
		}, reasons[id1])
		require.Equal(t, loc.Reason{
			Action: "delete",
			Rule:   "day",
			Bucket: loc.Days.Key(ts2),
			Rank:   2,
			Cap:    1,
			Note:   "exceeds per-bucket cap",
		}, reasons[id2])
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "day",
			Bucket: loc.Days.Key(ts3),
			Rank:   1,
			Cap:    1,
			Note:   "within bucket",
		}, reasons[id3])
	})

	t.Run("PrefersKeepOverDeleteAcrossRules", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}
		id3 := objects.MAC{3}

		ts1 := time.Date(2025, time.August, 28, 12, 0, 0, 0, time.UTC)
		ts2 := time.Date(2025, time.August, 28, 11, 0, 0, 0, time.UTC)
		ts3 := time.Date(2025, time.August, 27, 10, 0, 0, 0, time.UTC)

		items := []loc.Item{
			{ItemID: id1, Timestamp: ts1},
			{ItemID: id2, Timestamp: ts2},
			{ItemID: id3, Timestamp: ts3},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Day: loc.LocatePeriod{
					Keep: 2,
					Cap:  1,
				},
				Week: loc.LocatePeriod{
					Keep: 1,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Contains(t, kept, id2)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "week",
			Bucket: loc.Weeks.Key(ts2),
			Rank:   2,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[id2])
	})

	t.Run("ChoosesKeepReasonWithLowestRankAcrossRules", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}

		ts1 := time.Date(2025, time.August, 28, 12, 0, 0, 0, time.UTC)
		ts2 := time.Date(2025, time.August, 27, 10, 0, 0, 0, time.UTC)

		items := []loc.Item{
			{ItemID: id1, Timestamp: ts1},
			{ItemID: id2, Timestamp: ts2},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Day: loc.LocatePeriod{
					Keep: 2,
				},
				Week: loc.LocatePeriod{
					Keep: 1,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Contains(t, kept, id2)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "day",
			Bucket: loc.Days.Key(ts2),
			Rank:   1,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[id2])
	})

	t.Run("ChoosesDeleteReasonWithLowestRankAcrossRules", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}
		id3 := objects.MAC{3}
		id4 := objects.MAC{4}

		ts1 := time.Date(2025, time.August, 29, 9, 0, 0, 0, time.UTC)  // week rank 1
		ts2 := time.Date(2025, time.August, 28, 12, 0, 0, 0, time.UTC) // day rank 1, week rank 2
		ts3 := time.Date(2025, time.August, 28, 11, 0, 0, 0, time.UTC) // day rank 2, week rank 3
		ts4 := time.Date(2025, time.August, 27, 10, 0, 0, 0, time.UTC) // week rank 4

		items := []loc.Item{
			{ItemID: id1, Timestamp: ts1},
			{ItemID: id2, Timestamp: ts2},
			{ItemID: id3, Timestamp: ts3},
			{ItemID: id4, Timestamp: ts4},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Day: loc.LocatePeriod{
					Cap: 1,
				},
				Week: loc.LocatePeriod{
					Cap: 2,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.NotContains(t, kept, id3)
		require.Equal(t, loc.Reason{
			Action: "delete",
			Rule:   "day",
			Bucket: loc.Days.Key(ts3),
			Rank:   2,
			Cap:    1,
			Note:   "exceeds per-bucket cap",
		}, reasons[id3])
	})
	t.Run("KeepsFirstRuleWhenKeepRanksAreEqual", func(t *testing.T) {
		id := objects.MAC{9}
		ts := time.Date(2025, time.August, 28, 12, 0, 0, 0, time.UTC)

		items := []loc.Item{
			{ItemID: id, Timestamp: ts},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Day: loc.LocatePeriod{
					Keep: 1,
				},
				Week: loc.LocatePeriod{
					Keep: 1,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Contains(t, kept, id)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "day",
			Bucket: loc.Days.Key(ts),
			Rank:   1,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[id])
	})

	t.Run("KeepsAllMatchedItemsWhenNoPeriodsAndMultipleFiltersAreConfigured", func(t *testing.T) {
		id1 := objects.MAC{1}
		id2 := objects.MAC{2}
		id3 := objects.MAC{3}

		items := []loc.Item{
			{
				ItemID:    id1,
				Timestamp: time.Date(2025, time.August, 28, 11, 0, 0, 0, time.UTC),
				Filters: loc.ItemFilters{
					Name:        "daily-backup",
					Category:    "database",
					Environment: "prod",
					Perimeter:   "eu-west",
					Job:         "nightly",
					Tags:        []string{"daily", "important"},
					Roots:       []string{"/var/backups"},
				},
			},
			{
				ItemID:    id2,
				Timestamp: time.Date(2025, time.August, 27, 11, 0, 0, 0, time.UTC),
				Filters: loc.ItemFilters{
					Name:        "daily-backup",
					Category:    "database",
					Environment: "prod",
					Perimeter:   "eu-west",
					Job:         "nightly",
					Tags:        []string{"daily", "important"},
					Roots:       []string{"/var/backups"},
				},
			},
			{
				ItemID:    id3,
				Timestamp: time.Date(2025, time.August, 26, 11, 0, 0, 0, time.UTC),
				Filters: loc.ItemFilters{
					Name:        "daily-backup",
					Category:    "database",
					Environment: "prod",
					Perimeter:   "eu-west",
					Job:         "nightly",
					Tags:        []string{"daily"},
					Roots:       []string{"/var/backups"},
				},
			},
		}

		lo := &loc.LocateOptions{
			Filters: loc.LocateFilters{
				Name:        "daily-backup",
				Category:    "database",
				Environment: "prod",
				Perimeter:   "eu-west",
				Job:         "nightly",
				Tags:        []string{"daily", "important"},
				Roots:       []string{"/var/backups"},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Len(t, kept, 2)
		require.Contains(t, kept, id1)
		require.Contains(t, kept, id2)
		require.NotContains(t, kept, id3)
		require.Len(t, reasons, 2)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Note:   "matched filters",
		}, reasons[id1])
		require.Equal(t, loc.Reason{
			Action: "keep",
			Note:   "matched filters",
		}, reasons[id2])
	})
	t.Run("MondayKeepUsesMondayBucketsOnly", func(t *testing.T) {
		currentMondayID := objects.MAC{1}
		previousMondayID := objects.MAC{2}
		olderMondayID := objects.MAC{3}
		tuesdaySameWeekID := objects.MAC{4}

		currentMonday := time.Date(2025, time.August, 25, 10, 0, 0, 0, time.UTC)  // 2025-W35-monday
		previousMonday := time.Date(2025, time.August, 18, 9, 0, 0, 0, time.UTC)  // 2025-W34-monday
		olderMonday := time.Date(2025, time.August, 11, 8, 0, 0, 0, time.UTC)     // 2025-W33-monday
		tuesdaySameWeek := time.Date(2025, time.August, 26, 7, 0, 0, 0, time.UTC) // 2025-W35-tuesday

		items := []loc.Item{
			{ItemID: currentMondayID, Timestamp: currentMonday},
			{ItemID: previousMondayID, Timestamp: previousMonday},
			{ItemID: olderMondayID, Timestamp: olderMonday},
			{ItemID: tuesdaySameWeekID, Timestamp: tuesdaySameWeek},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Monday: loc.LocatePeriod{
					Keep: 2,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Len(t, kept, 2)
		require.Contains(t, kept, currentMondayID)
		require.Contains(t, kept, previousMondayID)
		require.NotContains(t, kept, olderMondayID)
		require.NotContains(t, kept, tuesdaySameWeekID)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "monday",
			Bucket: loc.Mondays.Key(currentMonday),
			Rank:   1,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[currentMondayID])
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "monday",
			Bucket: loc.Mondays.Key(previousMonday),
			Rank:   1,
			Cap:    0,
			Note:   "within bucket",
		}, reasons[previousMondayID])
		require.Equal(t, loc.Reason{
			Action: "delete",
			Note:   "outside retention windows",
		}, reasons[olderMondayID])
		require.Equal(t, loc.Reason{
			Action: "delete",
			Note:   "outside retention windows",
		}, reasons[tuesdaySameWeekID])
	})

	t.Run("MondayCapAppliesWithinAMondayBucket", func(t *testing.T) {
		newestMondayID := objects.MAC{5}
		olderSameMondayID := objects.MAC{6}
		previousMondayID := objects.MAC{7}

		newestMonday := time.Date(2025, time.August, 25, 18, 0, 0, 0, time.UTC)   // 2025-W35-monday
		olderSameMonday := time.Date(2025, time.August, 25, 9, 0, 0, 0, time.UTC) // same monday bucket
		previousMonday := time.Date(2025, time.August, 18, 12, 0, 0, 0, time.UTC) // 2025-W34-monday

		items := []loc.Item{
			{ItemID: newestMondayID, Timestamp: newestMonday},
			{ItemID: olderSameMondayID, Timestamp: olderSameMonday},
			{ItemID: previousMondayID, Timestamp: previousMonday},
		}

		lo := &loc.LocateOptions{
			Periods: loc.LocatePeriods{
				Monday: loc.LocatePeriod{
					Keep: 2,
					Cap:  1,
				},
			},
		}

		kept, reasons := lo.Match(items, now)
		require.Len(t, kept, 2)
		require.Contains(t, kept, newestMondayID)
		require.Contains(t, kept, previousMondayID)
		require.NotContains(t, kept, olderSameMondayID)
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "monday",
			Bucket: loc.Mondays.Key(newestMonday),
			Rank:   1,
			Cap:    1,
			Note:   "within bucket",
		}, reasons[newestMondayID])
		require.Equal(t, loc.Reason{
			Action: "delete",
			Rule:   "monday",
			Bucket: loc.Mondays.Key(olderSameMonday),
			Rank:   2,
			Cap:    1,
			Note:   "exceeds per-bucket cap",
		}, reasons[olderSameMondayID])
		require.Equal(t, loc.Reason{
			Action: "keep",
			Rule:   "monday",
			Bucket: loc.Mondays.Key(previousMonday),
			Rank:   1,
			Cap:    1,
			Note:   "within bucket",
		}, reasons[previousMondayID])
	})
}
