package locate_test

import (
	"testing"
	"time"

	loc "github.com/PlakarKorp/kloset/locate"
	"github.com/stretchr/testify/require"
)

var dateRef = time.Date(2023, time.March, 15, 13, 45, 59, 0, time.UTC)

func TestDays(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Days.Key(dateRef)
		require.Equal(t, "2023-03-15", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Days.Start(dateRef)
		want := time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Days.Start(dateRef)
		got := loc.Days.Prev(start)
		want := time.Date(2023, 3, 14, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Days.Start(input)
		want := time.Date(2023, 7, 5, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestMinutes(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Minutes.Key(dateRef)
		require.Equal(t, "2023-03-15-13:45", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Minutes.Start(dateRef)
		want := time.Date(2023, 3, 15, 13, 45, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Minutes.Start(dateRef)
		got := loc.Minutes.Prev(start)
		want := time.Date(2023, 3, 15, 13, 44, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Minutes.Start(input)
		want := time.Date(2023, 7, 5, 3, 59, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestHours(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Hours.Key(dateRef)
		require.Equal(t, "2023-03-15-13", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Hours.Start(dateRef)
		want := time.Date(2023, 3, 15, 13, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Hours.Start(dateRef)
		got := loc.Hours.Prev(start)
		want := time.Date(2023, 3, 15, 12, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Hours.Start(input)
		want := time.Date(2023, 7, 5, 3, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestMonths(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Months.Key(dateRef)
		require.Equal(t, "2023-03", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Months.Start(dateRef)
		want := time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Months.Start(dateRef)
		got := loc.Months.Prev(start)
		want := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 7, 4, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Months.Start(input)
		want := time.Date(2023, 7, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestYears(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		got := loc.Years.Key(dateRef)
		require.Equal(t, "2023", got)
	})

	t.Run("Start", func(t *testing.T) {
		got := loc.Years.Start(dateRef)
		want := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Years.Start(dateRef)
		got := loc.Years.Prev(start)
		want := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("normalizes input from another timezone", func(t *testing.T) {
		input := time.Date(2023, 12, 31, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Years.Start(input)
		want := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestWeeks(t *testing.T) {
	t.Run("Key", func(t *testing.T) {
		t.Run("returns ISO week for a regular date", func(t *testing.T) {
			got := loc.Weeks.Key(dateRef)
			require.Equal(t, "2023-W11", got)
		})

		t.Run("handles ISO year boundary", func(t *testing.T) {
			input := time.Date(2021, 1, 1, 10, 0, 0, 0, time.UTC)
			got := loc.Weeks.Key(input)
			require.Equal(t, "2020-W53", got)
		})
	})

	t.Run("Start", func(t *testing.T) {
		t.Run("returns monday at midnight for a weekday", func(t *testing.T) {
			got := loc.Weeks.Start(dateRef)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("returns previous monday for a sunday", func(t *testing.T) {
			input := time.Date(2023, 3, 19, 23, 59, 59, 0, time.UTC)
			got := loc.Weeks.Start(input)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("normalizes input from another timezone", func(t *testing.T) {
			input := time.Date(2023, 7, 9, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
			got := loc.Weeks.Start(input)
			want := time.Date(2023, 7, 10, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})
	})

	t.Run("Prev", func(t *testing.T) {
		start := loc.Weeks.Start(dateRef)
		got := loc.Weeks.Prev(start)
		want := time.Date(2023, 3, 6, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestWeekdayPeriod(t *testing.T) {
	t.Run("Name", func(t *testing.T) {
		got := loc.WeekdayPeriod(time.Monday)
		require.Equal(t, "monday", got.Name)
	})

	t.Run("Start", func(t *testing.T) {
		t.Run("aligns to monday of the same ISO week", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			got := monday.Start(dateRef)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("aligns to thursday of the same ISO week", func(t *testing.T) {
			thursday := loc.WeekdayPeriod(time.Thursday)
			got := thursday.Start(dateRef)
			want := time.Date(2023, 3, 16, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("normalizes input from another timezone", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			input := time.Date(2023, 7, 9, 23, 59, 59, 0, time.FixedZone("UTC-4", -4*60*60))
			got := monday.Start(input)
			want := time.Date(2023, 7, 10, 0, 0, 0, 0, time.UTC)
			require.Equal(t, want, got)
		})

		t.Run("handles sunday input when aligning to monday", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			input := time.Date(2023, 3, 19, 23, 59, 59, 0, time.UTC) // sunday

			got := monday.Start(input)
			want := time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC)

			require.Equal(t, want, got)
		})
	})

	t.Run("Prev", func(t *testing.T) {
		monday := loc.WeekdayPeriod(time.Monday)
		start := monday.Start(dateRef)
		got := monday.Prev(start)
		want := time.Date(2023, 3, 6, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Key", func(t *testing.T) {
		t.Run("returns ISO week key for aligned monday", func(t *testing.T) {
			monday := loc.WeekdayPeriod(time.Monday)
			start := monday.Start(dateRef)
			got := monday.Key(start)
			require.Equal(t, "2023-W11-monday", got)
		})

		t.Run("handles ISO year boundary for aligned thursday", func(t *testing.T) {
			thursday := loc.WeekdayPeriod(time.Thursday)
			input := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			start := thursday.Start(input)
			got := thursday.Key(start)
			require.Equal(t, "2026-W01-thursday", got)
		})
	})
}

func TestWeekdayAliases(t *testing.T) {
	tests := []struct {
		name      string
		period    loc.Period
		target    time.Weekday
		wantName  string
		wantStart time.Time
		wantPrev  time.Time
		wantKey   string
	}{
		{
			name:      "Mondays",
			period:    loc.Mondays,
			target:    time.Monday,
			wantName:  "monday",
			wantStart: time.Date(2023, 3, 13, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 6, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-monday",
		},
		{
			name:      "Tuesdays",
			period:    loc.Tuesdays,
			target:    time.Tuesday,
			wantName:  "tuesday",
			wantStart: time.Date(2023, 3, 14, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 7, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-tuesday",
		},
		{
			name:      "Wednesdays",
			period:    loc.Wednesdays,
			target:    time.Wednesday,
			wantName:  "wednesday",
			wantStart: time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 8, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-wednesday",
		},
		{
			name:      "Thursdays",
			period:    loc.Thursdays,
			target:    time.Thursday,
			wantName:  "thursday",
			wantStart: time.Date(2023, 3, 16, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 9, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-thursday",
		},
		{
			name:      "Fridays",
			period:    loc.Fridays,
			target:    time.Friday,
			wantName:  "friday",
			wantStart: time.Date(2023, 3, 17, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 10, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-friday",
		},
		{
			name:      "Saturdays",
			period:    loc.Saturdays,
			target:    time.Saturday,
			wantName:  "saturday",
			wantStart: time.Date(2023, 3, 18, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 11, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-saturday",
		},
		{
			name:      "Sundays",
			period:    loc.Sundays,
			target:    time.Sunday,
			wantName:  "sunday",
			wantStart: time.Date(2023, 3, 19, 0, 0, 0, 0, time.UTC),
			wantPrev:  time.Date(2023, 3, 12, 0, 0, 0, 0, time.UTC),
			wantKey:   "2023-W11-sunday",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("Name", func(t *testing.T) {
				require.Equal(t, tt.wantName, tt.period.Name)
			})

			t.Run("Start", func(t *testing.T) {
				got := tt.period.Start(dateRef)
				require.Equal(t, tt.wantStart, got)
			})

			t.Run("Prev", func(t *testing.T) {
				start := tt.period.Start(dateRef)
				got := tt.period.Prev(start)
				require.Equal(t, tt.wantPrev, got)
			})

			t.Run("Key", func(t *testing.T) {
				start := tt.period.Start(dateRef)
				got := tt.period.Key(start)
				require.Equal(t, tt.wantKey, got)
			})

			t.Run("matches factory output", func(t *testing.T) {
				factory := loc.WeekdayPeriod(tt.target)
				start := tt.period.Start(dateRef)
				require.Equal(t, factory.Name, tt.period.Name)
				require.Equal(t, factory.Start(dateRef), tt.period.Start(dateRef))
				require.Equal(t, factory.Prev(start), tt.period.Prev(start))
				require.Equal(t, factory.Key(start), tt.period.Key(start))
			})
		})
	}
}

func TestPeriodLastNKeys(t *testing.T) {
	t.Run("returns empty map when n is zero", func(t *testing.T) {
		got := loc.Days.LastNKeys(dateRef, 0)
		require.Empty(t, got)
	})

	t.Run("returns last N day keys including current day bucket", func(t *testing.T) {
		got := loc.Days.LastNKeys(dateRef, 3)
		require.Len(t, got, 3)
		require.Contains(t, got, "2023-03-15")
		require.Contains(t, got, "2023-03-14")
		require.Contains(t, got, "2023-03-13")
	})

	t.Run("returns last N minute keys including current minute bucket", func(t *testing.T) {
		got := loc.Minutes.LastNKeys(dateRef, 3)
		require.Len(t, got, 3)
		require.Contains(t, got, "2023-03-15-13:45")
		require.Contains(t, got, "2023-03-15-13:44")
		require.Contains(t, got, "2023-03-15-13:43")
	})

	t.Run("returns last N weekday alias keys from aligned buckets", func(t *testing.T) {
		got := loc.Mondays.LastNKeys(dateRef, 3)
		require.Len(t, got, 3)
		require.Contains(t, got, "2023-W11-monday")
		require.Contains(t, got, "2023-W10-monday")
		require.Contains(t, got, "2023-W09-monday")
	})

	t.Run("uses previous bucket when aligned weekday start is after now", func(t *testing.T) {
		got := loc.Thursdays.LastNKeys(dateRef, 3)
		require.Len(t, got, 3)
		require.Contains(t, got, "2023-W10-thursday")
		require.Contains(t, got, "2023-W09-thursday")
		require.Contains(t, got, "2023-W08-thursday")
		require.NotContains(t, got, "2023-W11-thursday")
	})
}

func TestPrevMonotonicBackwards(t *testing.T) {
	references := []struct {
		name string
		ref  time.Time
	}{
		{name: "regular date", ref: dateRef},
		{name: "leap day", ref: time.Date(2024, 2, 29, 12, 34, 56, 0, time.UTC)},
	}

	periods := []struct {
		name   string
		period loc.Period
	}{
		{name: "Days", period: loc.Days},
		{name: "Minutes", period: loc.Minutes},
		{name: "Hours", period: loc.Hours},
		{name: "Months", period: loc.Months},
		{name: "Years", period: loc.Years},
		{name: "Weeks", period: loc.Weeks},
		{name: "Mondays", period: loc.Mondays},
		{name: "Thursdays", period: loc.Thursdays},
	}

	for _, ref := range references {
		t.Run(ref.name, func(t *testing.T) {
			for _, tt := range periods {
				t.Run(tt.name, func(t *testing.T) {
					start := tt.period.Start(ref.ref)
					prev := tt.period.Prev(start)
					prevStart := tt.period.Start(prev)

					require.True(t, prev.Before(start), "Prev(%v) should be before %v", prev, start)
					require.False(t, prevStart.After(start), "Start(Prev(start)) = %v should not be after %v", prevStart, start)
				})
			}
		})
	}
}

func TestWeeksCalendarBoundaries(t *testing.T) {
	t.Run("Key handles ISO year boundary", func(t *testing.T) {
		input := time.Date(2021, 1, 1, 10, 0, 0, 0, time.UTC)
		got := loc.Weeks.Key(input)
		require.Equal(t, "2020-W53", got)
	})

	t.Run("Start aligns sunday to previous ISO monday across year boundary", func(t *testing.T) {
		input := time.Date(2021, 1, 3, 23, 59, 59, 0, time.UTC)
		got := loc.Weeks.Start(input)
		want := time.Date(2020, 12, 28, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev crosses ISO year boundary", func(t *testing.T) {
		start := loc.Weeks.Start(time.Date(2021, 1, 4, 12, 0, 0, 0, time.UTC))
		got := loc.Weeks.Prev(start)
		want := time.Date(2020, 12, 28, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestMonthsCalendarBoundaries(t *testing.T) {
	t.Run("Start truncates to first day of month at end of month", func(t *testing.T) {
		input := time.Date(2023, 3, 31, 22, 10, 0, 0, time.UTC)
		got := loc.Months.Start(input)
		want := time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Prev crosses from march to february in a regular year", func(t *testing.T) {
		start := loc.Months.Start(time.Date(2023, 3, 15, 13, 45, 59, 0, time.UTC))
		got := loc.Months.Prev(start)
		want := time.Date(2023, 2, 1, 0, 0, 0, 0, time.UTC)

		require.Equal(t, want, got)
	})

	t.Run("Prev crosses from march to february in a leap year", func(t *testing.T) {
		start := loc.Months.Start(time.Date(2024, 3, 15, 13, 45, 59, 0, time.UTC))
		got := loc.Months.Prev(start)
		want := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)

		require.Equal(t, want, got)
	})

	t.Run("normalizes timezone at month boundary", func(t *testing.T) {
		input := time.Date(2023, 3, 31, 23, 30, 0, 0, time.FixedZone("UTC-4", -4*60*60))
		got := loc.Months.Start(input)
		want := time.Date(2023, 4, 1, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})
}

func TestLeapYearBoundaries(t *testing.T) {
	t.Run("Days Key handles leap day", func(t *testing.T) {
		input := time.Date(2024, 2, 29, 12, 34, 56, 0, time.UTC)
		got := loc.Days.Key(input)
		require.Equal(t, "2024-02-29", got)
	})

	t.Run("Days Prev crosses from leap day to previous day", func(t *testing.T) {
		start := loc.Days.Start(time.Date(2024, 2, 29, 12, 34, 56, 0, time.UTC))
		got := loc.Days.Prev(start)
		want := time.Date(2024, 2, 28, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Weeks Start keeps leap day inside its ISO week", func(t *testing.T) {
		input := time.Date(2024, 2, 29, 12, 34, 56, 0, time.UTC)
		got := loc.Weeks.Start(input)
		want := time.Date(2024, 2, 26, 0, 0, 0, 0, time.UTC)
		require.Equal(t, want, got)
	})

	t.Run("Weeks Key handles leap day", func(t *testing.T) {
		input := time.Date(2024, 2, 29, 12, 34, 56, 0, time.UTC)
		got := loc.Weeks.Key(input)
		require.Equal(t, "2024-W09", got)
	})
}
