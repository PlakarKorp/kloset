package locate

import (
	"flag"
	"io"
	"reflect"
	"testing"
	"time"
)

func mustParseRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time %q: %v", s, err)
	}
	return tt
}

func newFlagSet(t *testing.T) *flag.FlagSet {
	t.Helper()
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	// Silence usage/error output in tests
	fs.SetOutput(io.Discard)
	return fs
}

func TestInstallLocateFlags_ParsesAllFlags(t *testing.T) {
	var po LocateOptions
	fs := newFlagSet(t)
	po.InstallLocateFlags(fs)

	args := []string{
		"--before=2024-01-02T03:04:05Z",
		"--since=2024-01-01T00:00:00Z",
		"--name=foo",
		"--category=db",
		"--environment=prod",
		"--perimeter=eu",
		"--job=backup",
		"--latest",

		// repeated/comma-separated
		"--source=repo1, repo2",
		"--source=repo3",

		"--tag=a,b",
		"--tag=c",

		// retention
		"--minutes=5",
		"--hours=6",
		"--days=7",
		"--weeks=8",
		"--months=9",
		"--years=10",

		// caps
		"--per-minute=11",
		"--per-hour=12",
		"--per-day=13",
		"--per-week=14",
		"--per-month=15",
		"--per-year=16",
	}

	if err := fs.Parse(args); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Generic filters
	if got, want := po.Filters.Name, "foo"; got != want {
		t.Fatalf("Name: got %q want %q", got, want)
	}
	if got, want := po.Filters.Category, "db"; got != want {
		t.Fatalf("Category: got %q want %q", got, want)
	}
	if got, want := po.Filters.Environment, "prod"; got != want {
		t.Fatalf("Environment: got %q want %q", got, want)
	}
	if got, want := po.Filters.Perimeter, "eu"; got != want {
		t.Fatalf("Perimeter: got %q want %q", got, want)
	}
	if got, want := po.Filters.Job, "backup"; got != want {
		t.Fatalf("Job: got %q want %q", got, want)
	}
	if !po.Filters.Latest {
		t.Fatalf("Latest: got false want true")
	}

	// Time flags
	wantBefore := mustParseRFC3339(t, "2024-01-02T03:04:05Z")
	wantSince := mustParseRFC3339(t, "2024-01-01T00:00:00Z")
	if !po.Filters.Before.Equal(wantBefore) {
		t.Fatalf("Before: got %v want %v", po.Filters.Before, wantBefore)
	}
	if !po.Filters.Since.Equal(wantSince) {
		t.Fatalf("Since: got %v want %v", po.Filters.Since, wantSince)
	}

	// Accumulating list flags
	wantRoots := []string{"repo1", "repo2", "repo3"}
	if !reflect.DeepEqual(po.Filters.Roots, wantRoots) {
		t.Fatalf("Roots: got %#v want %#v", po.Filters.Roots, wantRoots)
	}
	wantTags := []string{"a", "b", "c"}
	if !reflect.DeepEqual(po.Filters.Tags, wantTags) {
		t.Fatalf("Tags: got %#v want %#v", po.Filters.Tags, wantTags)
	}

	// Retention
	if po.Minute.Keep != 5 || po.Hour.Keep != 6 || po.Day.Keep != 7 || po.Week.Keep != 8 || po.Month.Keep != 9 || po.Year.Keep != 10 {
		t.Fatalf("Keep values: got (m=%d h=%d d=%d w=%d mo=%d y=%d) want (5 6 7 8 9 10)",
			po.Minute.Keep, po.Hour.Keep, po.Day.Keep, po.Week.Keep, po.Month.Keep, po.Year.Keep)
	}

	// Caps
	if po.Minute.Cap != 11 || po.Hour.Cap != 12 || po.Day.Cap != 13 || po.Week.Cap != 14 || po.Month.Cap != 15 || po.Year.Cap != 16 {
		t.Fatalf("Cap values: got (m=%d h=%d d=%d w=%d mo=%d y=%d) want (11 12 13 14 15 16)",
			po.Minute.Cap, po.Hour.Cap, po.Day.Cap, po.Week.Cap, po.Month.Cap, po.Year.Cap)
	}
}

func TestInstallDeletionFlags_OnlyGenericFlags(t *testing.T) {
	var po LocateOptions
	fs := newFlagSet(t)
	po.InstallDeletionFlags(fs)

	// Generic-only args should parse fine.
	okArgs := []string{
		"--before=2023-12-31T23:59:59Z",
		"--since=2023-12-01T00:00:00Z",
		"--name=n",
		"--category=c",
		"--environment=e",
		"--perimeter=p",
		"--job=j",
		"--latest",
		"--source=one, two",
		"--tag=x,y",
	}

	if err := fs.Parse(okArgs); err != nil {
		t.Fatalf("Parse generic flags failed: %v", err)
	}

	// Retention flags must NOT be registered in deletion mode.
	badArgs := []string{"--minutes=1"}
	if err := fs.Parse(badArgs); err == nil {
		t.Fatalf("expected error for unknown flag --minutes in deletion mode, got nil")
	}

	// Also ensure caps are not present.
	badArgs2 := []string{"--per-hour=1"}
	fs = newFlagSet(t)
	po.InstallDeletionFlags(fs)
	if err := fs.Parse(badArgs2); err == nil {
		t.Fatalf("expected error for unknown flag --per-hour in deletion mode, got nil")
	}
}

func TestSourceAndTagTrimAndEmptyAreIgnored(t *testing.T) {
	var po LocateOptions
	fs := newFlagSet(t)
	po.InstallLocateFlags(fs)

	args := []string{
		`--source= alpha ,  ,beta,  `,
		`--source= , , `,
		`--tag=  t1 , , t2`,
	}

	if err := fs.Parse(args); err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if want := []string{"alpha", "beta"}; !reflect.DeepEqual(po.Filters.Roots, want) {
		t.Fatalf("Roots trim/empty handling: got %#v want %#v", po.Filters.Roots, want)
	}
	if want := []string{"t1", "t2"}; !reflect.DeepEqual(po.Filters.Tags, want) {
		t.Fatalf("Tags trim/empty handling: got %#v want %#v", po.Filters.Tags, want)
	}
}
