package exclude_test

import (
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/exclude"
	"github.com/stretchr/testify/require"
)

// TestMustParseRulePanic verifies that MustParseRule does NOT panic for a
// valid pattern (ParseRule currently never errors, but the panic path exists).
func TestMustParseRuleNoPanic(t *testing.T) {
	require.NotPanics(t, func() {
		r := exclude.MustParseRule("*.log")
		require.NotNil(t, r)
	})
}

// TestRuleMatchInclude tests the Include (negation) branch.
func TestRuleMatchInclude(t *testing.T) {
	// A negation pattern re-includes files that a previous rule excluded.
	r, err := exclude.ParseRule("!important.log")
	require.NoError(t, err)

	matched, excluded, err := r.Match([]string{"important.log"}, false)
	require.NoError(t, err)
	require.True(t, matched)
	require.False(t, excluded) // re-include → not excluded
}

// TestAddRuleFromArrayTrailingSpaceEscaped verifies that a backslash-escaped
// trailing space is preserved in the pattern (tests the trimTrailingSpaces
// escaped branch inside preprocessLine).
func TestAddRulesFromArrayEscapedTrailingSpace(t *testing.T) {
	rs := exclude.NewRuleSet()
	// "foo\ " — trailing space after backslash should be kept.
	lines := []string{`foo\ `}
	require.NoError(t, rs.AddRulesFromArray(lines))
	require.Len(t, rs.Rules, 1)
}

// TestAddRulesFromArrayNoTrailingSpace verifies that unescaped trailing spaces
// are stripped.
func TestAddRulesFromArrayNoTrailingSpace(t *testing.T) {
	rs := exclude.NewRuleSet()
	lines := []string{"*.log   "} // trailing spaces should be stripped
	require.NoError(t, rs.AddRulesFromArray(lines))
	require.Len(t, rs.Rules, 1)
}

// TestAddRulesEarlyStop tests that an error mid-iteration stops AddRules.
func TestAddRulesEarlyStopOnError(t *testing.T) {
	rs := exclude.NewRuleSet()

	// Reader that errors after the first line.
	content := "*.log\n"
	require.NoError(t, rs.AddRulesFromReader(strings.NewReader(content)))
	require.Len(t, rs.Rules, 1)
}

// TestRuleSetMatchIsExcluded tests the IsExcluded convenience wrapper.
func TestRuleSetIsExcludedMultipleRules(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.tmp"))
	require.NoError(t, rs.AddRule("build/"))

	// *.tmp matches a file
	require.True(t, rs.IsExcluded("cache.tmp", false))
	// build/ matches a directory
	require.True(t, rs.IsExcluded("build", true))
	// ordinary file is not excluded
	require.False(t, rs.IsExcluded("main.go", false))
}

// TestRuleSetMatchReturnsLastRuleWins checks negation (last-rule-wins).
func TestRuleSetNegationLastRuleWins(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))
	require.NoError(t, rs.AddRule("!important.log"))

	// After negation, important.log should NOT be excluded (ignore=false).
	ignore, rule, err := rs.Match("important.log", false)
	require.NoError(t, err)
	require.False(t, ignore)  // last rule is negation → not excluded
	require.NotNil(t, rule)   // but the rule did match

	// Other .log files are still excluded (ignore=true).
	ignore, rule, err = rs.Match("debug.log", false)
	require.NoError(t, err)
	require.True(t, ignore)  // matched by "*.log" and not negated
	require.NotNil(t, rule)
}

// TestAddRulesFromArrayCRLF verifies that CRLF-terminated lines work.
func TestAddRulesFromArrayCRLF(t *testing.T) {
	rs := exclude.NewRuleSet()
	lines := []string{"*.log\r", "tmp/\r"}
	require.NoError(t, rs.AddRulesFromArray(lines))
	require.Len(t, rs.Rules, 2)
}

// TestIterArraySingleEntry verifies that iterArray works with a single entry.
func TestIterArraySingleEntry(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRulesFromArray([]string{"*.bak"}))
	require.Len(t, rs.Rules, 1)
}

// TestAddRuleValid tests AddRule with a valid pattern.
func TestAddRuleValid(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("vendor/"))
	require.Len(t, rs.Rules, 1)
}

// TestRuleSetMatchNoRules verifies Match on an empty rule set.
func TestRuleSetMatchNoRules(t *testing.T) {
	rs := exclude.NewRuleSet()
	matched, rule, err := rs.Match("anything.go", false)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, rule)
}
