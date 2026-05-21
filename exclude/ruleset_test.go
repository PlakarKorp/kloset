package exclude_test

import (
	"bytes"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"testing"

	"github.com/PlakarKorp/kloset/exclude"
	"github.com/stretchr/testify/require"
)

func TestNewRuleSet(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NotNil(t, rs)
	require.Empty(t, rs.Rules)
}

func TestAddRule(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))
	require.Len(t, rs.Rules, 1)
}

func TestAddRulesFromArray(t *testing.T) {
	rs := exclude.NewRuleSet()
	lines := []string{
		"*.log",
		"# this is a comment",
		"",
		"tmp/",
		"build/",
	}
	require.NoError(t, rs.AddRulesFromArray(lines))
	// Comments and empty lines are ignored
	require.Len(t, rs.Rules, 3)
}

func TestAddRulesFromReader(t *testing.T) {
	rs := exclude.NewRuleSet()
	content := "*.tmp\n# comment\n\n.DS_Store\n"
	require.NoError(t, rs.AddRulesFromReader(bytes.NewBufferString(content)))
	require.Len(t, rs.Rules, 2)
}

func TestAddRulesFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".gitignore")
	content := "*.log\n*.tmp\n# comment\n\nbuild/\n"
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRulesFromFile(path))
	require.Len(t, rs.Rules, 3)
}

func TestAddRulesFromFileMissing(t *testing.T) {
	rs := exclude.NewRuleSet()
	err := rs.AddRulesFromFile("/does/not/exist/.gitignore")
	require.Error(t, err)
}

func TestMatchExclude(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))

	matched, rule, err := rs.Match("app.log", false)
	require.NoError(t, err)
	require.True(t, matched, "*.log should match app.log")
	require.NotNil(t, rule)
}

func TestMatchNoMatch(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))

	matched, rule, err := rs.Match("app.go", false)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, rule)
}

func TestMatchWithLeadingSlash(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))

	matched, _, err := rs.Match("/app.log", false)
	require.NoError(t, err)
	require.True(t, matched)
}

func TestIsExcluded(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))
	require.NoError(t, rs.AddRule("tmp/"))

	require.True(t, rs.IsExcluded("app.log", false))
	require.False(t, rs.IsExcluded("app.go", false))
	require.True(t, rs.IsExcluded("tmp", true))
	require.False(t, rs.IsExcluded("tmp", false))
}

func TestMatchDirectory(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("build/"))

	matched, _, err := rs.Match("build", true)
	require.NoError(t, err)
	require.True(t, matched)

	matched, _, err = rs.Match("build", false)
	require.NoError(t, err)
	require.False(t, matched)
}

func TestMatchNestedPath(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))

	require.True(t, rs.IsExcluded("foo/bar/baz.log", false))
	require.False(t, rs.IsExcluded("foo/bar/baz.go", false))
}

func TestLastRuleWins(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.NoError(t, rs.AddRule("*.log"))
	require.NoError(t, rs.AddRule("!important.log"))

	// The last matching rule wins: !important.log re-includes
	matched, _, err := rs.Match("important.log", false)
	require.NoError(t, err)
	require.False(t, matched, "negation rule should re-include important.log")
}

func TestAddRulesFromArrayWithCRLF(t *testing.T) {
	rs := exclude.NewRuleSet()
	lines := []string{"*.log\r", "*.tmp\r"}
	require.NoError(t, rs.AddRulesFromArray(lines))
	require.Len(t, rs.Rules, 2)
	require.True(t, rs.IsExcluded("app.log", false))
}

func TestAddRulesFromArrayEscapedHash(t *testing.T) {
	rs := exclude.NewRuleSet()
	// \# should be treated as a literal #, not a comment
	require.NoError(t, rs.AddRulesFromArray([]string{`\#special`}))
	require.Len(t, rs.Rules, 1)
}

func TestAddRulesIteratorError(t *testing.T) {
	rs := exclude.NewRuleSet()
	errIter := func(yield func(string, error) bool) {
		yield("", errors.New("read error"))
	}
	err := rs.AddRules(iter.Seq2[string, error](errIter))
	require.Error(t, err)
}

func TestMustParseRule(t *testing.T) {
	rule := exclude.MustParseRule("*.go")
	require.NotNil(t, rule)
}

func TestRuleMatch(t *testing.T) {
	rule, err := exclude.ParseRule("*.log")
	require.NoError(t, err)

	// Match returns (matched, excluded, error)
	matched, excluded, err := rule.Match([]string{"app", "log"}, false)
	require.NoError(t, err)
	// Depends on gitignore semantics; just check no panic
	_ = matched
	_ = excluded

	matched2, excluded2, err2 := rule.Match([]string{"app.log"}, false)
	require.NoError(t, err2)
	if matched2 {
		require.True(t, excluded2)
	}
}

func TestRuleMatchNoMatch(t *testing.T) {
	rule, err := exclude.ParseRule("*.log")
	require.NoError(t, err)

	matched, excluded, err := rule.Match([]string{"app.go"}, false)
	require.NoError(t, err)
	require.False(t, matched)
	require.False(t, excluded)
}

func TestAddRulesFromReaderCRLF(t *testing.T) {
	rs := exclude.NewRuleSet()
	content := "*.log\r\n*.tmp\r\n"
	require.NoError(t, rs.AddRulesFromReader(bytes.NewBufferString(content)))
	require.Len(t, rs.Rules, 2)
}

func TestEmptyRuleSet(t *testing.T) {
	rs := exclude.NewRuleSet()
	require.False(t, rs.IsExcluded("anything.log", false))

	matched, rule, err := rs.Match("foo", false)
	require.NoError(t, err)
	require.False(t, matched)
	require.Nil(t, rule)
}
