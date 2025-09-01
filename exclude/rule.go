package exclude

import (
	"fmt"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
)

type Rule struct {
	Raw      string // original pattern
	Negate   bool   // '!' at start
	DirOnly  bool   // trailing '/'
	Anchored bool   // contains non-trailing '/'
	Path     []string
	Pattern  gitignore.Pattern
}

type RuleMatcher func(*Rule, []string, bool) (bool, bool, error)

func ParseRule(pattern string) (*Rule, error) {
	rule := Rule{}
	rule.Pattern = gitignore.ParsePattern(pattern, []string{})
	rule.Raw = pattern

	// Negation
	if strings.HasPrefix(pattern, "!") {
		rule.Negate = true
		pattern = pattern[1:]
		// If the remainder is empty, it's a no-op; Git treats it as invalid—ignore here.
		if pattern == "" {
			return nil, fmt.Errorf("empty negation pattern")
		}
	}

	// Normalize multiple consecutive slashes to a single slash
	for strings.Contains(pattern, "//") {
		pattern = strings.ReplaceAll(pattern, "//", "/")
	}

	// Directory-only (trailing '/')
	if strings.HasSuffix(pattern, "/") && pattern != "/" {
		rule.DirOnly = true
		pattern = strings.TrimSuffix(pattern, "/")
	}

	// Anchored if contains '/')
	if strings.ContainsRune(pattern, '/') {
		rule.Anchored = true
		pattern = strings.TrimPrefix(pattern, "/")
	}
	rule.Path = strings.Split(pattern, "/")
	return &rule, nil
}

func MustParseRule(pattern string) *Rule {
	rule, err := ParseRule(pattern)
	if err != nil {
		panic(err)
	}
	return rule
}

func RuleMatch(rule *Rule, path []string, isDir bool) (bool, bool, error) {
	switch rule.Pattern.Match(path, isDir) {
	case gitignore.NoMatch:
		return false, false, nil
	case gitignore.Exclude:
		return true, true, nil
	case gitignore.Include:
		return true, false, nil
	default:
		return false, false, fmt.Errorf("unexpected result")
	}
}
