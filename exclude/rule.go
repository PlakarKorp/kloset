package exclude

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
)

type Rule struct {
	Negate   bool   // '!' at start
	DirOnly  bool   // trailing '/'
	Anchored bool   // leading '/'
	Pattern  string // resulting pattern
	Raw      string // original line
	Re       *regexp.Regexp
}

func ParseRule(pattern string) (*Rule, error) {
	rule := Rule{}
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

	// Anchored (leading '/')
	if strings.HasPrefix(pattern, "/") {
		rule.Anchored = true
		pattern = strings.TrimPrefix(pattern, "/")
	}
	rule.Pattern = pattern

	var err error
	rule.Re, err = regexp.Compile(toRegex(rule.Pattern, rule.Anchored))
	if err != nil {
		return nil, fmt.Errorf("failed to compile regexp: %w", err)
	}

	_, err = doublestar.Match(rule.Pattern, "foo")
	if err != nil {
		return nil, fmt.Errorf("failed to parse dublestar pattern: %w", err)
	}

	return &rule, nil
}

func MustParseRule(pattern string) *Rule {
	rule, err := ParseRule(pattern)
	if err != nil {
		panic(err)
	}
	return rule
}

func (rule *Rule) Match(path string, useRegex bool) bool {
	if useRegex {
		return rule.Re.MatchString(path)
	}
	m, _ := doublestar.Match(rule.Pattern, path)
	return m
}
