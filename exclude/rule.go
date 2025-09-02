package exclude

import (
	"fmt"

	"github.com/go-git/go-git/plumbing/format/gitignore"
)

type Rule struct {
	Pattern gitignore.Pattern
}

func ParseRule(pattern string) (*Rule, error) {
	return &Rule{
		Pattern: gitignore.ParsePattern(pattern, []string{}),
	}, nil
}

func MustParseRule(pattern string) *Rule {
	rule, err := ParseRule(pattern)
	if err != nil {
		panic(err)
	}
	return rule
}

func (rule *Rule) Match(path []string, isDir bool) (bool, bool, error) {
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
