package exclude

import (
	"fmt"
	"io"
	"iter"
)

type RuleSet struct {
	Root    string
	Rules   []*Rule
	Matcher RuleMatcher
}

func NewRuleSet(root string, matcher RuleMatcher) *RuleSet {
	return &RuleSet{
		Root:    toSlashNoTrail(root),
		Matcher: matcher,
	}
}

func (ruleset *RuleSet) AddRule(pattern string) error {
	rule, err := ParseRule(pattern)
	if err != nil {
		return fmt.Errorf("failed to parse rule: %w", err)
	}
	ruleset.Rules = append(ruleset.Rules, rule)
	return nil
}

func (ruleset *RuleSet) AddRules(iterator iter.Seq2[string, error]) error {
	lineno := 0
	for line, err := range iterator {
		if err != nil {
			return fmt.Errorf("iteration failed: %w", err)
		}
		lineno++
		pattern := preprocessLine(line)
		if pattern == "" {
			continue
		}
		err := ruleset.AddRule(pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern on line %v: %q: %w", lineno, line, err)
		}
	}
	return nil
}

func (ruleset *RuleSet) AddRulesFromArray(lines []string) error {
	return ruleset.AddRules(iterArray(lines))
}

func (ruleset *RuleSet) AddRulesFromReader(rd io.Reader) error {
	return ruleset.AddRules(iterReader(rd))
}

func (ruleset *RuleSet) AddRulesFromFile(filename string) error {
	return ruleset.AddRules(iterFile(filename))
}

// Ignore reports whether path (relative or absolute) is ignored.
// isDir should be true if `path` is a directory (needed for patterns with trailing '/').
func (ruleset *RuleSet) Match(path string, isDir bool) (bool, *Rule, error) {
	//rel := relativeTo(path, ruleset.Root)
	//rel = strings.TrimLeft(rel, "/") // .gitignore patterns see paths relative to the file
	rel := path

	var matchedRule *Rule
	matched := false
	for _, rule := range ruleset.Rules {
		if rule.DirOnly && !isDir {
			continue
		}

		m, err := ruleset.Matcher(rule, rel)
		if err != nil {
			return false, rule, err
		}
		if m {
			matchedRule = rule
			matched = !rule.Negate
			// last match wins; keep scanning
		}
	}

	return matched, matchedRule, nil
}
