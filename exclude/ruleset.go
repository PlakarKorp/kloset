package exclude

import (
	"fmt"
	"io"
	"iter"
	"strings"
)

type RuleSet struct {
	Root  string
	Rules []*Rule
}

func NewRuleSet(root string) *RuleSet {
	return &RuleSet{
		Root: toSlashNoTrail(root),
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

func (ruleset *RuleSet) Match(path string, isDir bool) (bool, *Rule, error) {
	comps := strings.Split(path, "/")

	var matchedRule *Rule
	ignore := false
	for _, rule := range ruleset.Rules {
		matched, excluded, err := RuleMatch(rule, comps, isDir)
		if err != nil {
			return false, rule, err
		}
		if matched {
			ignore = excluded
			matchedRule = rule
		}
	}

	return ignore, matchedRule, nil
}

func (ruleset *RuleSet) IsExcluded(path string, isDir bool) bool {
	exclude, _, _ := ruleset.Match(path, isDir)
	return exclude
}
