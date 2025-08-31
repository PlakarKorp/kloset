package exclude

import (
	"fmt"
	"os"
	"os/exec"
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

type RuleMatcher func(*Rule, string) (bool, error)

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

func RuleMatchRegex(rule *Rule, path string) (bool, error) {
	return rule.Re.MatchString(path), nil
}

func RuleMatchDoubleStar(rule *Rule, path string) (bool, error) {
	return doublestar.Match(rule.Pattern, path)
}

func RuleMatchGit(rule *Rule, path string) (bool, error) {
	tmp, err := os.MkdirTemp("/tmp", "git.*")
	if err != nil {
		return false, err
	}
	defer os.RemoveAll(tmp)
	_, err = exec.Command("git", "-C", tmp, "init").Output()
	if err != nil {
		return false, err
	}
	fd, err := os.Create(tmp + "/.gitignore")
	if err != nil {
		return false, err
	}
	defer fd.Close()
	_, err = fd.WriteString(rule.Pattern + "\n")
	if err != nil {
		return false, err
	}
	fd.Close()
	out, err := exec.Command("git", "-C", tmp, "check-ignore", path).Output()
	if err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			if exiterr.ExitCode() == 1 {
				return false, nil
			}
		}
		return false, err
	}
	res := strings.TrimSuffix(string(out), "\n")

	switch res {
	case path:
		return true, nil
	default:
		return false, fmt.Errorf("unexpected output %q", out)
	}
}
