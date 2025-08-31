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
	Negate   bool // '!' at start
	DirOnly  bool // trailing '/'
	Anchored bool // contains non-trailing '/'

	Raw      string // original pattern (for git)
	Globstar string // resulting globstar pattern (for doublestar)
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

	// Anchored if contains '/')
	if strings.ContainsRune(pattern, '/') {
		rule.Anchored = true
		pattern = strings.TrimPrefix(pattern, "/")
	}

	// Compile to regex
	var err error
	rule.Re, err = regexp.Compile(toRegex(pattern, rule.Anchored))
	if err != nil {
		return nil, fmt.Errorf("failed to compile regexp: %w", err)
	}

	// Tweak for doublestar
	if !rule.Anchored && !strings.HasPrefix(pattern, "**/") {
		pattern = "**/" + pattern
	}
	if rule.DirOnly {
		pattern = pattern + "/"
	}
	_, err = doublestar.Match(pattern, "foo")
	if err != nil {
		return nil, fmt.Errorf("failed to parse doublestar pattern: %w", err)
	}
	rule.Globstar = pattern

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
	return doublestar.Match(rule.Globstar, path)
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
	_, err = fd.WriteString(rule.Raw + "\n")
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
		return rule.Negate, err
	}
	res := strings.TrimSuffix(string(out), "\n")
	if res == path {
		return !rule.Negate, nil
	}

	return false, fmt.Errorf("unexpected output %q", res)
}
