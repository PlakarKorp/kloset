package exclude

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type RuleTestCase struct {
	Pattern string
	Path    string
}

func (tc RuleTestCase) RunTest() error {
	path, isDir := strings.CutPrefix(tc.Path, "/")
	comps := strings.Split(path, "/")

	// Check what git does
	gitrule, _ := ParseGitRule(tc.Pattern)
	expectMatched, expectExcluded, err := gitrule.Match(comps, isDir)
	if err != nil {
		return err
	}

	rule, err := ParseRule(tc.Pattern)
	if err != nil {
		return err
	}
	matched, excluded, err := rule.Match(comps, isDir)
	if err != nil {
		return err
	}

	if expectMatched != matched {
		return fmt.Errorf("expect matched %v, got %v", expectMatched, matched)
	}
	if expectExcluded != excluded {
		return fmt.Errorf("expect excluded %v, got %v", expectExcluded, excluded)
	}

	return nil
}

var testCases = []RuleTestCase{
	// The pattern doc/frotz and /doc/frotz have the same
	// effect in any .gitignore file. In other words, a
	// leading slash is not relevant if there is already a
	// middle slash in the pattern.

	// For example, a pattern doc/frotz/ matches doc/frotz
	// directory, but not a/doc/frotz directory; however
	// frotz/ matches frotz and a/frotz that is a
	// directory (all paths are relative from the
	// .gitignore file).
	{"doc/frotz/", "doc/frotz/"},
	{"doc/frotz/", "a/doc/frotz/"},
	{"frotz/", "frotz/"},
	{"frotz/", "a/doc/frotz/"},

	// The pattern hello.* matches any file or directory
	// whose name begins with hello.. If one wants to
	// restrict this only to the directory and not in its
	// subdirectories, one can prepend the pattern with a
	// slash, i.e. /hello.*; the pattern now matches
	// hello.txt, hello.c but not a/hello.java.

	{"hello.*", "hello.c"},
	{"hello.*", "hello.c/"},
	{"hello.*", "hello.c/foo"},
	{"hello.*", "src/hello.c"},
	{"hello.*", "src/hello.c/"},
	{"hello.*", "src/hello.c/foo"},

	{"/hello.*", "hello.c"},
	{"/hello.*", "hello.c/"},
	{"/hello.*", "hello.c/foo"},
	{"/hello.*", "src/hello.c"},
	{"/hello.*", "src/hello.c/"},
	{"/hello.*", "src/hello.c/foo"},

	// The pattern foo/ will match a directory foo and
	// paths underneath it, but will not match a regular
	// file or a symbolic link foo (this is consistent
	// with the way how pathspec works in general in Git)
	{"foo/", "foo/"},
	{"foo/", "bar/foo/"},
	{"foo/", "foo"},
	{"foo/", "bar/foo"},

	// The pattern foo/*, matches foo/test.json (a regular
	// file), foo/bar (a directory), but it does not match
	// foo/bar/hello.c (a regular file), as the asterisk
	// in the pattern does not match bar/hello.c which has
	// a slash in it.
	{"foo/*", "foo/test.json"},
	{"foo/*", "foo/bar/"},
	{"foo/*", "foo/bar/hello.c"},

	// Two consecutive asterisks ("**") in patterns
	// matched against full pathname may have special
	// meaning:

	// A leading "**" followed by a slash means match in
	// all directories. For example, "**/foo" matches file
	// or directory "foo" anywhere, the same as pattern
	// "foo". "**/foo/bar" matches file or directory "bar"
	// anywhere that is directly under directory "foo".
	{"**/foo", "foo"},
	{"**/foo", "foo/"},
	{"**/foo", "foo/bar"},
	{"**/foo", "foo/bar/"},
	{"**/foo", "baz/foo"},
	{"**/foo", "baz/foo/"},
	{"**/foo", "baz"},
	{"**/foo", "baz/"},
	{"**/foo", "baz/foo"},
	{"**/foo", "baz/foo/bar"},
	{"**/foo", "baz/foo/bar/"},
	{"**/foo", "baz/foo/bar/hello.c"},

	{"**/foo/bar", "foo"},
	{"**/foo/bar", "foo/"},
	{"**/foo/bar", "foo/bar"},
	{"**/foo/bar", "foo/bar/"},
	{"**/foo/bar", "foo/bar/baz"},
	{"**/foo/bar", "baz/foo"},
	{"**/foo/bar", "baz/foo/bar"},
	{"**/foo/bar", "baz/foo/bar/"},
	{"**/foo/bar", "baz/foo/bar/hello.c"},

	// A trailing "/**" matches everything inside. For
	// example, "abc/**" matches all files inside
	// directory "abc", relative to the location of the
	// .gitignore file, with infinite depth.
	//{"abc/**", "abc"},
	{"abc/**", "abc/foo"},
	{"abc/**", "abc/foo/"},
	{"abc/**", "abc/foo/bar"},

	{"abc/**", "d/abc"},
	{"abc/**", "d/abc/foo"},
	{"abc/**", "d/abc/foo/"},
	{"abc/**", "d/abc/foo/bar"},

	// A slash followed by two consecutive asterisks then
	// a slash matches zero or more directories. For
	// example, "a/**/b" matches "a/b", "a/x/b", "a/x/y/b"
	// and so on.

	{"a/**/b", "a/b"},
	{"a/**/b", "a/b/"},
	{"a/**/b", "a/x/b"},
	{"a/**/b", "a/x/b/"},
	{"a/**/b", "a/x/y/b"},
	{"a/**/b", "a/x/y/b/"},
	{"a/**/b", "a/b/c"},

	{"a/**/b", "d/a/b"},
	{"a/**/b", "d/a/b/"},
	{"a/**/b", "d/a/x/b"},
	{"a/**/b", "d/a/x/b/"},
	{"a/**/b", "d/a/x/y/b"},
	{"a/**/b", "d/a/x/y/b/"},

	{"**/a/**/b", "d/a/b"},
	{"**/a/**/b", "d/a/b/"},
	{"**/a/**/b", "d/a/x/b"},
	{"**/a/**/b", "d/a/x/b/"},
	{"**/a/**/b", "d/a/x/y/b"},
	{"**/a/**/b", "d/a/x/y/b/"},
}

func TestMatchRule(t *testing.T) {
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			if err := tc.RunTest(); err != nil {
				t.Error(fmt.Sprintf("pattern=%q, path=%q", tc.Pattern, tc.Path), err)
			}
		})
	}
}

type GitRule struct {
	Raw      string // original pattern
	Negate   bool   // '!' at start
	DirOnly  bool   // trailing '/'
	Anchored bool   // contains non-trailing '/'
	Path     []string
}

func ParseGitRule(pattern string) (*GitRule, error) {
	rule := &GitRule{}
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
	return rule, nil
}

func (rule *GitRule) Match(path []string, isDir bool) (bool, bool, error) {
	tmp, err := os.MkdirTemp("/tmp", "git.*")
	if err != nil {
		return false, false, err
	}
	defer os.RemoveAll(tmp)
	_, err = exec.Command("git", "-C", tmp, "init").Output()
	if err != nil {
		return false, false, err
	}
	fd, err := os.Create(tmp + "/.gitignore")
	if err != nil {
		return false, false, err
	}
	defer fd.Close()

	// Always write a ignore pattern, so that we can detect if it matches
	pattern := rule.Raw
	if rule.Negate {
		pattern = pattern[1:]
	}
	_, err = fd.WriteString(pattern + "\n")
	if err != nil {
		return false, false, err
	}
	fd.Close()

	strpath := strings.Join(path, "/")
	if isDir {
		strpath = strpath + "/"
	}
	out, err := exec.Command("git", "-C", tmp, "check-ignore", strpath).Output()
	if err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			if exiterr.ExitCode() == 1 {
				return false, false, nil // no match
			}
		}
		return false, false, err
	}
	res := strings.TrimSuffix(string(out), "\n")
	if res == strpath {
		return true, !rule.Negate, nil
	}

	return false, false, fmt.Errorf("unexpected output %q", res)
}
