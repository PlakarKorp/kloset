package exclude

import (
	"fmt"
	"strings"
	"testing"
)

type RuleTestCase struct {
	Pattern  string
	Path     string
	Expected bool
}

func (tc RuleTestCase) Display() string {
	return fmt.Sprintf("pattern=%q path=%q", tc.Pattern, tc.Path)
}

func (tc RuleTestCase) RunTest(useRegex bool) error {
	var isDir bool

	path := tc.Path
	if strings.HasSuffix(tc.Path, "/") {
		isDir = true
		path = strings.TrimSuffix(tc.Path, "/")
	}

	rule := MustParseRule(tc.Pattern)
	if rule.DirOnly && !isDir {
		if !tc.Expected {
			return nil
		}
		return fmt.Errorf("directory rule can't match non-directory")
	}

	matched := rule.Match(path, useRegex)
	if matched != tc.Expected {
		return fmt.Errorf("expect %v, got %v", tc.Expected, matched)
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
	{"doc/frotz/", "doc/frotz/", true},
	{"doc/frotz/", "a/doc/frotz/", false},
	{"frotz/", "frotz/", true},
	//{"frotz/", "a/doc/frotz/", true},

	// The pattern hello.* matches any file or directory
	// whose name begins with hello.. If one wants to
	// restrict this only to the directory and not in its
	// subdirectories, one can prepend the pattern with a
	// slash, i.e. /hello.*; the pattern now matches
	// hello.txt, hello.c but not a/hello.java.

	{"hello.*", "hello.c", true},
	{"hello.*", "hello.c/", true},
	{"hello.*", "hello.c/foo", false},
	//{"hello.*", "src/hello.c", true},
	//{"hello.*", "src/hello.c/", true},
	{"hello.*", "src/hello.c/foo", false},

	{"/hello.*", "hello.c", true},
	{"/hello.*", "hello.c/", true},
	{"/hello.*", "hello.c/foo", false},
	{"/hello.*", "src/hello.c", false},
	{"/hello.*", "src/hello.c/", false},
	{"/hello.*", "src/hello.c/foo", false},

	// The pattern foo/ will match a directory foo and
	// paths underneath it, but will not match a regular
	// file or a symbolic link foo (this is consistent
	// with the way how pathspec works in general in Git)
	{"foo/", "foo/", true},
	//{"foo/", "bar/foo/", true},
	{"foo/", "foo", false},
	{"foo/", "bar/foo", false},

	// The pattern foo/*, matches foo/test.json (a regular
	// file), foo/bar (a directory), but it does not match
	// foo/bar/hello.c (a regular file), as the asterisk
	// in the pattern does not match bar/hello.c which has
	// a slash in it.
	{"foo/*", "foo/test.json", true},
	{"foo/*", "foo/bar/", true},
	{"foo/*", "foo/bar/hello.c", false},

	// Two consecutive asterisks ("**") in patterns
	// matched against full pathname may have special
	// meaning:

	// A leading "**" followed by a slash means match in
	// all directories. For example, "**/foo" matches file
	// or directory "foo" anywhere, the same as pattern
	// "foo". "**/foo/bar" matches file or directory "bar"
	// anywhere that is directly under directory "foo".
	{"**/foo", "foo", true},
	{"**/foo", "foo/", true},
	{"**/foo", "foo/bar", false},
	{"**/foo", "foo/bar/", false},
	{"**/foo", "baz/foo", true},
	{"**/foo", "baz/foo/", true},
	{"**/foo", "baz", false},
	{"**/foo", "baz/", false},
	{"**/foo", "baz/foo", true},
	{"**/foo", "baz/foo/bar", false},
	{"**/foo", "baz/foo/bar/", false},
	{"**/foo", "baz/foo/bar/hello.c", false},

	{"**/foo/bar", "foo", false},
	{"**/foo/bar", "foo/", false},
	{"**/foo/bar", "foo/bar", true},
	{"**/foo/bar", "foo/bar/", true},
	{"**/foo/bar", "foo/bar/baz", false},
	{"**/foo/bar", "baz/foo", false},
	{"**/foo/bar", "baz/foo/bar", true},
	{"**/foo/bar", "baz/foo/bar/", true},
	{"**/foo/bar", "baz/foo/bar/hello.c", false},

	// A trailing "/**" matches everything inside. For
	// example, "abc/**" matches all files inside
	// directory "abc", relative to the location of the
	// .gitignore file, with infinite depth.
	//{"abc/**", "abc", false},
	{"abc/**", "abc/foo", true},
	{"abc/**", "abc/foo/", true},
	{"abc/**", "abc/foo/bar", true},

	{"abc/**", "d/abc", false},
	{"abc/**", "d/abc/foo", false},
	{"abc/**", "d/abc/foo/", false},
	{"abc/**", "d/abc/foo/bar", false},

	// A slash followed by two consecutive asterisks then
	// a slash matches zero or more directories. For
	// example, "a/**/b" matches "a/b", "a/x/b", "a/x/y/b"
	// and so on.

	{"a/**/b", "a/b", true},
	{"a/**/b", "a/b/", true},
	{"a/**/b", "a/x/b", true},
	{"a/**/b", "a/x/b/", true},
	{"a/**/b", "a/x/y/b", true},
	{"a/**/b", "a/x/y/b/", true},
	{"a/**/b", "a/b/c", false},

	{"a/**/b", "d/a/b", false},
	{"a/**/b", "d/a/b/", false},
	{"a/**/b", "d/a/x/b", false},
	{"a/**/b", "d/a/x/b/", false},
	{"a/**/b", "d/a/x/y/b", false},
	{"a/**/b", "d/a/x/y/b/", false},

	{"**/a/**/b", "d/a/b", true},
	{"**/a/**/b", "d/a/b/", true},
	{"**/a/**/b", "d/a/x/b", true},
	{"**/a/**/b", "d/a/x/b/", true},
	{"**/a/**/b", "d/a/x/y/b", true},
	{"**/a/**/b", "d/a/x/y/b/", true},
}

func TestRules(t *testing.T) {
	for idx, tc := range testCases {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			err := tc.RunTest(false)
			if err != nil {
				t.Error(tc.Display(), err)
			}
		})
	}

}
