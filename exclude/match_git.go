package exclude

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func RuleMatchGit(rule *Rule, path []string, isDir bool) (bool, bool, error) {
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
