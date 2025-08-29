package exclude

import (
	"bufio"
	"io"
	"iter"
	"os"
	"path/filepath"
	"strings"
)

// Preprocess a gitgnore pattern line:
func preprocessLine(line string) string {
	// Handle CRLF safely
	line = strings.TrimRight(line, "\r")

	line = trimTrailingSpaces(line)

	if line == "" {
		return ""
	}

	// Comment lines start with unescaped '#'
	if strings.HasPrefix(line, "#") {
		return ""
	}
	// If it starts with '\#', drop the backslash to treat '#' literally.
	if strings.HasPrefix(line, `\#`) {
		line = line[1:]
	}
	return line
}

// Trim trailing spaces unless the last one is escaped
// "foo bar   "   ->  "foo bar"
// "foo bar \ "   ->  "foo bar  "
// "foo bar \  "  ->  "foo bar  "
func trimTrailingSpaces(s string) string {
	// Count trailing spaces
	i := len(s)
	for i > 0 && s[i-1] == ' ' {
		i--
	}
	if i == len(s) {
		return s // no trailing spaces
	}
	// There were trailing spaces.
	// If the char before the last trailing space is a backslash,
	// keep that space and drop the backslash.
	if i > 0 && s[i-1] == '\\' {
		return s[:i-1] + " "
	}
	// Otherwise, drop them.
	return s[:i]
}

// Convert path to '/' and remove the trailing one if any
func toSlashNoTrail(p string) string {
	if p == "" {
		return ""
	}
	p = filepath.Clean(p)
	p = filepath.ToSlash(p)
	if p != "/" {
		p = strings.TrimRight(p, "/")
	}
	return p
}

func relativeTo(path string, root string) string {
	path = toSlashNoTrail(path)

	// If it's exactly root, rel is empty
	if path == root {
		return ""
	}

	// If absolute or starts with root, make relative to root
	if strings.HasPrefix(path, root+"/") {
		return strings.TrimPrefix(path, root+"/")
	}

	// If absolute but different root, best effort: strip drive letters and leading slashes
	if filepath.IsAbs(path) {
		return strings.TrimLeft(path, "/")
	}

	return path
}

func iterArray(a []string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for _, line := range a {
			if !yield(line, nil) {
				return
			}
		}
	}
}

func iterReader(rd io.Reader) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		sc := bufio.NewScanner(rd)
		for sc.Scan() {
			line := sc.Text()
			line = strings.TrimSuffix(line, "\r")
			if !yield(line, nil) {
				return
			}
		}
		if err := sc.Err(); err != nil {
			yield("", err)
		}
	}
}

func iterFile(filename string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		file, err := os.Open(filename)
		if err != nil {
			yield("", err)
			return
		}
		defer file.Close()
		for line, err := range iterReader(file) {
			if !yield(line, err) {
				return
			}
		}
	}
}
