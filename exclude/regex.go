package exclude

import (
	"strings"
)

// converts a single .gitignore pattern (without leading '!' or trailing '/') into a Go regexp string.
// Key mappings:
//
//	**  -> matches across path separators
//	 *  -> matches any run except '/'
//	 ?  -> matches any single char except '/'
//
// A pattern without '/' is matched against any path segment anywhere.
func toRegex(pattern string, anchored bool) string {
	// Unescape escaped spaces and escaped special chars (\, space, #, !)
	// Keep backslashes that escape regex specials handled later.
	pattern = unescapeGitignoreEscapes(pattern)

	// Escape regex metacharacters first
	var b strings.Builder
	for _, r := range pattern {
		switch r {
		case '.', '+', '(', ')', '^', '$', '{', '}', '|':
			b.WriteRune('\\')
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	s := b.String()

	// Replace '**' tokens with a placeholder to avoid double-processing '*'
	// Use a rune unlikely to appear (unit separator).
	const dbl = "\x1F"
	s = strings.ReplaceAll(s, "**", dbl)

	// Now replace single-star and question
	s = strings.ReplaceAll(s, "*", "[^/]*")
	s = strings.ReplaceAll(s, "?", "[^/]")

	// Replace '**' placeholder allowing slashes
	s = strings.ReplaceAll(s, dbl, ".*")

	// At this point, s contains a regex for within a path.
	// If pattern was anchored, match from start; otherwise allow anywhere starting at a segment boundary.
	if anchored {
		return "^" + s + "$"
	}

	// If the pattern contains no '/', it should match a single path segment anywhere.
	if !strings.Contains(s, "/") {
		// (^|.*/)<segment>(/|$) — matches any segment equal to the pattern
		return "^(?:|.*/)" + s + "(?:/.*|$)"
	}

	// Otherwise, allow match at any position starting at a directory boundary.
	return "^(?:|.*/)" + s + "$"
}

func unescapeGitignoreEscapes(s string) string {
	// In .gitignore, backslash can escape space, '#', '!', and '\'.
	// We'll convert "\ " -> " ", "\#" -> "#", "\!" -> "!", "\\ "-> "\ ".
	// For other sequences, keep the backslash: e.g., "\*" should be treated as literal '*' so we preserve it
	// until after we swap globs; handle that here explicitly.
	var b strings.Builder
	esc := false
	for _, r := range s {
		if !esc {
			if r == '\\' {
				esc = true
			} else {
				b.WriteRune(r)
			}
			continue
		}
		// we are escaping this rune
		switch r {
		case ' ', '#', '!', '\\':
			b.WriteRune(r)
		//case '*', '?', '/':
		//	// escape means literal for glob specials -> write with extra backslash so later replacements don't treat it as glob
		//	b.WriteRune('\\')
		//	b.WriteRune(r)
		default:
			// unknown escape, keep the backslash and char
			b.WriteRune('\\')
			b.WriteRune(r)
		}
		esc = false
	}
	if esc {
		// trailing backslash -> keep it
		b.WriteRune('\\')
	}
	return b.String()
}
