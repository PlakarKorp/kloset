package snapshot

// SetSourceRoot overrides the root computed by NewSource, so tests can
// rebuild the layout left by builders that did not clean it.
func SetSourceRoot(s *Source, root string) {
	s.root = root
}
