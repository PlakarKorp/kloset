package snapshot

func (snap *Snapshot) Dup() (*Snapshot, error) {
	newSnapshot, err := snap.Fork(&BuilderOptions{
		NoCommit: false,
	})
	if err != nil {
		return nil, err
	}
	if err := newSnapshot.Commit(&BuilderContext{}); err != nil {
		return nil, err
	}

	return Load(snap.repository, newSnapshot.Header.GetIndexID())
}
