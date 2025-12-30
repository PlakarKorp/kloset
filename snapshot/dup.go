package snapshot

func (snap *Snapshot) Dup(builderOptions *BuilderOptions) (*Snapshot, error) {
	newSnapshot, err := snap.Fork(builderOptions)
	if err != nil {
		return nil, err
	}
	if err := newSnapshot.Commit(true); err != nil {
		return nil, err
	}

	return Load(snap.repository, newSnapshot.Header.GetIndexID())
}
