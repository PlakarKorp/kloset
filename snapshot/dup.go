package snapshot

func (snap *Snapshot) Dup() (*Snapshot, error) {
	newSnapshot, err := snap.Fork()
	if err != nil {
		return nil, err
	}
	if err := newSnapshot.Commit(&BackupContext{}, true); err != nil {
		return nil, err
	}

	return Load(snap.repository, newSnapshot.Header.GetIndexID())
}
