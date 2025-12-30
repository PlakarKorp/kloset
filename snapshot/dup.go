package snapshot

func (snap *Snapshot) Dup(builderOptions *BackupOptions) (*Snapshot, error) {
	newSnapshot, err := snap.Fork(builderOptions)
	if err != nil {
		return nil, err
	}
	if err := newSnapshot.Commit(&BackupContext{}, true); err != nil {
		return nil, err
	}

	return Load(snap.repository, newSnapshot.Header.GetIndexID())
}
