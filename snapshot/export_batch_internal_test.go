package snapshot

import (
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

func SetExportBatchLimits(t testing.TB, files, bytes, smallFileBytes int) {
	oldFiles, oldBytes, oldSmall := exportBatchFiles, exportBatchBytes, exportSmallFileBytes
	exportBatchFiles, exportBatchBytes, exportSmallFileBytes = files, bytes, smallFileBytes
	t.Cleanup(func() {
		exportBatchFiles, exportBatchBytes, exportSmallFileBytes = oldFiles, oldBytes, oldSmall
	})
}

func FetchExportFiles(snap *Snapshot, entries []*vfs.Entry) ([][]byte, []error) {
	b := newExportBatch()
	for _, e := range entries {
		b.files = append(b.files, &exportFile{entry: e})
	}
	snap.fetchBatch(b)

	pvfs, err := snap.Filesystem()
	if err != nil {
		panic(err)
	}
	data := make([][]byte, len(entries))
	errs := make([]error, len(entries))
	for i, f := range b.files {
		rd, err := f.open(pvfs)
		if err != nil {
			errs[i] = err
			continue
		}
		data[i], errs[i] = io.ReadAll(rd)
	}
	return data, errs
}
