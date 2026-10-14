package snapshot_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/analyzer"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// keywords tags text files containing "secret" and counts them per snapshot.
type keywords struct {
	mu        sync.Mutex
	count     uint64
	processed atomic.Int64
}

func (k *keywords) Name() string    { return "keywords" }
func (k *keywords) Version() string { return "0.0.1" }
func (k *keywords) Scope() analyzer.Scope {
	return analyzer.ScopeFile | analyzer.ScopeSnapshot
}
func (k *keywords) Filter() analyzer.Filter {
	return analyzer.Filter{MIMETypes: []string{"text/*"}, Content: analyzer.ContentFull}
}
func (k *keywords) Close(ctx context.Context) error { return nil }

func (k *keywords) NewProcessor(ctx context.Context, file analyzer.File) (analyzer.Processor, error) {
	k.processed.Add(1)
	return &keywordsProc{}, nil
}

func (k *keywords) Observe(ctx context.Context, pathname string, classes []analyzer.Class) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.count++
	return nil
}

func (k *keywords) Finalize(ctx context.Context) ([]analyzer.Class, error) {
	return []analyzer.Class{{Name: "secret.word", Probability: 1, Count: k.count}}, nil
}

type keywordsProc struct {
	buf bytes.Buffer
}

func (p *keywordsProc) Write(b []byte) (bool, error) {
	p.buf.Write(b)
	return true, nil
}

func (p *keywordsProc) Close() ([]analyzer.Class, error) {
	if strings.Contains(p.buf.String(), "secret") {
		return []analyzer.Class{{Name: "secret.word", Probability: 1}}, nil
	}
	return nil, nil
}

func analysisTree(ch chan<- *connectors.Record) {
	file := func(pathname, content string) {
		name := pathname[strings.LastIndex(pathname, "/")+1:]
		ch <- connectors.NewRecord(pathname, "", objects.FileInfo{
			Lname: name, Lmode: 0644, Lsize: int64(len(content)),
		}, nil, func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(content)), nil
		})
	}
	ch <- connectors.NewRecord("/", "", objects.FileInfo{Lname: "/", Lmode: os.ModeDir | 0755}, nil, nil)
	ch <- connectors.NewRecord("/d", "", objects.FileInfo{Lname: "d", Lmode: os.ModeDir | 0755}, nil, nil)
	file("/d/leak.txt", "the secret is here")
	file("/d/plain.txt", "nothing to see")
	file("/d/empty.txt", "")
}

func backupAnalyzed(t *testing.T, repo *repository.Repository, vfsCache *vfs.Filesystem, kw *keywords) *snapshot.Snapshot {
	t.Helper()

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:      "analyzed",
		Analyzers: []analyzer.Analyzer{kw},
	})
	require.NoError(t, err)
	if vfsCache != nil {
		builder.WithVFSCache(vfsCache)
	}

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{}, "mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).SetGenerator(analysisTree)

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)
	require.NoError(t, src.SetExcludes(nil))

	require.NoError(t, builder.Backup(src))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, builder.Repository().RebuildState())

	snap, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)

	checkCache, err := repo.AppContext().GetCache().Check()
	require.NoError(t, err)
	snap.SetCheckCache(checkCache)
	return snap
}

func requireAnalyzed(t *testing.T, snap *snapshot.Snapshot) {
	t.Helper()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	leak, err := fs.GetEntry("/d/leak.txt")
	require.NoError(t, err)
	require.Equal(t, []vfs.Classification{{
		Analyzer: "keywords",
		Classes:  []string{"secret.word"},
		Scores:   []vfs.Score{{Key: "secret.word", Value: 1}},
	}}, leak.Classifications)

	plain, err := fs.GetEntry("/d/plain.txt")
	require.NoError(t, err)
	require.Empty(t, plain.Classifications)

	var found []string
	for c, err := range snap.ListClassified("keywords", "secret") {
		require.NoError(t, err)
		require.Equal(t, "secret.word", c.Class)
		found = append(found, c.Path)

		entry, err := fs.ResolveEntry(c.MAC)
		require.NoError(t, err)
		require.Equal(t, c.Path, entry.Path())
	}
	require.Equal(t, []string{"/d/leak.txt"}, found)

	// a class prefix must stop at a dot boundary
	for range snap.ListClassified("keywords", "sec") {
		t.Fatal("\"sec\" must not match \"secret.word\"")
	}

	require.Equal(t, []header.Classification{{
		Analyzer: "keywords",
		Classes:  []header.Class{{Name: "secret.word", Probability: 1, Count: 1}},
	}}, snap.Header.Classifications)
}

func TestBackupAnalysis(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	kw := &keywords{}
	first := backupAnalyzed(t, repo, nil, kw)
	defer first.Close()
	requireAnalyzed(t, first)
	require.EqualValues(t, 3, kw.processed.Load())

	// unchanged files come from the cache: their classifications are carried
	// forward, indexed again, and still counted at the snapshot level
	firstFS, err := first.FilesystemWithCache()
	require.NoError(t, err)

	kw = &keywords{}
	second := backupAnalyzed(t, repo, firstFS, kw)
	defer second.Close()
	requireAnalyzed(t, second)
	require.Zero(t, kw.processed.Load(), "cached files must not be re-analyzed")
	require.NoError(t, second.Check("/", &snapshot.CheckOptions{}))

	// maintenance must know about the index, or it would refuse the snapshot
	packfiles, err := second.ListPackfiles()
	require.NoError(t, err)
	for _, err := range packfiles {
		require.NoError(t, err)
	}
	_, found := second.ClassificationIdxRoot()
	require.True(t, found)
}

func TestBackupWithoutClassificationsHasNoIndex(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	snap := backupOnce(t, repo, "plain", nil, analysisTree)
	defer snap.Close()

	// keeps snapshots readable by versions that predate the index
	_, found := snap.ClassificationIdxRoot()
	require.False(t, found)
	require.Empty(t, snap.Header.Classifications)
}

func TestCachedResultsOfDisabledAnalyzersAreDropped(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	first := backupAnalyzed(t, repo, nil, &keywords{})
	defer first.Close()
	requireAnalyzed(t, first)

	firstFS, err := first.FilesystemWithCache()
	require.NoError(t, err)

	// same tree, nothing changed, but the analyzer is no longer enabled
	second := backupOnce(t, repo, "not-analyzed", firstFS, analysisTree)
	defer second.Close()

	fs, err := second.Filesystem()
	require.NoError(t, err)
	leak, err := fs.GetEntry("/d/leak.txt")
	require.NoError(t, err)
	require.Empty(t, leak.Classifications)

	_, found := second.ClassificationIdxRoot()
	require.False(t, found)
	require.Empty(t, second.Header.Classifications)
	require.NoError(t, second.Check("/", &snapshot.CheckOptions{}))
}

// requireSameClassifications compares every entry of two snapshots.
func requireSameClassifications(t *testing.T, src, dst *snapshot.Snapshot) {
	t.Helper()

	srcFS, err := src.Filesystem()
	require.NoError(t, err)
	dstFS, err := dst.Filesystem()
	require.NoError(t, err)

	for p, err := range srcFS.Pathnames() {
		require.NoError(t, err)
		se, err := srcFS.GetEntryNoFollow(p)
		require.NoError(t, err)
		de, err := dstFS.GetEntryNoFollow(p)
		require.NoError(t, err)
		require.Equal(t, se.Classifications, de.Classifications, p)
	}

	var srcIdx, dstIdx []snapshot.Classified
	for c, err := range src.ListClassified("keywords", "") {
		require.NoError(t, err)
		c.MAC = objects.MAC{}
		srcIdx = append(srcIdx, c)
	}
	for c, err := range dst.ListClassified("keywords", "") {
		require.NoError(t, err)
		c.MAC = objects.MAC{}
		dstIdx = append(dstIdx, c)
	}
	require.NotEmpty(t, srcIdx)
	require.Equal(t, srcIdx, dstIdx)

	require.Equal(t, src.Header.Classifications, dst.Header.Classifications)
}

func synchronizeTo(t *testing.T, src *snapshot.Snapshot, dstRepo *repository.Repository, vfsCache *vfs.Filesystem) *snapshot.Snapshot {
	t.Helper()

	dstBuilder, err := snapshot.Create(dstRepo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{Name: "sync"})
	require.NoError(t, err)
	if vfsCache != nil {
		dstBuilder.WithVFSCache(vfsCache)
	}

	require.NoError(t, src.Synchronize(dstBuilder))
	require.NoError(t, dstBuilder.Close())
	require.NoError(t, dstBuilder.Repository().RebuildState())

	synced, err := snapshot.Load(dstRepo, dstBuilder.Header.Identifier)
	require.NoError(t, err)

	checkCache, err := dstRepo.AppContext().GetCache().Check()
	require.NoError(t, err)
	synced.SetCheckCache(checkCache)
	return synced
}

func TestSynchronizeKeepsClassifications(t *testing.T) {
	srcRepo := ptesting.GenerateRepository(t, nil, nil, nil)
	dstRepo := ptesting.GenerateRepository(t, nil, nil, nil)

	src := backupAnalyzed(t, srcRepo, nil, &keywords{})
	defer src.Close()

	synced := synchronizeTo(t, src, dstRepo, nil)
	defer synced.Close()
	requireSameClassifications(t, src, synced)
	require.NoError(t, synced.Check("/", &snapshot.CheckOptions{}))

	// an incremental sync goes through the destination's VFS cache, which
	// must neither drop nor alter what is copied
	syncedFS, err := synced.FilesystemWithCache()
	require.NoError(t, err)

	again := synchronizeTo(t, src, dstRepo, syncedFS)
	defer again.Close()
	requireSameClassifications(t, src, again)
	require.NoError(t, again.Check("/", &snapshot.CheckOptions{}))
}

func TestSynchronizeRefusesAnalyzers(t *testing.T) {
	srcRepo := ptesting.GenerateRepository(t, nil, nil, nil)
	dstRepo := ptesting.GenerateRepository(t, nil, nil, nil)

	src := backupAnalyzed(t, srcRepo, nil, &keywords{})
	defer src.Close()

	dstBuilder, err := snapshot.Create(dstRepo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:      "sync",
		Analyzers: []analyzer.Analyzer{&keywords{}},
	})
	require.NoError(t, err)
	defer dstBuilder.Close()

	require.Error(t, src.Synchronize(dstBuilder))
}
