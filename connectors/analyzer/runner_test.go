package analyzer_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/analyzer"
	"github.com/stretchr/testify/require"
)

// fake records what it is given and returns canned classes.
type fake struct {
	name    string
	scope   analyzer.Scope
	filter  analyzer.Filter
	classes []analyzer.Class

	// stopAfter makes Write ask for no more content once that many bytes
	// were seen, 0 never stops.
	stopAfter  int
	writeErr   error
	panicWrite bool

	seen     map[string][]byte
	writes   int
	observed map[string][]analyzer.Class
	final    []analyzer.Class
	closed   bool
}

func (f *fake) Name() string                    { return f.name }
func (f *fake) Version() string                 { return "0.0.1" }
func (f *fake) Scope() analyzer.Scope           { return f.scope }
func (f *fake) Filter() analyzer.Filter         { return f.filter }
func (f *fake) Close(ctx context.Context) error { f.closed = true; return nil }

func (f *fake) NewProcessor(ctx context.Context, file analyzer.File) (analyzer.Processor, error) {
	return &fakeProc{f: f, file: file}, nil
}

func (f *fake) Observe(ctx context.Context, pathname string, classes []analyzer.Class) error {
	if f.observed == nil {
		f.observed = make(map[string][]analyzer.Class)
	}
	f.observed[pathname] = classes
	return nil
}

func (f *fake) Finalize(ctx context.Context) ([]analyzer.Class, error) {
	return f.final, nil
}

type fakeProc struct {
	f    *fake
	file analyzer.File
	buf  bytes.Buffer
}

func (p *fakeProc) Write(b []byte) (bool, error) {
	if p.f.panicWrite {
		panic("boom")
	}
	if p.f.writeErr != nil {
		return false, p.f.writeErr
	}
	p.f.writes++
	p.buf.Write(b)
	return p.f.stopAfter == 0 || p.buf.Len() < p.f.stopAfter, nil
}

func (p *fakeProc) Close() ([]analyzer.Class, error) {
	if p.f.seen == nil {
		p.f.seen = make(map[string][]byte)
	}
	p.f.seen[p.file.Path] = p.buf.Bytes()
	return p.f.classes, nil
}

func runFile(t *testing.T, r *analyzer.Runner, pathname, ctype string, xattrs []string, chunks ...string) []analyzer.Result {
	t.Helper()
	run := r.Start(context.Background(), pathname, 0, xattrs)
	if run == nil {
		return nil
	}
	for _, c := range chunks {
		run.Begin(ctype)
		run.Write([]byte(c))
	}
	return run.Finish(ctype)
}

func TestFilters(t *testing.T) {
	byExt := &fake{name: "ext", scope: analyzer.ScopeFile,
		filter:  analyzer.Filter{Extensions: []string{"JPG", ".png"}},
		classes: []analyzer.Class{{Name: "image", Probability: 1}}}
	byMime := &fake{name: "mime", scope: analyzer.ScopeFile,
		filter:  analyzer.Filter{MIMETypes: []string{"text/*"}},
		classes: []analyzer.Class{{Name: "text", Probability: 1}}}
	byXattr := &fake{name: "xattr", scope: analyzer.ScopeFile,
		filter:  analyzer.Filter{Xattrs: []string{"user.tag"}},
		classes: []analyzer.Class{{Name: "tagged", Probability: 1}}}

	r, err := analyzer.NewRunner(nil, []analyzer.Analyzer{byExt, byMime, byXattr})
	require.NoError(t, err)

	res := runFile(t, r, "/a/photo.jpg", "image/jpeg", nil, "x")
	require.Equal(t, []analyzer.Result{{Analyzer: "ext", Classes: byExt.classes}}, res)

	res = runFile(t, r, "/a/notes", "text/plain; charset=utf-8", nil, "x")
	require.Equal(t, []analyzer.Result{{Analyzer: "mime", Classes: byMime.classes}}, res)

	res = runFile(t, r, "/a/blob", "application/octet-stream", []string{"user.other", "user.tag"}, "x")
	require.Equal(t, []analyzer.Result{{Analyzer: "xattr", Classes: byXattr.classes}}, res)

	// first stage rejects before any content is read
	require.Nil(t, r.Start(context.Background(), "/a/blob", 0, nil).Finish("image/png"))
}

func TestContentModes(t *testing.T) {
	none := &fake{name: "none", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentNone}}
	head := &fake{name: "head", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentHead, HeadSize: 5}}
	full := &fake{name: "full", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentFull}}
	early := &fake{name: "early", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentFull}, stopAfter: 3}

	r, err := analyzer.NewRunner(nil, []analyzer.Analyzer{none, head, full, early})
	require.NoError(t, err)

	runFile(t, r, "/f", "text/plain", nil, "abc", "def", "ghi")

	require.Empty(t, none.seen["/f"])
	require.Equal(t, "abcde", string(head.seen["/f"]))
	require.Equal(t, "abcdefghi", string(full.seen["/f"]))
	require.Equal(t, "abc", string(early.seen["/f"]))
	require.Equal(t, 1, early.writes)
}

func TestFailuresAreIsolated(t *testing.T) {
	failing := &fake{name: "failing", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentFull}, writeErr: errors.New("nope")}
	panicking := &fake{name: "panicking", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentFull}, panicWrite: true}
	healthy := &fake{name: "healthy", scope: analyzer.ScopeFile,
		classes: []analyzer.Class{{Name: "ok", Probability: 1}}}

	r, err := analyzer.NewRunner(nil, []analyzer.Analyzer{failing, panicking, healthy})
	require.NoError(t, err)

	res := runFile(t, r, "/f", "text/plain", nil, "abc")
	require.Equal(t, []analyzer.Result{
		{Analyzer: "failing", Classes: []analyzer.Class{{Name: analyzer.ClassError}}},
		{Analyzer: "panicking", Classes: []analyzer.Class{{Name: analyzer.ClassError}}},
		{Analyzer: "healthy", Classes: healthy.classes},
	}, res)
}

func TestClassesAreSanitized(t *testing.T) {
	leaky := &fake{name: "leaky", scope: analyzer.ScopeFile, classes: []analyzer.Class{
		{Name: "pii.email", Probability: 0.9},
		{Name: "pii.email", Probability: 0.5},
		{Name: "john@example.com", Probability: 1},
		{Name: "_error", Probability: 1},
		{Name: "secret.Key", Probability: 1},
		{Name: "secret.key", Probability: 2},
	}}

	r, err := analyzer.NewRunner(nil, []analyzer.Analyzer{leaky})
	require.NoError(t, err)

	res := runFile(t, r, "/f", "text/plain", nil, "abc")
	require.Equal(t, []analyzer.Result{{Analyzer: "leaky", Classes: []analyzer.Class{
		{Name: "pii.email", Probability: 0.9},
	}}}, res)
}

func TestScopes(t *testing.T) {
	fileOnly := &fake{name: "file-only", scope: analyzer.ScopeFile,
		classes: []analyzer.Class{{Name: "a", Probability: 1}}}
	snapOnly := &fake{name: "snap-only", scope: analyzer.ScopeSnapshot,
		classes: []analyzer.Class{{Name: "b", Probability: 1}},
		final:   []analyzer.Class{{Name: "b", Probability: 1, Count: 2}}}
	both := &fake{name: "both", scope: analyzer.ScopeFile | analyzer.ScopeSnapshot,
		classes: []analyzer.Class{{Name: "c", Probability: 1}},
		final:   []analyzer.Class{{Name: "c", Probability: 1, Count: 3}}}

	r, err := analyzer.NewRunner(nil, []analyzer.Analyzer{fileOnly, snapOnly, both})
	require.NoError(t, err)

	res := runFile(t, r, "/f", "text/plain", nil, "abc")
	require.Equal(t, []analyzer.Result{
		{Analyzer: "file-only", Classes: fileOnly.classes},
		{Analyzer: "both", Classes: both.classes},
	}, res)

	require.Nil(t, fileOnly.observed)
	require.Equal(t, snapOnly.classes, snapOnly.observed["/f"])
	require.Equal(t, both.classes, both.observed["/f"])

	// cached results feed snapshot-level analyzers, without the failures
	r.ObserveCached(context.Background(), "/cached", []analyzer.Result{
		{Analyzer: "both", Classes: []analyzer.Class{{Name: "c", Probability: 1}, {Name: analyzer.ClassError}}},
		{Analyzer: "gone", Classes: []analyzer.Class{{Name: "x", Probability: 1}}},
	})
	require.Equal(t, []analyzer.Class{{Name: "c", Probability: 1}}, both.observed["/cached"])

	require.Equal(t, []analyzer.Result{
		{Analyzer: "snap-only", Classes: snapOnly.final},
		{Analyzer: "both", Classes: both.final},
	}, r.Finalize(context.Background()))

	require.NoError(t, r.Close(context.Background()))
	require.True(t, fileOnly.closed && snapOnly.closed && both.closed)
}

func TestNewRunnerRejectsBadConfig(t *testing.T) {
	_, err := analyzer.NewRunner(nil, []analyzer.Analyzer{&fake{name: "Bad/Name", scope: analyzer.ScopeFile}})
	require.Error(t, err)

	_, err = analyzer.NewRunner(nil, []analyzer.Analyzer{&fake{name: "noscope"}})
	require.Error(t, err)

	_, err = analyzer.NewRunner(nil, []analyzer.Analyzer{&fake{name: "head", scope: analyzer.ScopeFile,
		filter: analyzer.Filter{Content: analyzer.ContentHead}}})
	require.Error(t, err)

	_, err = analyzer.NewRunner(nil, []analyzer.Analyzer{
		&fake{name: "twice", scope: analyzer.ScopeFile},
		&fake{name: "twice", scope: analyzer.ScopeFile},
	})
	require.Error(t, err)
}
