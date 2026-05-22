package events_test

import (
	"errors"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewEventsBUS(t *testing.T) {
	bus := events.NewEventsBUS(10)
	require.NotNil(t, bus)
}

func TestEventsBUSClose(t *testing.T) {
	bus := events.NewEventsBUS(10)
	ch := bus.Listen()
	require.NotNil(t, ch)
	// Close should close the channel
	bus.Close()
	_, ok := <-ch
	require.False(t, ok, "channel should be closed")
}

func TestEventsBUSCloseWithoutListen(t *testing.T) {
	bus := events.NewEventsBUS(10)
	// Close without Listen should not panic
	bus.Close()
}

func TestNewDummyEmitter(t *testing.T) {
	bus := events.NewEventsBUS(0)
	emitter := bus.NewDummyEmitter()
	require.NotNil(t, emitter)

	// Dummy emitter should not send any events
	ch := bus.Listen()
	emitter.Info("test", nil)
	emitter.Warn("test", nil)
	emitter.Error("test", nil)
	emitter.Quiet("test", nil)

	select {
	case <-ch:
		t.Error("dummy emitter should not send events")
	default:
	}
}

func TestNewRepositoryEmitter(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	repoID := uuid.New()
	emitter := bus.NewRepositoryEmitter(repoID, "test-workflow")
	require.NotNil(t, emitter)

	// Should receive the workflow.start event
	select {
	case ev := <-ch:
		require.Equal(t, "workflow.start", ev.Type)
		require.Equal(t, events.Info, ev.Level)
		require.Equal(t, repoID, ev.Repository)
		require.Equal(t, "test-workflow", ev.Workflow)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for workflow.start event")
	}

	emitter.Close()

	select {
	case ev := <-ch:
		require.Equal(t, "workflow.end", ev.Type)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for workflow.end event")
	}
}

func TestNewSnapshotEmitter(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	repoID := uuid.New()
	snap := objects.MAC{1, 2, 3}
	emitter := bus.NewSnapshotEmitter(repoID, snap, "snapshot-workflow")
	require.NotNil(t, emitter)

	select {
	case ev := <-ch:
		require.Equal(t, "workflow.start", ev.Type)
		require.Equal(t, repoID, ev.Repository)
		require.Equal(t, snap, ev.Snapshot)
		require.Equal(t, "snapshot-workflow", ev.Workflow)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
	emitter.Close()
	<-ch // consume workflow.end
}

func TestEmitterLevels(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	repoID := uuid.New()
	emitter := bus.NewRepositoryEmitter(repoID, "test")
	<-ch // consume workflow.start

	cases := []struct {
		fn    func(string, map[string]any)
		level string
	}{
		{emitter.Info, events.Info},
		{emitter.Warn, events.Warn},
		{emitter.Error, events.Error},
		{emitter.Quiet, events.Quiet},
	}

	for _, c := range cases {
		c.fn("test.event", map[string]any{"key": "value"})
		select {
		case ev := <-ch:
			require.Equal(t, c.level, ev.Level)
			require.Equal(t, "test.event", ev.Type)
		case <-time.After(time.Second):
			t.Fatalf("timeout for level %s", c.level)
		}
	}

	emitter.Close()
	<-ch // workflow.end
	bus.Close()
}

func TestEmitterPathEvents(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	emitter := bus.NewRepositoryEmitter(uuid.New(), "w")
	<-ch

	emitter.Path("/foo/bar")
	ev := <-ch
	require.Equal(t, "path", ev.Type)
	require.Equal(t, "/foo/bar", ev.Data["path"])

	emitter.Directory("/foo")
	ev = <-ch
	require.Equal(t, "directory", ev.Type)

	emitter.File("/foo/file.txt")
	ev = <-ch
	require.Equal(t, "file", ev.Type)

	emitter.Xattr("/foo/file.txt")
	ev = <-ch
	require.Equal(t, "xattr", ev.Type)

	emitter.Symlink("/foo/link")
	ev = <-ch
	require.Equal(t, "symlink", ev.Type)

	mac := objects.MAC{1}
	emitter.Object(mac)
	ev = <-ch
	require.Equal(t, "object", ev.Type)

	emitter.Chunk(mac)
	ev = <-ch
	require.Equal(t, "chunk", ev.Type)

	emitter.Close()
	<-ch
	bus.Close()
}

func TestEmitterOkEvents(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	emitter := bus.NewRepositoryEmitter(uuid.New(), "w")
	<-ch

	mac := objects.MAC{2}

	emitter.PathOk("/foo")
	ev := <-ch
	require.Equal(t, "path.ok", ev.Type)

	emitter.DirectoryOk("/foo", objects.FileInfo{})
	ev = <-ch
	require.Equal(t, "directory.ok", ev.Type)

	emitter.FileOk("/foo/f", objects.FileInfo{})
	ev = <-ch
	require.Equal(t, "file.ok", ev.Type)

	emitter.XattrOk("/foo/f", 42)
	ev = <-ch
	require.Equal(t, "xattr.ok", ev.Type)
	require.Equal(t, int64(42), ev.Data["size"])

	emitter.SymlinkOk("/foo/link")
	ev = <-ch
	require.Equal(t, "symlink.ok", ev.Type)

	emitter.ObjectOk(mac)
	ev = <-ch
	require.Equal(t, "object.ok", ev.Type)

	emitter.ChunkOk(mac)
	ev = <-ch
	require.Equal(t, "chunk.ok", ev.Type)

	emitter.Close()
	<-ch
	bus.Close()
}

func TestEmitterCachedEvents(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	emitter := bus.NewRepositoryEmitter(uuid.New(), "w")
	<-ch

	mac := objects.MAC{3}

	emitter.PathCached("/foo")
	ev := <-ch
	require.Equal(t, "path.cached", ev.Type)

	emitter.DirectoryCached("/foo", objects.FileInfo{})
	ev = <-ch
	require.Equal(t, "directory.cached", ev.Type)

	emitter.FileCached("/foo/f", objects.FileInfo{})
	ev = <-ch
	require.Equal(t, "file.cached", ev.Type)

	emitter.XattrCached("/foo/f", 100)
	ev = <-ch
	require.Equal(t, "xattr.cached", ev.Type)

	emitter.SymlinkCached("/foo/link")
	ev = <-ch
	require.Equal(t, "symlink.cached", ev.Type)

	emitter.ObjectCached(mac)
	ev = <-ch
	require.Equal(t, "object.cached", ev.Type)

	emitter.ChunkCached(mac)
	ev = <-ch
	require.Equal(t, "chunk.cached", ev.Type)

	emitter.Close()
	<-ch
	bus.Close()
}

func TestEmitterErrorEvents(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	emitter := bus.NewRepositoryEmitter(uuid.New(), "w")
	<-ch

	mac := objects.MAC{4}
	someErr := errors.New("test error")

	emitter.PathError("/foo", someErr)
	ev := <-ch
	require.Equal(t, "path.error", ev.Type)
	require.Equal(t, events.Error, ev.Level)

	emitter.DirectoryError("/foo", someErr)
	ev = <-ch
	require.Equal(t, "directory.error", ev.Type)

	emitter.FileError("/foo/f", someErr)
	ev = <-ch
	require.Equal(t, "file.error", ev.Type)

	emitter.XattrError("/foo/f", someErr)
	ev = <-ch
	require.Equal(t, "xattr.error", ev.Type)

	emitter.SymlinkError("/foo/link", someErr)
	ev = <-ch
	require.Equal(t, "symlink.error", ev.Type)

	emitter.ObjectError(mac, someErr)
	ev = <-ch
	require.Equal(t, "object.error", ev.Type)

	emitter.ChunkError(mac, someErr)
	ev = <-ch
	require.Equal(t, "chunk.error", ev.Type)

	emitter.Close()
	<-ch
	bus.Close()
}

func TestEmitterResult(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	emitter := bus.NewRepositoryEmitter(uuid.New(), "w")
	<-ch

	emitter.Result("/target", 1024, 0, time.Second, 512, 256)
	ev := <-ch
	require.Equal(t, "result", ev.Type)
	require.Equal(t, events.Quiet, ev.Level)
	require.Equal(t, uint64(1024), ev.Data["size"])
	require.Equal(t, uint64(0), ev.Data["errors"])

	emitter.Close()
	<-ch
	bus.Close()
}

func TestEmitterFilesystemSummary(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	emitter := bus.NewRepositoryEmitter(uuid.New(), "w")
	<-ch

	emitter.FilesystemSummary(10, 5, 2, 1, 4096)
	ev := <-ch
	require.Equal(t, "fs.summary", ev.Type)
	require.Equal(t, uint64(10), ev.Data["files"])
	require.Equal(t, uint64(5), ev.Data["directories"])
	require.Equal(t, uint64(2), ev.Data["symlinks"])
	require.Equal(t, uint64(1), ev.Data["xattrs"])
	require.Equal(t, uint64(4096), ev.Data["size"])

	emitter.Close()
	<-ch
	bus.Close()
}

func TestEventFields(t *testing.T) {
	bus := events.NewEventsBUS(100)
	ch := bus.Listen()

	repoID := uuid.New()
	emitter := bus.NewRepositoryEmitter(repoID, "check")
	ev := <-ch

	require.Equal(t, 1, ev.Version)
	require.Equal(t, repoID, ev.Repository)
	require.False(t, ev.Timestamp.IsZero())
	require.NotEqual(t, uuid.Nil, ev.Job)

	emitter.Close()
	<-ch
	bus.Close()
}

func TestListenReturnsConsistentChannel(t *testing.T) {
	bus := events.NewEventsBUS(10)
	ch1 := bus.Listen()
	ch2 := bus.Listen()
	require.Equal(t, ch1, ch2, "Listen should return the same channel")
	bus.Close()
}
