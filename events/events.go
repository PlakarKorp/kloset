package events

import (
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type EventsBUS struct {
	c      chan *Event
	buffer int
}

func NewEventsBUS(buffer int) *EventsBUS {
	return &EventsBUS{
		buffer: buffer,
	}
}

func (eb *EventsBUS) Close() {
	if eb.c != nil {
		close(eb.c)
	}
}

func (eb *EventsBUS) NewDummyEmitter() *Emitter {
	return newDummyEmitter()
}

func (eb *EventsBUS) NewRepositoryEmitter(repository uuid.UUID, workflow string) *Emitter {
	job := uuid.Must(uuid.NewRandom())
	return newEmitter(eb, repository, objects.NilMac, job, workflow)
}

func (eb *EventsBUS) NewSnapshotEmitter(repository uuid.UUID, snapshot objects.MAC, workflow string) *Emitter {
	job := uuid.Must(uuid.NewRandom())
	return newEmitter(eb, repository, snapshot, job, workflow)
}

func (eb *EventsBUS) Listen() <-chan *Event {
	if eb.c == nil {
		eb.c = make(chan *Event, eb.buffer)
	}
	return eb.c
}

const (
	Info  = "info"
	Warn  = "warn"
	Error = "error"
	Quiet = "quiet"
)

type Event struct {
	Version    int            `msgpack:"version"`
	Timestamp  time.Time      `msgpack:"timestamp"`
	Repository uuid.UUID      `msgpack:"repository"`
	Snapshot   objects.MAC    `msgpack:"snapshot"`
	Level      string         `msgpack:"level"`
	Workflow   string         `msgpack:"workflow"`
	Job        uuid.UUID      `msgpack:"job"`
	Type       string         `msgpack:"type"`
	Data       map[string]any `msgpack:"kv,omitempty"`
}

type Emitter struct {
	bus        *EventsBUS
	repository uuid.UUID
	snapshot   objects.MAC
	job        uuid.UUID
	workflow   string
}

func (e *Emitter) Close() {
	e.emit("workflow.end", Info, map[string]any{
		"workflow": e.workflow,
	})
}

func newDummyEmitter() *Emitter {
	return &Emitter{bus: nil}
}

func newEmitter(eb *EventsBUS, repository uuid.UUID, snapshot objects.MAC, job uuid.UUID, workflow string) *Emitter {
	e := &Emitter{bus: eb, repository: repository, snapshot: snapshot, job: job, workflow: workflow}
	e.emit("workflow.start", Info, map[string]any{
		"workflow": e.workflow,
	})
	return e
}

func (e *Emitter) emit(typ, level string, kv map[string]any) {
	if e.bus == nil || e.bus.c == nil {
		return
	}
	e.bus.c <- &Event{
		Version: 1, Timestamp: time.Now().UTC(),
		Level: level, Type: typ, Repository: e.repository, Snapshot: e.snapshot, Workflow: e.workflow, Job: e.job, Data: kv,
	}
}

func (e *Emitter) Info(typ string, kv map[string]any) {
	e.emit(typ, Info, kv)
}

func (e *Emitter) Quiet(typ string, kv map[string]any) {
	e.emit(typ, Quiet, kv)
}

func (e *Emitter) Warn(typ string, kv map[string]any) {
	e.emit(typ, Warn, kv)
}

func (e *Emitter) Error(typ string, kv map[string]any) {
	e.emit(typ, Error, kv)
}

/////

func (e *Emitter) Path(path string) {
	e.emit("path", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) Directory(path string) {
	e.emit("directory", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) File(path string) {
	e.emit("file", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) Symlink(path string) {
	e.emit("symlink", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) Object(object objects.MAC) {
	e.emit("object", Info, map[string]any{
		"mac": object,
	})
}

func (e *Emitter) Chunk(chunk objects.MAC) {
	e.emit("chunk", Info, map[string]any{
		"mac": chunk,
	})
}

/////

func (e *Emitter) PathOk(path string) {
	e.emit("path.ok", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) DirectoryOk(path string) {
	e.emit("directory.ok", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) FileOk(path string) {
	e.emit("file.ok", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) SymlinkOk(path string) {
	e.emit("symlink.ok", Info, map[string]any{
		"path": path,
	})
}

func (e *Emitter) ObjectOk(object objects.MAC) {
	e.emit("object.ok", Info, map[string]any{
		"mac": object,
	})
}

func (e *Emitter) ChunkOk(chunk objects.MAC) {
	e.emit("chunk.ok", Info, map[string]any{
		"mac": chunk,
	})
}

/////

func (e *Emitter) PathError(path string, err error) {
	e.emit("path.error", Error, map[string]any{
		"path":  path,
		"error": err,
	})
}

func (e *Emitter) DirectoryError(path string, err error) {
	e.emit("directory.error", Error, map[string]any{
		"path":  path,
		"error": err,
	})
}

func (e *Emitter) FileError(path string, err error) {
	e.emit("file.error", Error, map[string]any{
		"path":  path,
		"error": err,
	})
}

func (e *Emitter) SymlinkError(path string, err error) {
	e.emit("symlink.error", Error, map[string]any{
		"path":  path,
		"error": err,
	})
}

func (e *Emitter) ObjectError(object objects.MAC, err error) {
	e.emit("object.error", Error, map[string]any{
		"mac":   object,
		"error": err,
	})
}

func (e *Emitter) ChunkError(chunk objects.MAC, err error) {
	e.emit("chunk.error", Error, map[string]any{
		"mac":   chunk,
		"error": err,
	})
}

/////

func (e *Emitter) Result(target string, size uint64, errors uint64, duration time.Duration, rBytes int64, wBytes int64) {
	e.emit("result", Quiet, map[string]any{
		"target":   target,
		"size":     size,
		"errors":   errors,
		"duration": duration,
		"rbytes":   rBytes,
		"wbytes":   wBytes,
	})
}
