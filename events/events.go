package events

import (
	"time"
)

/*
	file.ok, directory.ok
	path.ok					snapshot	[]byte
						path		string

	file.error,  (directory.error?)
	path.error				snapshot	[]byte
						path		string
						error		string

	importer.start		snapshot	[]byte

	importer.done		snapshot	[]byte
						nfiles		uint64
						ndirs		uint64
						size		uint64

	backup.start,
	backup.done			snapshot	[]byte

	sync.start,
	sync.done			snapshot	[]byte


TODO:
	readd ...
	add repository id to all events (if possible)
	add a job id
*/

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

func (eb *EventsBUS) Emitter() *Emitter {
	return &Emitter{bus: eb}
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
)

type Event struct {
	Version   int            `msgpack:"version"`
	Timestamp time.Time      `msgpack:"timestamp"`
	Level     string         `msgpack:"level"`
	Type      string         `msgpack:"type"`
	Data      map[string]any `msgpack:"kv,omitempty"`
}

type Emitter struct {
	bus *EventsBUS
}

func NewDummyEmitter() *Emitter {
	return &Emitter{bus: nil}
}

func NewEmitter(eb *EventsBUS) *Emitter {
	return &Emitter{bus: eb}
}

func (e *Emitter) emit(typ, level string, kv map[string]any) {
	if e.bus == nil || e.bus.c == nil {
		return
	}
	e.bus.c <- &Event{
		Version: 1, Timestamp: time.Now().UTC(),
		Level: level, Type: typ, Data: kv,
	}
}

func (e *Emitter) Emit(typ string, kv map[string]any) {
	e.emit(typ, "", kv)
}

func (e *Emitter) Info(typ string, kv map[string]any) {
	e.emit(typ, Info, kv)
}

func (e *Emitter) Warn(typ string, kv map[string]any) {
	e.emit(typ, Warn, kv)
}

func (e *Emitter) Error(typ string, kv map[string]any) {
	e.emit(typ, Error, kv)
}
