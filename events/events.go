package events

import (
	"time"
)

type EventsBUS chan *Event

func NewEventsBUS(buffer int) EventsBUS {
	return make(EventsBUS, buffer)
}

func (eb EventsBUS) Close() {
	close(eb)
}

func (eb EventsBUS) Emitter() *Emitter {
	return &Emitter{in: eb}
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
	in chan<- *Event
}

func NewDummyEmitter() *Emitter {
	return &Emitter{in: nil}
}

func NewEmitter(c chan<- *Event) *Emitter {
	return &Emitter{in: c}
}

func (e *Emitter) Emit(typ, level string, kv map[string]any) {
	if e.in == nil {
		return
	}
	e.in <- &Event{
		Version: 1, Timestamp: time.Now().UTC(),
		Level: level, Type: typ, Data: kv,
	}
}
