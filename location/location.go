package location

import (
	"errors"
	"slices"
	"strings"
	"sync"
)

type Flags uint32

const (
	FLAG_LOCALFS Flags = 1 << 0
	FLAG_FILE    Flags = 1 << 1
)

var ErrUnknownFlag = errors.New("unknown flag")

type tWrapper[T any] struct {
	item  T
	flags Flags
}

type Location[T any] struct {
	mtx      sync.Mutex
	items    map[string]tWrapper[T]
	fallback string
}

func New[T any](fallback string) *Location[T] {
	return &Location[T]{
		items:    make(map[string]tWrapper[T]),
		fallback: fallback,
	}
}

func (l *Location[T]) Register(name string, item T, flags Flags) bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if _, ok := l.items[name]; ok {
		return false
	}
	l.items[name] = tWrapper[T]{
		item:  item,
		flags: flags,
	}
	return true
}

func (l *Location[T]) Unregister(name string) bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if _, ok := l.items[name]; ok {
		delete(l.items, name)
		return true
	}
	return false
}

func (l *Location[T]) Names() []string {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	ret := make([]string, 0, len(l.items))
	for name := range l.items {
		ret = append(ret, name)
	}
	slices.Sort(ret)
	return ret
}

func allowedInUri(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '+' || c == '-' || c == '.'
}

func (l *Location[T]) Lookup(uri string) (proto, location string, item T, flags Flags, ok bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	proto = uri
	location = uri

	for i, c := range uri {
		if !allowedInUri(c) {
			/*
			 * don't accept one-character URI, it makes
			 * things confusing for windows where
			 * E:\path\to\foo is the norm.
			 */
			if i > 1 && strings.HasPrefix(uri[i:], ":") {
				proto = uri[:i]
				location = uri[i+1:]
				location = strings.TrimPrefix(location, "//")
			}
			break
		}
	}

	if proto == location {
		proto = l.fallback
	}

	t, ok := l.items[proto]
	if ok {
		item = t.item
		flags = t.flags
	}
	return
}

func ParseFlag(name string) (Flags, error) {
	switch name {
	case "localfs":
		return FLAG_LOCALFS, nil
	case "file":
		return FLAG_FILE, nil
	default:
		return 0, ErrUnknownFlag
	}
}
