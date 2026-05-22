package kcontext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewKContext(t *testing.T) {
	ctx := NewKContext()
	require.NotNil(t, ctx)
	require.NotNil(t, ctx.Stdin)
	require.NotNil(t, ctx.Stdout)
	require.NotNil(t, ctx.Stderr)
	require.NotNil(t, ctx.Context)
	require.NotNil(t, ctx.Cancel)
	require.NotNil(t, ctx.events)
}

func TestNewKContextFrom(t *testing.T) {
	base := NewKContext()
	base.Username = "alice"
	base.Hostname = "server"

	derived := NewKContextFrom(base)
	require.NotNil(t, derived)
	require.Equal(t, "alice", derived.Username)
	require.Equal(t, "server", derived.Hostname)
	// derived should have its own events bus (different pointer)
	require.NotSame(t, base.events, derived.events)
}

func TestKContextDeadline(t *testing.T) {
	ctx := NewKContext()
	defer ctx.Close()

	deadline, ok := ctx.Deadline()
	// Background context has no deadline
	require.False(t, ok)
	require.True(t, deadline.IsZero())
}

func TestKContextDone(t *testing.T) {
	ctx := NewKContext()
	ch := ctx.Done()
	require.NotNil(t, ch)

	// Channel should be open before Close
	select {
	case <-ch:
		t.Error("context done before Close")
	default:
	}

	ctx.Close()

	// After close, channel should be closed
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Error("context done channel not closed after Close")
	}
}

func TestKContextErr(t *testing.T) {
	ctx := NewKContext()
	require.NoError(t, ctx.Err())

	ctx.Close()
	// After cancel, Err() should be non-nil
	require.Error(t, ctx.Err())
}

func TestKContextValue(t *testing.T) {
	ctx := NewKContext()
	defer ctx.Close()
	// Background context has no values
	require.Nil(t, ctx.Value("nonexistent"))
}

func TestKContextEvents(t *testing.T) {
	ctx := NewKContext()
	defer ctx.Close()

	ev := ctx.Events()
	require.NotNil(t, ev)
	require.Equal(t, ctx.events, ev)
}

func TestNewKContextFromCancelPropagation(t *testing.T) {
	parent := NewKContext()
	child := NewKContextFrom(parent)

	// Cancel parent — child should also be cancelled
	parent.Cancel(nil)

	select {
	case <-child.Done():
	case <-time.After(time.Second):
		t.Error("child context not cancelled when parent cancelled")
	}

	child.Close()
}
