package logging

import (
	"io"
)

type Logger interface {
	SetOutput(w io.Writer)
	SetSyslogOutput(w io.Writer)

	Printf(format string, args ...any)
	Stdout(format string, args ...any)
	Stderr(format string, args ...any)

	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
	Debug(format string, args ...any)

	Trace(subsystem string, format string, args ...any)

	EnableInfo()
	EnableTracing(traces string)

	InfoEnabled() bool
	TracingEnabled() string
}
