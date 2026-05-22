package logging_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/logging"
)

func TestNewLogger(t *testing.T) {
	var out, err bytes.Buffer
	l := logging.NewLogger(&out, &err)
	if l == nil {
		t.Fatal("NewLogger returned nil")
	}
}

func TestPrintf(t *testing.T) {
	var out bytes.Buffer
	l := logging.NewLogger(&out, &out)
	l.Printf("hello %s", "world")
	if !strings.Contains(out.String(), "hello world") {
		t.Errorf("Printf output missing: %q", out.String())
	}
}

func TestStdout(t *testing.T) {
	var out bytes.Buffer
	l := logging.NewLogger(&out, &out)
	l.Stdout("stdout message")
	if !strings.Contains(out.String(), "stdout message") {
		t.Errorf("Stdout output missing: %q", out.String())
	}
}

func TestStderr(t *testing.T) {
	var err bytes.Buffer
	l := logging.NewLogger(&err, &err)
	l.Stderr("stderr message")
	if !strings.Contains(err.String(), "stderr message") {
		t.Errorf("Stderr output missing: %q", err.String())
	}
}

func TestWarn(t *testing.T) {
	var err bytes.Buffer
	l := logging.NewLogger(&err, &err)
	l.Warn("warn message")
	if !strings.Contains(err.String(), "warn: warn message") {
		t.Errorf("Warn output missing: %q", err.String())
	}
}

func TestError(t *testing.T) {
	var err bytes.Buffer
	l := logging.NewLogger(&err, &err)
	l.Error("error message")
	if !strings.Contains(err.String(), "error: error message") {
		t.Errorf("Error output missing: %q", err.String())
	}
}

func TestDebug(t *testing.T) {
	var err bytes.Buffer
	l := logging.NewLogger(&err, &err)
	l.Debug("debug message")
	if !strings.Contains(err.String(), "debug: debug message") {
		t.Errorf("Debug output missing: %q", err.String())
	}
}

// TestInfo verifies that Info only logs when EnabledInfo is true.
func TestInfo(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		var out bytes.Buffer
		l := logging.NewLogger(&out, &out)
		// EnabledInfo is false by default — nothing should be written.
		l.Info("should not appear")
		if out.Len() != 0 {
			t.Errorf("expected no output, got %q", out.String())
		}
	})

	t.Run("enabled", func(t *testing.T) {
		var out bytes.Buffer
		l := logging.NewLogger(&out, &out)
		l.EnableInfo()
		l.Info("should appear")
		if !strings.Contains(out.String(), "info: should appear") {
			t.Errorf("Info output missing: %q", out.String())
		}
	})
}

// TestTrace verifies the Trace method only logs when the subsystem is enabled.
func TestTrace(t *testing.T) {
	t.Run("no tracing", func(t *testing.T) {
		var out bytes.Buffer
		l := logging.NewLogger(&out, &out)
		l.Trace("subsys", "should not appear")
		if out.Len() != 0 {
			t.Errorf("expected no output, got %q", out.String())
		}
	})

	t.Run("specific subsystem", func(t *testing.T) {
		var out bytes.Buffer
		l := logging.NewLogger(&out, &out)
		l.EnableTracing("subsys")
		l.Trace("subsys", "trace message")
		if !strings.Contains(out.String(), "trace: subsys: trace message") {
			t.Errorf("Trace output missing: %q", out.String())
		}
	})

	t.Run("all subsystems", func(t *testing.T) {
		var out bytes.Buffer
		l := logging.NewLogger(&out, &out)
		l.EnableTracing("all")
		l.Trace("anything", "all trace")
		if !strings.Contains(out.String(), "trace: anything: all trace") {
			t.Errorf("Trace(all) output missing: %q", out.String())
		}
	})

	t.Run("wrong subsystem not logged", func(t *testing.T) {
		var out bytes.Buffer
		l := logging.NewLogger(&out, &out)
		l.EnableTracing("subsys")
		l.Trace("other", "should not appear")
		if out.Len() != 0 {
			t.Errorf("expected no output for wrong subsystem, got %q", out.String())
		}
	})
}

// TestSetOutput verifies that SetOutput redirects both loggers.
func TestSetOutput(t *testing.T) {
	var orig, newDest bytes.Buffer
	l := logging.NewLogger(&orig, &orig)

	l.SetOutput(&newDest)
	l.Stdout("after redirect")

	if orig.Len() != 0 {
		t.Errorf("expected nothing in original buffer, got %q", orig.String())
	}
	if !strings.Contains(newDest.String(), "after redirect") {
		t.Errorf("expected output in newDest, got %q", newDest.String())
	}
}

// TestSetSyslogOutput verifies that SetSyslogOutput works.
func TestSetSyslogOutput(t *testing.T) {
	var syslog bytes.Buffer
	var out bytes.Buffer
	l := logging.NewLogger(&out, &out)
	l.SetSyslogOutput(&syslog)
	// After SetSyslogOutput both loggers write to syslog.
	l.Stderr("syslog message")
	if !strings.Contains(syslog.String(), "syslog message") {
		t.Errorf("expected syslog output, got %q", syslog.String())
	}
}

// TestEnableTracingMultiple checks that comma-separated subsystems all get enabled.
func TestEnableTracingMultiple(t *testing.T) {
	var out bytes.Buffer
	l := logging.NewLogger(&out, &out)
	l.EnableTracing("alpha,beta,gamma")

	for _, sub := range []string{"alpha", "beta", "gamma"} {
		out.Reset()
		l.Trace(sub, "msg")
		if !strings.Contains(out.String(), "trace: "+sub+": msg") {
			t.Errorf("subsystem %q not traced, got %q", sub, out.String())
		}
	}

	// A subsystem not in the list should not be traced.
	out.Reset()
	l.Trace("delta", "should not appear")
	if out.Len() != 0 {
		t.Errorf("expected no output for delta, got %q", out.String())
	}
}
