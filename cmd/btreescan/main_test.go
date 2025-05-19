package main

import (
	"bytes"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ptesting "github.com/PlakarKorp/plakar/testing"
)

// resetFlags resets the flag state between tests
func resetFlags() {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
}

func TestBTreeScanMemory(t *testing.T) {
	resetFlags()

	tmpSourceDir := ptesting.GenerateFiles(t, []ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockDir("another_subdir"),
		ptesting.NewMockFile("subdir/dummy.txt", 0644, "hello dummy"),
		ptesting.NewMockFile("subdir/foo.txt", 0644, "hello foo"),
		ptesting.NewMockFile("subdir/to_exclude", 0644, "*/subdir/to_exclude\n"),
		ptesting.NewMockFile("another_subdir/bar.txt", 0644, "hello bar"),
	})

	// Capture log output
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)
	defer log.SetOutput(os.Stderr) // Restore original output

	// Run the btreescan command
	os.Args = []string{
		"btreescan",
		"-dbpath", "memory",
		"-order", "50",
		"-verify",
		"-xattr",
		tmpSourceDir,
	}

	// Run main() in a separate goroutine since it might block
	done := make(chan struct{})
	go func() {
		main()
		close(done)
	}()

	// Wait for main() to complete
	<-done

	// Get the log output
	logContent := logOutput.String()

	// Verify expected log messages
	expectedLogs := []string{
		"starting the scan",
		"scan finished. 9 items scanned",
	}

	for _, expected := range expectedLogs {
		if !strings.Contains(logContent, expected) {
			t.Errorf("Expected log output to contain %q, but it didn't", expected)
		}
	}
}

func TestBTreeScanPebble(t *testing.T) {
	resetFlags()

	tmpSourceDir := ptesting.GenerateFiles(t, []ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockDir("another_subdir"),
		ptesting.NewMockFile("subdir/dummy.txt", 0644, "hello dummy"),
		ptesting.NewMockFile("subdir/foo.txt", 0644, "hello foo"),
		ptesting.NewMockFile("subdir/to_exclude", 0644, "*/subdir/to_exclude\n"),
		ptesting.NewMockFile("another_subdir/bar.txt", 0644, "hello bar"),
	})

	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "btreescan-output-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Capture log output
	var logOutput bytes.Buffer
	log.SetOutput(&logOutput)
	defer log.SetOutput(os.Stderr) // Restore original output

	// Create a temporary directory for the pebble database
	dbDir, err := os.MkdirTemp("", "btreescan-db-*")
	if err != nil {
		t.Fatalf("Failed to create temp db dir: %v", err)
	}
	defer os.RemoveAll(dbDir)

	// Run the btreescan command
	os.Args = []string{
		"btreescan",
		"-dbpath", dbDir,
		"-order", "50",
		"-verify",
		"-xattr",
		"-profile-mem", filepath.Join(tempDir, "pprof.mem"),
		"-profile-cpu", filepath.Join(tempDir, "pprof.cpu"),
		"-dot", filepath.Join(tempDir, "dot.out"),
		tmpSourceDir,
	}

	// Run main() in a separate goroutine since it might block
	done := make(chan struct{})
	go func() {
		main()
		close(done)
	}()

	// Wait for main() to complete
	<-done

	// Get the log output
	logContent := logOutput.String()

	// Verify expected log messages
	expectedLogs := []string{
		"starting the scan",
		"scan finished. 9 items scanned",
	}

	for _, expected := range expectedLogs {
		if !strings.Contains(logContent, expected) {
			t.Errorf("Expected log output to contain %q, but it didn't", expected)
		}
	}

	// Verify that the database directory was created and contains files
	dbFiles, err := os.ReadDir(dbDir)
	if err != nil {
		t.Fatalf("Failed to read db directory: %v", err)
	}

	if len(dbFiles) == 0 {
		t.Error("Database directory is empty, expected some files")
	}

	// Verify that the dot file was created
	dotFile := filepath.Join(tempDir, "dot.out")
	if _, err := os.Stat(dotFile); os.IsNotExist(err) {
		t.Errorf("Expected dot file %s to exist, but it doesn't", dotFile)
	}

	// Verify that the memory profile file was created
	memFile := filepath.Join(tempDir, "pprof.mem")
	if _, err := os.Stat(memFile); os.IsNotExist(err) {
		t.Errorf("Expected memory profile file %s to exist, but it doesn't", memFile)
	}

	// Verify that the CPU profile file was created
	cpuFile := filepath.Join(tempDir, "pprof.cpu")
	if _, err := os.Stat(cpuFile); os.IsNotExist(err) {
		t.Errorf("Expected CPU profile file %s to exist, but it doesn't", cpuFile)
	}
}
