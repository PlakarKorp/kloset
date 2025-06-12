package testing

import (
	"github.com/PlakarKorp/kloset/classifier"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

func init() {
	classifier.Register("mock", func() classifier.Backend {
		return &MockClassifierBackend{name: "mock"}
	})
}

type MockClassifierBackend struct {
	name string
}

func (m *MockClassifierBackend) Processor(backend classifier.Backend, pathname string) classifier.ProcessorBackend {
	return &MockProcessorBackend{name: m.name}
}

func (m *MockClassifierBackend) Close() error {
	return nil
}

type MockProcessorBackend struct {
	name string
}

func (m *MockProcessorBackend) Name() string {
	return m.name
}

func (m *MockProcessorBackend) File(fileEntry *vfs.Entry) []string {
	return []string{"test-file-class"}
}

func (m *MockProcessorBackend) Directory(dirEntry *vfs.Entry) []string {
	return []string{"test-dir-class"}
}

func (m *MockProcessorBackend) Write(buf []byte) bool {
	return true
}

func (m *MockProcessorBackend) Finalize() []string {
	return []string{"test-final-class"}
}
