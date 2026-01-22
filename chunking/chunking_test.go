package chunking

import (
	"testing"

	chunkers "github.com/PlakarKorp/go-cdc-chunkers"
)

func TestDefaultAlgorithm(t *testing.T) {
	expected := "fastcdc-v1.0.0"
	result := NewDefaultConfiguration().Algorithm

	if result != expected {
		t.Errorf("DefaultAlgorithm failed: expected %v, got %v", expected, result)
	}
}

func TestDefaultConfiguration(t *testing.T) {
	expected := &chunkers.ChunkerOpts{
		MinSize:    512 * 1024,
		NormalSize: 1 * 1024 * 1024,
		MaxSize:    8 * 1024 * 1024,
	}

	result := NewDefaultConfiguration()

	if int(result.MinSize) != expected.MinSize {
		t.Errorf("DefaultConfiguration MinSize failed: expected %v, got %v", expected.MinSize, result.MinSize)
	}
	if int(result.NormalSize) != expected.NormalSize {
		t.Errorf("DefaultConfiguration NormalSize failed: expected %v, got %v", expected.NormalSize, result.NormalSize)
	}
	if int(result.MaxSize) != expected.MaxSize {
		t.Errorf("DefaultConfiguration MaxSize failed: expected %v, got %v", expected.MaxSize, result.MaxSize)
	}
}
