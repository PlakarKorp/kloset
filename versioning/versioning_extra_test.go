package versioning

import (
	"testing"

	"github.com/PlakarKorp/kloset/resources"
)

// TestVersionEquals verifies that Equals uses semver comparison correctly.
func TestVersionEquals(t *testing.T) {
	a := NewVersion(1, 2, 3)
	b := NewVersion(1, 2, 3)
	c := NewVersion(1, 2, 4)

	if !a.Equals(b) {
		t.Error("equal versions: Equals should return true")
	}
	if a.Equals(c) {
		t.Error("different versions: Equals should return false")
	}
	// Zero version equals itself.
	z := NewVersion(0, 0, 0)
	if !z.Equals(NewVersion(0, 0, 0)) {
		t.Error("zero version: Equals should return true")
	}
}

// TestIsCompatibleWithCurrentVersion checks that older/equal versions are
// accepted but newer ones are rejected.
func TestIsCompatibleWithCurrentVersion(t *testing.T) {
	// Use a fresh resource type to avoid conflicting with other tests.
	// RT_SNAPSHOT is registered by snapshot/header's init(), but we need
	// something we can register ourselves. We use RT_LOCK which is unlikely
	// to be registered in unit tests for this package.
	//
	// Reset the map so we have a clean slate.
	currentVersions = make(map[resources.Type]Version)

	typ := resources.RT_LOCK
	curr := NewVersion(2, 0, 0)
	Register(typ, curr)

	tests := []struct {
		name    string
		version Version
		want    bool
	}{
		{"same version", NewVersion(2, 0, 0), true},
		{"older major", NewVersion(1, 0, 0), true},
		{"older minor", NewVersion(1, 9, 9), true},
		{"newer major", NewVersion(3, 0, 0), false},
		{"newer minor", NewVersion(2, 1, 0), false},
		{"newer patch", NewVersion(2, 0, 1), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsCompatibleWithCurrentVersion(typ, tt.version)
			if got != tt.want {
				t.Errorf("IsCompatibleWithCurrentVersion(%v) = %v, want %v", tt.version, got, tt.want)
			}
		})
	}
}
