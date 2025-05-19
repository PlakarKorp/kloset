package versioning

import (
	"testing"

	"github.com/PlakarKorp/kloset/resources"
)

func TestNewVersion(t *testing.T) {
	tests := []struct {
		name                string
		major, minor, patch uint32
		want                Version
	}{
		{"basic version", 1, 2, 3, Version(0x010203)},
		{"zero version", 0, 0, 0, Version(0)},
		{"max single byte", 255, 255, 255, Version(0xFFFFFF)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewVersion(tt.major, tt.minor, tt.patch)
			if got != tt.want {
				t.Errorf("NewVersion(%d, %d, %d) = %v, want %v", tt.major, tt.minor, tt.patch, got, tt.want)
			}
		})
	}
}

func TestVersionComponents(t *testing.T) {
	v := NewVersion(1, 2, 3)

	if major := v.Major(); major != 1 {
		t.Errorf("Major() = %d, want 1", major)
	}
	if minor := v.Minor(); minor != 2 {
		t.Errorf("Minor() = %d, want 2", minor)
	}
	if patch := v.Patch(); patch != 3 {
		t.Errorf("Patch() = %d, want 3", patch)
	}
}

func TestVersionString(t *testing.T) {
	tests := []struct {
		name string
		v    Version
		want string
	}{
		{"basic version", NewVersion(1, 2, 3), "1.2.3"},
		{"zero version", NewVersion(0, 0, 0), "0.0.0"},
		{"max single byte", NewVersion(255, 255, 255), "255.255.255"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.v.String(); got != tt.want {
				t.Errorf("Version.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    Version
		wantErr bool
	}{
		{"valid version", "1.2.3", NewVersion(1, 2, 3), false},
		{"zero version", "0.0.0", NewVersion(0, 0, 0), false},
		{"max version", "255.255.255", NewVersion(255, 255, 255), false},
		{"invalid format", "1.2", Version(0), true},
		{"invalid numbers", "a.b.c", Version(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wantErr {
						t.Errorf("FromString() panic = %v, want no panic", r)
					}
				} else if tt.wantErr {
					t.Error("FromString() did not panic, want panic")
				}
			}()

			got := FromString(tt.input)
			if got != tt.want {
				t.Errorf("FromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegisterAndGetCurrentVersion(t *testing.T) {
	// Reset the currentVersions map for testing
	currentVersions = make(map[resources.Type]Version)

	testType := resources.RT_CONFIG
	testVersion := NewVersion(1, 0, 0)

	// Test registration
	t.Run("register version", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Register() panic = %v, want no panic", r)
			}
		}()

		Register(testType, testVersion)
	})

	// Test getting registered version
	t.Run("get current version", func(t *testing.T) {
		got := GetCurrentVersion(testType)
		if got != testVersion {
			t.Errorf("GetCurrentVersion() = %v, want %v", got, testVersion)
		}
	})

	// Test double registration
	t.Run("double registration", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Register() did not panic, want panic")
			}
		}()

		Register(testType, testVersion)
	})

	// Test getting unregistered version
	t.Run("get unregistered version", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("GetCurrentVersion() did not panic, want panic")
			}
		}()

		GetCurrentVersion(resources.RT_LOCK)
	})
}
