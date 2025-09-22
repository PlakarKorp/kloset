package location

import (
	"testing"
)

func TestNew(t *testing.T) {
	fallback := "default"
	loc := New[string](fallback)
	if loc == nil {
		t.Fatal("New returned nil")
	}
	if loc.fallback != fallback {
		t.Errorf("fallback = %q, want %q", loc.fallback, fallback)
	}
	if loc.items == nil {
		t.Error("items map is nil")
	}
}

func TestRegister(t *testing.T) {
	loc := New[string]("default")

	// Test successful registration
	if !loc.Register("test", "value", 0) {
		t.Error("Register failed to register new item")
	}

	// Test duplicate registration
	if loc.Register("test", "value2", 0) {
		t.Error("Register succeeded when it should have failed for duplicate")
	}

	// Verify the value wasn't changed
	if v, ok := loc.items["test"]; !ok || v.item != "value" {
		t.Errorf("Duplicate registration changed value: got %v, want %v", v, "value")
	}
}

func TestNames(t *testing.T) {
	loc := New[string]("default")
	expected := []string{"a", "b", "c"}

	// Register items in reverse order to test sorting
	loc.Register("c", "value3", 0)
	loc.Register("b", "value2", 0)
	loc.Register("a", "value1", 0)

	names := loc.Names()
	if len(names) != len(expected) {
		t.Errorf("Names() returned %d items, want %d", len(names), len(expected))
	}

	for i, name := range names {
		if name != expected[i] {
			t.Errorf("Names()[%d] = %q, want %q", i, name, expected[i])
		}
	}
}

func TestLookup(t *testing.T) {
	loc := New[string]("default")
	loc.Register("http", "http-value", 0)
	loc.Register("https", "https-value", 0)

	tests := []struct {
		name         string
		uri          string
		wantProto    string
		wantLocation string
		wantValue    string
		wantFound    bool
	}{
		{
			name:         "simple http",
			uri:          "http://example.com",
			wantProto:    "http",
			wantLocation: "example.com",
			wantValue:    "http-value",
			wantFound:    true,
		},
		{
			name:         "simple https",
			uri:          "https://example.com",
			wantProto:    "https",
			wantLocation: "example.com",
			wantValue:    "https-value",
			wantFound:    true,
		},
		{
			name:         "unknown protocol",
			uri:          "ftp://example.com",
			wantProto:    "ftp",
			wantLocation: "example.com",
			wantValue:    "",
			wantFound:    false,
		},
		{
			name:         "no protocol",
			uri:          "example.com",
			wantProto:    "default",
			wantLocation: "example.com",
			wantValue:    "",
			wantFound:    false,
		},
		{
			name:         "windows absolute path",
			uri:          "C:\\Users\\Plakup",
			wantProto:    "default",
			wantLocation: "C:\\Users\\Plakup",
			wantValue:    "",
			wantFound:    false,
		},
		{
			name:         "empty string",
			uri:          "",
			wantProto:    "default",
			wantLocation: "",
			wantValue:    "",
			wantFound:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proto, location, value, _, found := loc.Lookup(tt.uri)
			if proto != tt.wantProto {
				t.Errorf("Lookup() proto = %v, want %v", proto, tt.wantProto)
			}
			if location != tt.wantLocation {
				t.Errorf("Lookup() location = %v, want %v", location, tt.wantLocation)
			}
			if value != tt.wantValue {
				t.Errorf("Lookup() value = %v, want %v", value, tt.wantValue)
			}
			if found != tt.wantFound {
				t.Errorf("Lookup() found = %v, want %v", found, tt.wantFound)
			}
		})
	}
}
