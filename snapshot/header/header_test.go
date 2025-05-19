package header

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestSortHeaders(t *testing.T) {
	// Define base test data for consistent resetting
	baseHeaders := []Header{
		{Timestamp: time.Now().Add(-1 * time.Hour), Identifier: [32]byte{0x1}},
		{Timestamp: time.Now().Add(-2 * time.Hour), Identifier: [32]byte{0x3}},
		{Timestamp: time.Now(), Identifier: [32]byte{0x2}},
	}

	// Helper function to reset headers before each test
	resetHeaders := func() []Header {
		return append([]Header(nil), baseHeaders...)
	}

	// Test 1: Sort by CreationTime, ascending
	headers := resetHeaders()
	expected1 := []Header{headers[1], headers[0], headers[2]}
	if err := SortHeaders(headers, []string{"Timestamp"}); err != nil {
		t.Fatalf("Test 1 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected1) {
		t.Errorf("Test 1 failed: expected %v, got %v", expected1, headers)
	}

	// Test 2: Sort by CreationTime, descending
	headers = resetHeaders()
	expected2 := []Header{headers[2], headers[0], headers[1]}
	if err := SortHeaders(headers, []string{"-Timestamp"}); err != nil {
		t.Fatalf("Test 2 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected2) {
		t.Errorf("Test 2 failed: expected %v, got %v", expected2, headers)
	}

	// Test 3: Sort by SnapshotID, ascending (lexicographical comparison of [32]byte)
	headers = resetHeaders()
	expected3 := []Header{headers[0], headers[2], headers[1]}
	if err := SortHeaders(headers, []string{"Identifier"}); err != nil {
		t.Fatalf("Test 3 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected3) {
		t.Errorf("Test 3 failed: expected %v, got %v", expected3, headers)
	}

	// Test 4: Sort by SnapshotID, descending
	headers = resetHeaders()
	expected4 := []Header{headers[1], headers[2], headers[0]}
	if err := SortHeaders(headers, []string{"-Identifier"}); err != nil {
		t.Fatalf("Test 4 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected4) {
		t.Errorf("Test 4 failed: expected %v, got %v", expected4, headers)
	}

	// Test 5: Invalid sort key (should return error)
	headers = resetHeaders()
	err := SortHeaders(headers, []string{"InvalidKey"})
	if err == nil || err.Error() != "invalid sort key: InvalidKey" {
		t.Errorf("Test 5 failed: expected error 'invalid sort key: InvalidKey', got %v", err)
	}

	// Multi-key test: Sort by FilesCount ascending, then CreationTime ascending
	headers = resetHeaders()
	expected6 := []Header{headers[0], headers[2], headers[1]} // FilesCount orders, then CreationTime as tie-breaker
	if err := SortHeaders(headers, []string{"Identifier", "Timestamp"}); err != nil {
		t.Fatalf("Test 6 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected6) {
		t.Errorf("Test 6 failed: expected %v, got %v", expected6, headers)
	}

	// Multi-key test: Sort by FilesCount, then CreationTime descending
	headers = resetHeaders()
	expected7 := []Header{headers[1], headers[2], headers[0]} // FilesCount orders, then CreationTime as tie-breaker
	if err := SortHeaders(headers, []string{"-Identifier", "-Timestamp"}); err != nil {
		t.Fatalf("Test 7 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected7) {
		t.Errorf("Test 7 failed: expected %v, got %v", expected7, headers)
	}

	// Test 8: Sort by Version, ascending
	headers = resetHeaders()
	headers[0].Version = versioning.NewVersion(1, 0, 1)
	headers[1].Version = versioning.NewVersion(1, 0, 3)
	headers[2].Version = versioning.NewVersion(1, 0, 2)
	expected8 := []Header{headers[0], headers[2], headers[1]}
	if err := SortHeaders(headers, []string{"Version"}); err != nil {
		t.Fatalf("Test 8 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected8) {
		t.Errorf("Test 8 failed: expected %v, got %v", expected8, headers)
	}

	// Test 9: Sort by Version, descending
	headers = resetHeaders()
	headers[0].Version = versioning.NewVersion(1, 0, 1)
	headers[1].Version = versioning.NewVersion(1, 0, 3)
	headers[2].Version = versioning.NewVersion(1, 0, 2)
	expected9 := []Header{headers[1], headers[2], headers[0]}
	if err := SortHeaders(headers, []string{"-Version"}); err != nil {
		t.Fatalf("Test 9 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected9) {
		t.Errorf("Test 9 failed: expected %v, got %v", expected9, headers)
	}

	// Test 10: Sort by Tags, ascending
	headers = resetHeaders()
	headers[0].Tags = []string{"beta", "alpha"}
	headers[1].Tags = []string{"alpha", "beta"}
	headers[2].Tags = []string{"zeta"}
	expected10 := []Header{headers[1], headers[0], headers[2]}
	if err := SortHeaders(headers, []string{"Tags"}); err != nil {
		t.Fatalf("Test 10 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected10) {
		t.Errorf("Test 10 failed: expected %v, got %v", expected10, headers)
	}

	// Test 11: Sort by Tags, ascending, with a subslice
	headers = resetHeaders()
	headers[0].Tags = []string{"alpha", "beta", "gamma"}
	headers[1].Tags = []string{"alpha", "beta"}
	headers[2].Tags = []string{"alpha"}
	expected11 := []Header{headers[2], headers[1], headers[0]}
	if err := SortHeaders(headers, []string{"Tags"}); err != nil {
		t.Fatalf("Test 11 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected11) {
		t.Errorf("Test 11 failed: expected %v, got %v", expected11, headers)
	}

	// Test 12: Sort by Tags, descending
	headers = resetHeaders()
	headers[0].Tags = []string{"beta", "alpha"}
	headers[1].Tags = []string{"alpha", "beta"}
	headers[2].Tags = []string{"zeta"}
	expected12 := []Header{headers[2], headers[0], headers[1]}
	if err := SortHeaders(headers, []string{"-Tags"}); err != nil {
		t.Fatalf("Test 12 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected12) {
		t.Errorf("Test 12 failed: expected %v, got %v", expected12, headers)
	}

	// Test 13: Sort by Tags, descending, with a subslice
	headers = resetHeaders()
	headers[0].Tags = []string{"beta", "alpha"}
	headers[1].Tags = []string{"alpha", "beta", "gamma"}
	headers[2].Tags = []string{"alpha"}
	expected13 := []Header{headers[0], headers[1], headers[2]}
	if err := SortHeaders(headers, []string{"-Tags"}); err != nil {
		t.Fatalf("Test 13 failed: unexpected error: %v", err)
	}
	if !reflect.DeepEqual(headers, expected13) {
		t.Errorf("Test 13 failed: expected %v, got %v", expected13, headers)
	}

}
func TestParseSortKeys(t *testing.T) {
	tests := []struct {
		sortKeysStr string
		expected    []string
		err         error
	}{
		{
			sortKeysStr: "",
			expected:    nil,
			err:         nil,
		},
		{
			sortKeysStr: "Name",
			expected:    []string{"Name"},
			err:         nil,
		},
		{
			sortKeysStr: "Name,Name",
			expected:    nil,
			err:         errors.New("duplicate sort key: Name"),
		},
		{
			sortKeysStr: "-Name,Invalid",
			expected:    nil,
			err:         errors.New("invalid sort key: Invalid"),
		},
	}

	for _, test := range tests {
		keys, err := ParseSortKeys(test.sortKeysStr)
		require.Equal(t, keys, test.expected)
		require.Equal(t, err, test.err)
	}
}

func TestHeaderMethods(t *testing.T) {
	header := NewHeader("default", objects.MAC{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
	require.Equal(t, header.GetIndexID(), [32]uint8([32]uint8{0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}))
	require.Equal(t, header.GetIndexShortID(), []uint8([]byte{0x1, 0x2, 0x3, 0x4}))

	require.Equal(t, header.GetContext("key"), "")
	header.SetContext("key", "value")
	require.Equal(t, header.GetContext("key"), "value")

	serialized, err := header.Serialize()
	require.NoError(t, err)
	require.NotNil(t, serialized)

	header2, err := NewFromBytes(serialized)
	require.NoError(t, err)
	require.NotNil(t, header2)
	require.Equal(t, header2.Name, header.Name)

	require.Equal(t, NewSource(), *header.GetSource(0))
}
