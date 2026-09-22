package repository

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func TestMergeFetchRangesEmpty(t *testing.T) {
	require.Nil(t, mergeFetchRanges(nil, 0))
}

func TestMergeFetchRangesSingle(t *testing.T) {
	got := mergeFetchRanges([]PackfileFetchRange{{Offset: 10, Length: 5}}, 0)
	require.Equal(t, []PackfileFetchRange{{Offset: 10, Length: 5}}, got)
}

func TestMergeFetchRangesAdjacentBridged(t *testing.T) {
	ranges := []PackfileFetchRange{
		{Offset: 100, Length: 10},
		{Offset: 110, Length: 10},
	}
	got := mergeFetchRanges(ranges, 0)
	require.Equal(t, []PackfileFetchRange{{Offset: 100, Length: 20}}, got)
}

func TestMergeFetchRangesGapWithinThresholdBridged(t *testing.T) {
	ranges := []PackfileFetchRange{
		{Offset: 0, Length: 10},
		{Offset: 20, Length: 10},
	}
	got := mergeFetchRanges(ranges, 10)
	require.Equal(t, []PackfileFetchRange{{Offset: 0, Length: 30}}, got)
}

func TestMergeFetchRangesGapBeyondThresholdNotBridged(t *testing.T) {
	ranges := []PackfileFetchRange{
		{Offset: 0, Length: 10},
		{Offset: 21, Length: 10},
	}
	got := mergeFetchRanges(ranges, 10)
	require.Equal(t, []PackfileFetchRange{
		{Offset: 0, Length: 10},
		{Offset: 21, Length: 10},
	}, got)
}

func TestMergeFetchRangesUnsortedInput(t *testing.T) {
	ranges := []PackfileFetchRange{
		{Offset: 100, Length: 10},
		{Offset: 0, Length: 10},
	}
	got := mergeFetchRanges(ranges, 0)
	require.Equal(t, []PackfileFetchRange{
		{Offset: 0, Length: 10},
		{Offset: 100, Length: 10},
	}, got)
}

func TestMergeFetchRangesOverlappingKeepsMaxEnd(t *testing.T) {
	ranges := []PackfileFetchRange{
		{Offset: 0, Length: 20},
		{Offset: 5, Length: 5},
	}
	got := mergeFetchRanges(ranges, 0)
	require.Equal(t, []PackfileFetchRange{{Offset: 0, Length: 20}}, got)
}

func TestPackfileSpanCacheLookupMiss(t *testing.T) {
	c := newPackfileSpanCache()
	_, ok := c.lookup(objects.MAC{1}, 0, 10)
	require.False(t, ok)
}

func TestPackfileSpanCacheLookupHit(t *testing.T) {
	c := newPackfileSpanCache()
	mac := objects.MAC{1}
	c.store(mac, 100, []byte("0123456789"))

	data, ok := c.lookup(mac, 102, 4)
	require.True(t, ok)
	require.Equal(t, []byte("2345"), data)
}

func TestPackfileSpanCacheLookupPartialCoverageMiss(t *testing.T) {
	c := newPackfileSpanCache()
	mac := objects.MAC{1}
	c.store(mac, 100, []byte("0123456789"))

	_, ok := c.lookup(mac, 105, 10)
	require.False(t, ok)
}

func TestPackfileSpanCacheLookupWrongPackfileMiss(t *testing.T) {
	c := newPackfileSpanCache()
	c.store(objects.MAC{1}, 0, []byte("0123456789"))

	_, ok := c.lookup(objects.MAC{2}, 0, 5)
	require.False(t, ok)
}
