package repository

import (
	"math/rand/v2"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

func bl(pf byte, offset uint64, length uint32) blobLocation {
	return blobLocation{
		BlobReq{Type: resources.RT_CHUNK, MAC: objects.MAC{pf, byte(offset), byte(offset >> 8)}},
		state.Location{Packfile: objects.MAC{pf}, Offset: offset, Length: length},
	}
}

func TestPlanReadsAdjacentMerge(t *testing.T) {
	// Three contiguous blobs coalesce into a single zero-waste read.
	plans := readPlanner([]blobLocation{
		bl(1, 200, 100),
		bl(1, 0, 100),
		bl(1, 100, 100),
	}, maxSize, 0.3)

	require.Len(t, plans, 1)
	require.Equal(t, uint64(0), plans[0].loc.Offset)
	require.Equal(t, uint32(300), plans[0].loc.Length)
	require.Len(t, plans[0].blobs, 3)
}

func TestPlanReadsPackfileBoundary(t *testing.T) {
	// Contiguous offsets in different packfiles never merge.
	plans := readPlanner([]blobLocation{
		bl(1, 0, 100),
		bl(2, 100, 100),
	}, maxSize, 0.3)

	require.Len(t, plans, 2)
}

func TestPlanReadsMaxReadSize(t *testing.T) {
	// Merging stops when the merged read would exceed maxReadSize, and
	// a single blob larger than maxReadSize still gets its own read.
	plans := readPlanner([]blobLocation{
		bl(1, 0, 600),
		bl(1, 600, 600),
		bl(1, 1200, 2000),
	}, 1000, 0.3)

	require.Len(t, plans, 3)
	require.Equal(t, uint32(2000), plans[2].loc.Length)
}

// checkPlan asserts the invariants any correct plan must satisfy, whatever
// the merge policy in force:
//
//  1. every input blob lands in exactly one plan (none dropped, none doubled);
//  2. a plan only contains blobs of its own packfile;
//  3. a plan's range covers every blob assigned to it, entirely;
//  4. a plan's range is tight: it starts at its first blob and ends at the
//     end of its outermost blob — gaps between blobs are fetched, slack
//     beyond them is not;
//  5. a multi-blob plan never exceeds maxReadSize (a single blob larger
//     than maxReadSize is allowed a plan of its own).
func checkPlan(t *testing.T, input []blobLocation, plans []*rangeRead, maxReadSize uint32) {
	t.Helper()

	seen := 0
	for i, p := range plans {
		require.NotEmpty(t, p.blobs, "plan %d contains no blobs", i)

		planEnd := p.loc.Offset + uint64(p.loc.Length)
		minOff, maxEnd := p.blobs[0].Offset, uint64(0)
		for _, b := range p.blobs {
			blobEnd := b.Offset + uint64(b.Length)
			require.Equal(t, p.loc.Packfile, b.Packfile,
				"plan %d: blob %x belongs to another packfile", i, b.MAC)
			require.GreaterOrEqual(t, b.Offset, p.loc.Offset,
				"plan %d: blob %x starts before the plan's range", i, b.MAC)
			require.LessOrEqual(t, blobEnd, planEnd,
				"plan %d: blob %x overruns the plan's range", i, b.MAC)
			minOff = min(minOff, b.Offset)
			maxEnd = max(maxEnd, blobEnd)
			seen++
		}

		require.Equal(t, minOff, p.loc.Offset,
			"plan %d fetches slack before its first blob", i)
		require.Equal(t, maxEnd, planEnd,
			"plan %d fetches slack past its outermost blob", i)

		if len(p.blobs) > 1 {
			require.LessOrEqual(t, p.loc.Length, maxReadSize,
				"plan %d: merged read exceeds maxReadSize", i)
		}
	}
	require.Equal(t, len(input), seen,
		"every input blob must appear in exactly one plan")
}

// checkMaximal asserts that no two consecutive same-packfile plans could
// have been merged: their combined span must exceed maxReadSize. Unlike
// checkPlan this is specific to the everything-permitted policy
// (maxWaste 1.0) — with a real waste policy a split can be legitimate
// below the size cap. It catches the failure mode checkPlan cannot:
// a planner drifting back toward one read per blob would produce
// perfectly valid, tiny plans, and defeat the whole point.
//
// It assumes plans come out sorted by (packfile, offset), which the
// sequential greedy emission over sorted input guarantees.
func checkMaximal(t *testing.T, plans []*rangeRead, maxReadSize uint32) {
	t.Helper()

	for i := 1; i < len(plans); i++ {
		prev, cur := plans[i-1], plans[i]
		if prev.loc.Packfile != cur.loc.Packfile {
			continue
		}
		span := cur.loc.Offset + uint64(cur.loc.Length) - prev.loc.Offset
		require.Greater(t, span, uint64(maxReadSize),
			"plans %d and %d span %d bytes together and should have been merged",
			i-1, i, span)
	}
}

func TestPlanReadsGapCountsTowardMaxReadSize(t *testing.T) {
	// Two 100-byte blobs 800 bytes apart: the merged READ would be
	// 1000 bytes even though only 200 are wanted. With maxReadSize
	// 500 they must not merge — the cap bounds the fetched span,
	// gaps included, not the sum of blob lengths.
	input := []blobLocation{
		bl(1, 0, 100),
		bl(1, 900, 100),
	}
	plans := readPlanner(input, 500, 1.0)

	require.Len(t, plans, 2)
	checkPlan(t, input, plans, 500)
}

func TestPlanReadsGapExtendsLength(t *testing.T) {
	// Two blobs with a 100-byte gap, everything permitted: one plan,
	// whose length spans the gap (300), not the sum of blob lengths
	// (200) — otherwise the read misses the second blob's tail.
	input := []blobLocation{
		bl(1, 0, 100),
		bl(1, 200, 100),
	}
	plans := readPlanner(input, maxSize, 1.0)

	require.Len(t, plans, 1)
	require.Equal(t, uint64(0), plans[0].loc.Offset)
	require.Equal(t, uint32(300), plans[0].loc.Length)
	checkPlan(t, input, plans, maxSize)
}

func TestPlanReadsMaxReadSizeExact(t *testing.T) {
	// A merge landing exactly on maxReadSize is allowed: the cap is
	// inclusive ("reads up to maxReadSize"), not exclusive.
	input := []blobLocation{
		bl(1, 0, 400),
		bl(1, 600, 400),
	}
	plans := readPlanner(input, 1000, 1.0)

	require.Len(t, plans, 1)
	require.Equal(t, uint32(1000), plans[0].loc.Length)
	checkPlan(t, input, plans, 1000)
}

func TestPlanReadsOversizedBlobDoesNotBlockNeighbours(t *testing.T) {
	// A blob larger than maxReadSize gets a plan of its own, and the
	// blobs after it still merge with each other.
	input := []blobLocation{
		bl(1, 0, 2000),
		bl(1, 2000, 100),
		bl(1, 2100, 100),
	}
	plans := readPlanner(input, 1000, 1.0)

	require.Len(t, plans, 2)
	require.Equal(t, uint32(2000), plans[0].loc.Length)
	require.Equal(t, uint32(200), plans[1].loc.Length)
	checkPlan(t, input, plans, 1000)
}

func TestPlanReadsInterleavedPackfiles(t *testing.T) {
	// Unsorted input alternating between two packfiles: blobs regroup
	// per packfile and merge within it.
	input := []blobLocation{
		bl(2, 100, 100),
		bl(1, 100, 100),
		bl(2, 0, 100),
		bl(1, 0, 100),
	}
	plans := readPlanner(input, maxSize, 1.0)

	require.Len(t, plans, 2)
	for _, p := range plans {
		require.Equal(t, uint64(0), p.loc.Offset)
		require.Equal(t, uint32(200), p.loc.Length)
	}
	checkPlan(t, input, plans, maxSize)
}

func TestPlanReadsSingle(t *testing.T) {
	input := []blobLocation{bl(1, 42, 100)}
	plans := readPlanner(input, maxSize, 1.0)

	require.Len(t, plans, 1)
	require.Equal(t, uint64(42), plans[0].loc.Offset)
	require.Equal(t, uint32(100), plans[0].loc.Length)
	checkPlan(t, input, plans, maxSize)
}

func TestPlanReadsStress(t *testing.T) {
	// Deterministic pseudo-random batches laid out the way blobs
	// really sit in packfiles — disjoint ranges, sometimes adjacent,
	// sometimes gapped, occasionally oversized — checked purely
	// against the checkPlan invariants. Policy-agnostic on purpose:
	// this must keep passing unchanged once the waste policy lands.
	rng := rand.New(rand.NewPCG(0x9E3779B9, 0x7F4A7C15))

	for round := range 20 {
		maxReadSize := uint32(1024 + rng.IntN(8192))

		input := make([]blobLocation, 0, 512)
		for pf := byte(1); pf <= 3; pf++ {
			var offset uint64
			for range 150 + rng.IntN(50) {
				if rng.IntN(3) > 0 { // 1 in 3 blobs is adjacent to the previous one
					offset += uint64(rng.IntN(3000))
				}
				length := uint32(1 + rng.IntN(2000))
				if rng.IntN(50) == 0 { // occasional oversized blob
					length = maxReadSize + uint32(rng.IntN(1000))
				}
				input = append(input, bl(pf, offset, length))
				offset += uint64(length)
			}
		}
		rng.Shuffle(len(input), func(i, j int) {
			input[i], input[j] = input[j], input[i]
		})

		plans := readPlanner(input, maxReadSize, 1.0)
		if t.Failed() {
			return
		}
		t.Logf("round %d: maxReadSize=%d, %d blobs -> %d plans",
			round, maxReadSize, len(input), len(plans))
		checkPlan(t, input, plans, maxReadSize)
		checkMaximal(t, plans, maxReadSize)
	}
}

// BenchmarkReadPlanner runs at the scale of the measured no-change
// kernel-backup log (~162k blobs over 20 packfiles): planning must stay
// trivial next to a single ~80ms store round trip.
func BenchmarkReadPlanner(b *testing.B) {
	rng := rand.New(rand.NewPCG(0xDEADBEEF, 0xCAFEBABE))

	input := make([]blobLocation, 0, 162_000)
	for pf := byte(1); pf <= 20; pf++ {
		var offset uint64
		for range 8_100 {
			if rng.IntN(3) > 0 {
				offset += uint64(rng.IntN(3000))
			}
			length := uint32(1 + rng.IntN(2000))
			input = append(input, bl(pf, offset, length))
			offset += uint64(length)
		}
	}
	rng.Shuffle(len(input), func(i, j int) {
		input[i], input[j] = input[j], input[i]
	})

	scratch := make([]blobLocation, len(input))
	b.ReportAllocs()
	for b.Loop() {
		// readPlanner sorts its input in place; replanning already
		// sorted input would flatter the numbers.
		copy(scratch, input)
		readPlanner(scratch, maxSize, 1.0)
	}
}
