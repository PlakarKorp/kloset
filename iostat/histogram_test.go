package iostat

import (
	"math"
	"testing"
)

// histBucketIndex must file every value inside its bucket's nominal bounds
// even a few ulps around a boundary, where Log2/Exp2 rounding disagree.
func TestHistogramBucketIndexInvariant(t *testing.T) {
	for k := 1; k < histNumBuckets; k++ {
		b := histBucketLower(k)
		for _, v := range []float64{math.Nextafter(b, 0), b, math.Nextafter(b, math.Inf(1))} {
			i := histBucketIndex(v)
			if histBucketLower(i) > v {
				t.Fatalf("v=%v filed in bucket %d with lower bound %v > v", v, i, histBucketLower(i))
			}
			if i+1 < histNumBuckets && v >= histBucketLower(i+1) {
				t.Fatalf("v=%v filed in bucket %d with upper bound %v <= v", v, i, histBucketLower(i+1))
			}
		}
	}
}

// A dense uniform distribution interpolates almost exactly (the ~9% bucket
// width only bounds the worst case), so 2% is tight enough to catch a broken
// interpolation, not just a broken clamp.
func TestHistogramPercentiles(t *testing.T) {
	var h histogram
	for i := 1; i <= 1000; i++ {
		h.observe(float64(i))
	}

	if h.count != 1000 {
		t.Errorf("expected count 1000, got %d", h.count)
	}
	if h.min != 1 || h.max != 1000 {
		t.Errorf("expected exact min/max 1/1000, got %f/%f", h.min, h.max)
	}
	if avg := h.avg(); avg != 500.5 {
		t.Errorf("expected exact avg 500.5, got %f", avg)
	}

	for p, want := range map[float64]float64{
		50: 500, 75: 750, 90: 900, 95: 950, 99: 990,
	} {
		got := h.percentile(p)
		if math.Abs(got-want)/want > 0.02 {
			t.Errorf("p%v: got %f, want %f ±2%%", p, got, want)
		}
	}
}

func TestHistogramPercentileEdgeCases(t *testing.T) {
	var h histogram
	if v := h.percentile(50); v != 0 {
		t.Errorf("expected 0 for empty histogram, got %f", v)
	}

	for _, v := range []float64{1.0, 2.0, 3.0} {
		h.observe(v)
	}
	if v := h.percentile(0); v != 1.0 {
		t.Errorf("expected the exact minimum for p=0, got %f", v)
	}
	if v := h.percentile(100); v != 3.0 {
		t.Errorf("expected the exact maximum for p=100, got %f", v)
	}
	if v := h.percentile(50); v < 1.0 || v > 3.0 {
		t.Errorf("expected p=50 within observed range, got %f", v)
	}
}

// The first bucket is open-ended below: a distribution entirely below the
// base unit must still interpolate over the observed range instead of
// collapsing every percentile onto the extremes.
func TestHistogramSubUnitDistribution(t *testing.T) {
	var h histogram
	for i := 1; i <= 900; i++ {
		h.observe(float64(i) / 1000) // uniform over 0.001..0.9, all in bucket 0
	}

	for p, want := range map[float64]float64{
		25: 0.225, 50: 0.45, 90: 0.81,
	} {
		got := h.percentile(p)
		if math.Abs(got-want)/want > 0.05 {
			t.Errorf("p%v: got %f, want %f ±5%%", p, got, want)
		}
	}
}

// The last bucket is symmetric: values beyond the nominal range interpolate
// over [bucket lower, observed max] without inverting the bounds.
func TestHistogramOverflowDistribution(t *testing.T) {
	var h histogram
	lo, hi := math.Exp2(61), math.Exp2(65) // both beyond the last bucket bound
	h.observe(lo)
	h.observe(hi)

	if p := h.percentile(50); p < lo || p > hi {
		t.Errorf("p50 %f outside observed range [%f, %f]", p, lo, hi)
	}
	if p99, p50 := h.percentile(99), h.percentile(50); p99 < p50 {
		t.Errorf("p99 %f below p50 %f", p99, p50)
	}
}

func TestHistogramBucketIndexNonFinite(t *testing.T) {
	cases := map[string]struct {
		v    float64
		want int
	}{
		"NaN":      {math.NaN(), 0},
		"+Inf":     {math.Inf(1), histNumBuckets - 1},
		"-Inf":     {math.Inf(-1), 0},
		"negative": {-42, 0},
		"zero":     {0, 0},
	}
	for name, c := range cases {
		if got := histBucketIndex(c.v); got != c.want {
			t.Errorf("%s: got bucket %d, want %d", name, got, c.want)
		}
	}
}

func TestHistogramExtremes(t *testing.T) {
	var h histogram
	h.observe(0)             // below the first bucket
	h.observe(0.25)          // below the first bucket
	h.observe(math.Exp2(70)) // beyond the last bucket

	if h.count != 3 {
		t.Errorf("expected count 3, got %d", h.count)
	}
	if h.min != 0 {
		t.Errorf("expected min 0, got %f", h.min)
	}
	if h.max != math.Exp2(70) {
		t.Errorf("expected max 2^70, got %f", h.max)
	}
	if p := h.percentile(99); p > h.max {
		t.Errorf("p99 %f exceeds max %f", p, h.max)
	}
}

func TestHistogramNegativeOnly(t *testing.T) {
	var h histogram
	h.observe(-5)
	h.observe(-2)

	if h.min != -5 || h.max != -2 {
		t.Errorf("expected exact min/max -5/-2, got %f/%f", h.min, h.max)
	}
	if p := h.percentile(50); p < -5 || p > -2 {
		t.Errorf("p50 %f outside observed range [-5, -2]", p)
	}
}

// Non-finite values would poison min/max/sum until reset; observe drops
// them.
func TestHistogramObserveNonFinite(t *testing.T) {
	var h histogram
	h.observe(math.NaN())
	h.observe(math.Inf(1))
	h.observe(math.Inf(-1))
	if h.count != 0 {
		t.Errorf("expected non-finite values to be dropped, count %d", h.count)
	}

	h.observe(42)
	if h.count != 1 || h.min != 42 || h.max != 42 || h.sum != 42 {
		t.Errorf("expected stats unpoisoned after non-finite values, got %+v", h)
	}
}

func TestHistogramReset(t *testing.T) {
	var h histogram
	h.observe(42)
	h.reset()

	if h.count != 0 || h.sum != 0 || h.percentile(50) != 0 {
		t.Errorf("expected empty histogram after reset, got %+v", h)
	}
}
