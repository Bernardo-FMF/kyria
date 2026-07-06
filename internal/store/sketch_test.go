package store

import "testing"

// The sketch hashes keys, so exact cell values depend on a random seed and on
// collisions. These tests therefore assert the sketch's INVARIANTS — which hold
// regardless of hashing — rather than exact counts.

// TestSketch_EmptyIsZero: a fresh sketch has seen nothing, so every estimate is 0.
func TestSketch_EmptyIsZero(t *testing.T) {
	s := NewCountMinSketch(100)
	if got := s.estimate("anything"); got != 0 {
		t.Errorf("estimate on empty sketch = %d, want 0", got)
	}
}

// TestSketch_NeverUndercounts: after adding a key k times (k below the saturation
// cap), the estimate is at least k. Collisions may push it higher, but a count-min
// sketch never reports less than the truth.
func TestSketch_NeverUndercounts(t *testing.T) {
	s := NewCountMinSketch(100)
	const k = 10
	for i := 0; i < k; i++ {
		s.add("x")
	}
	if got := s.estimate("x"); got < k {
		t.Errorf("estimate after %d adds = %d, want >= %d (never undercount)", k, got, k)
	}
}

// TestSketch_SaturatesAtMax: counters cap at counterMax, so a heavily-added key
// estimates exactly counterMax and never overflows.
func TestSketch_SaturatesAtMax(t *testing.T) {
	s := NewCountMinSketch(100)
	for i := 0; i < 100; i++ {
		s.add("x")
	}
	if got := s.estimate("x"); got != counterMax {
		t.Errorf("estimate after saturation = %d, want %d", got, counterMax)
	}
}

// TestSketch_ResetHalvesSaturatedCounters: after saturating a key (so all its
// cells are exactly counterMax, independent of collisions), reset halves every
// cell — the estimate becomes counterMax/2 exactly. This is aging in miniature.
func TestSketch_ResetHalvesSaturatedCounters(t *testing.T) {
	s := NewCountMinSketch(100)
	for i := 0; i < 2*counterMax; i++ {
		s.add("x")
	}
	if got := s.estimate("x"); got != counterMax {
		t.Fatalf("pre-reset estimate = %d, want %d (saturated)", got, counterMax)
	}

	s.reset()

	if got, want := s.estimate("x"), uint8(counterMax)>>1; got != want {
		t.Errorf("post-reset estimate = %d, want %d (halved)", got, want)
	}
}

// TestSketch_AutoResetsAfterSampleSize: add() ages automatically once size hits
// sampleSize. Saturate a key (cells at counterMax), then let the increments cross
// the threshold; the automatic reset halves it to counterMax/2.
func TestSketch_AutoResetsAfterSampleSize(t *testing.T) {
	s := NewCountMinSketch(10) // sampleSize is well above counterMax, so "x" saturates first
	for i := 0; i < s.sampleSize; i++ {
		s.add("x")
	}
	if got, want := s.estimate("x"), uint8(counterMax)>>1; got != want {
		t.Errorf("estimate after auto-reset = %d, want %d", got, want)
	}
}
