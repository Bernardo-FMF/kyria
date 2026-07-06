package store

import (
	"hash/maphash"
	"math/bits"
)

const (
	sketchDepth = 4  // counter rows; an estimate is the min across the rows
	counterMax  = 15 // per-counter saturation cap (TinyLFU uses 4-bit counters)
)

// countMinSketch estimates how often each key has been seen, in fixed memory and
// without storing the keys. It is the frequency engine behind TinyLFU eviction.
//
// It is a grid of small counters — sketchDepth rows × width columns, held in one
// flat slice. add(key) hashes the key to one column per row and increments those
// cells (saturating at counterMax); estimate(key) returns the MIN of those cells.
// Taking the min is the trick: a collision can only push a cell up, so the
// smallest of a key's cells is the least-corrupted estimate — the sketch may
// over-count but never under-counts. reset() halves every counter, and add()
// calls it automatically once size reaches sampleSize, so stale frequency decays.
//
// This implementation is single-threaded (plain []uint8); concurrent use will
// need atomic counters.
type countMinSketch struct {
	counters   []uint8      // sketchDepth rows × width columns, row-major
	width      int          // columns per row; a power of two, so a hash maps to a column with & mask
	mask       uint64       // width-1, to pick a column with & instead of %
	size       int          // increments since the last reset (drives aging)
	sampleSize int          // once size reaches this, reset halves every counter
	seed       maphash.Seed // one seed; each row's column is derived from it by double hashing
}

// newCountMinSketch returns a sketch sized for roughly capacity distinct keys.
// width is capacity rounded up to a power of two — about one column per expected
// key keeps collisions low without wasting memory.
func newCountMinSketch(capacity int) *countMinSketch {
	if capacity < 1 {
		capacity = 1
	}

	width := nextPowerOfTwo(capacity)

	return &countMinSketch{
		counters:   make([]uint8, sketchDepth*width),
		width:      width,
		mask:       uint64(width - 1),
		sampleSize: 10 * capacity, // reset roughly every 10×capacity increments
		seed:       maphash.MakeSeed(),
	}
}

// column returns the flat counters index of key's cell in the given row, where
// h1/h2 are the two halves of the key's hash. Double hashing (h1 + row*h2) gives
// each row a different column from a single hash; & mask wraps it into [0,width),
// and row*width offsets to the right row's block in the flat slice.
func (s *countMinSketch) column(row int, h1, h2 uint32) int {
	col := (h1 + uint32(row)*h2) & uint32(s.mask)
	return row*s.width + int(col)
}

// add records one occurrence of key: it increments the key's cell in every row
// (saturating at counterMax), then ages the whole sketch once size reaches
// sampleSize.
func (s *countMinSketch) add(key string) {
	h := maphash.String(s.seed, key)
	h1, h2 := uint32(h), uint32(h>>32)

	for row := range sketchDepth {
		i := s.column(row, h1, h2)
		if s.counters[i] < counterMax {
			s.counters[i]++
		}
	}

	s.size++
	if s.size >= s.sampleSize {
		s.reset()
	}
}

// estimate returns key's approximate frequency: the minimum of its per-row cells.
func (s *countMinSketch) estimate(key string) uint8 {
	h := maphash.String(s.seed, key)
	h1, h2 := uint32(h), uint32(h>>32)

	minCount := uint8(counterMax)
	for row := range sketchDepth {
		if c := s.counters[s.column(row, h1, h2)]; c < minCount {
			minCount = c
		}
	}

	return minCount
}

// reset ages the sketch by halving every counter (a right shift) and zeroing the
// increment count, so recent accesses outweigh old ones over time.
func (s *countMinSketch) reset() {
	for i := range s.counters {
		s.counters[i] >>= 1
	}
	s.size = 0
}

// nextPowerOfTwo returns the smallest power of two >= n (and 1 for n <= 1).
func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}
