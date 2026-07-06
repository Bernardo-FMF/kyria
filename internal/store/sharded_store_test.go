package store

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"
)

// TestShardedStore_Basic is a quick single-goroutine sanity check: a
// ShardedStore must behave like any other Store.
func TestShardedStore_Basic(t *testing.T) {
	s := NewSharded(8)

	if _, err := s.Set("k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, ok := s.Get("k")
	if !ok || !bytes.Equal(got, []byte("v")) {
		t.Fatalf(`Get("k") = %q, %v; want "v", true`, got, ok)
	}
	if s.Size() != 1 {
		t.Errorf("Size = %d, want 1", s.Size())
	}
	if !s.Delete("k") {
		t.Errorf(`Delete("k") = false, want true`)
	}
	if s.Size() != 0 {
		t.Errorf("Size after delete = %d, want 0", s.Size())
	}
}

// TestShardedStore_Concurrent hammers the store from many goroutines with
// interleaved Set/Get/Delete. A correct implementation passes cleanly.
//
// To SEE the bug this phase fixes: temporarily change the constructor below to a
// plain MapStore — `var s Store = New()` — and run it. The Go runtime aborts the
// whole test binary with "fatal error: concurrent map writes". Then switch back.
//
// With a C compiler on PATH you can also run it under the race detector for a
// detailed report:  go test ./internal/store/ -run Concurrent -race
func TestShardedStore_Concurrent(t *testing.T) {
	const (
		goroutines = 64
		opsPerG    = 2000
		keySpace   = 512
	)
	s := NewSharded(16)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				key := "key" + strconv.Itoa((g+i)%keySpace)
				if _, err := s.Set(key, []byte("value")); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
				s.Get(key)
				if i%5 == 0 {
					s.Delete(key)
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkShardedStore_Set runs the SAME parallel write workload against stores
// with different shard counts. shards=1 is a single global lock (every goroutine
// serialises); more shards means more operations proceed in parallel. Watch the
// ns/op drop as shards grow:
//
//	go test ./internal/store/ -bench BenchmarkShardedStore_Set -benchmem
func BenchmarkShardedStore_Set(b *testing.B) {
	for _, shards := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			s := NewSharded(shards)
			val := []byte("value")
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					_, _ = s.Set("key"+strconv.Itoa(i%1024), val)
					i++
				}
			})
		})
	}
}
