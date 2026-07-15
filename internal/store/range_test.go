package store

import (
	"testing"
	"time"
)

// collectRange runs Range over s and returns the visited entries as a map.
func collectRange(s Store) map[string]string {
	got := map[string]string{}
	s.Range(func(key string, value []byte) {
		got[key] = string(value)
	})
	return got
}

// TestRange_MapStore_VisitsAllLive: Range visits every live entry with its value.
func TestRange_MapStore_VisitsAllLive(t *testing.T) {
	m := New()
	m.Set("a", []byte("1"))
	m.Set("b", []byte("2"))
	m.Set("c", []byte("3"))

	got := collectRange(m)
	want := map[string]string{"a": "1", "b": "2", "c": "3"}

	if len(got) != len(want) {
		t.Fatalf("Range visited %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Range[%q] = %q, want %q", k, got[k], v)
		}
	}
}

// TestRange_MapStore_SkipsExpired: an expired-but-unreaped entry is not visited, matching
// Get's lazy-expiry view (the janitor may not have reaped it yet).
func TestRange_MapStore_SkipsExpired(t *testing.T) {
	m := New()
	m.Set("live", []byte("here"))
	// Plant an already-expired entry directly (white-box).
	m.data["dead"] = entry{value: []byte("gone"), expiresAt: time.Now().Add(-time.Hour)}

	got := collectRange(m)

	if _, ok := got["dead"]; ok {
		t.Error("Range visited an expired entry")
	}
	if got["live"] != "here" {
		t.Errorf("Range missed the live entry, got %v", got)
	}
}

// TestRange_ShardedStore_VisitsAllAcrossShards: Range covers entries spread over shards.
func TestRange_ShardedStore_VisitsAllAcrossShards(t *testing.T) {
	s := NewSharded(8)
	want := map[string]string{}
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"} {
		s.Set(k, []byte(k+"-v"))
		want[k] = k + "-v"
	}

	got := collectRange(s)

	if len(got) != len(want) {
		t.Fatalf("Range visited %d entries, want %d (%v)", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Range[%q] = %q, want %q", k, got[k], v)
		}
	}
}

// TestRange_Empty: Range on an empty store visits nothing.
func TestRange_Empty(t *testing.T) {
	if got := collectRange(New()); len(got) != 0 {
		t.Errorf("Range on empty store visited %v, want nothing", got)
	}
}
