package server

import (
	"reflect"
	"testing"
)

// TestBucketsCodec_RoundTrip: a bucket set survives encode→decode, order preserved.
func TestBucketsCodec_RoundTrip(t *testing.T) {
	buckets := []int{0, 3, 17, 1023}

	got, err := decodeBuckets(encodeBuckets(buckets))
	if err != nil {
		t.Fatalf("decodeBuckets(encodeBuckets()): %v", err)
	}
	if !reflect.DeepEqual(got, buckets) {
		t.Errorf("round-trip = %v, want %v", got, buckets)
	}
}

// TestBucketsCodec_Empty: an empty bucket set round-trips to an empty slice.
func TestBucketsCodec_Empty(t *testing.T) {
	got, err := decodeBuckets(encodeBuckets(nil))
	if err != nil {
		t.Fatalf("decodeBuckets(encodeBuckets(nil)): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("round-trip of empty = %v, want no buckets", got)
	}
}

// TestBucketsDecode_Truncated: every proper prefix of a valid encoding is rejected.
func TestBucketsDecode_Truncated(t *testing.T) {
	full := encodeBuckets([]int{1, 2, 3})
	for i := 1; i < len(full); i++ {
		if _, err := decodeBuckets(full[:i]); err == nil {
			t.Errorf("decodeBuckets of a truncated blob (%d of %d bytes) = nil error, want an error", i, len(full))
		}
	}
}

// TestEntriesCodec_RoundTrip: (key, blob) pairs survive encode→decode, including a binary
// blob and an empty value.
func TestEntriesCodec_RoundTrip(t *testing.T) {
	entries := map[string][]byte{
		"a":     []byte("1"),
		"key-2": {0x00, 0xff, 0x10},
		"empty": {},
	}

	got, err := decodeEntries(encodeEntries(entries))
	if err != nil {
		t.Fatalf("decodeEntries(encodeEntries()): %v", err)
	}
	if !reflect.DeepEqual(got, entries) {
		t.Errorf("round-trip = %#v, want %#v", got, entries)
	}
}

// TestEntriesCodec_Empty: no entries round-trips to an empty (non-nil) map.
func TestEntriesCodec_Empty(t *testing.T) {
	got, err := decodeEntries(encodeEntries(map[string][]byte{}))
	if err != nil {
		t.Fatalf("decodeEntries(encodeEntries(empty)): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("round-trip of empty = %v, want no entries", got)
	}
}

// TestEntriesDecode_Truncated: every proper prefix of a valid encoding is rejected.
func TestEntriesDecode_Truncated(t *testing.T) {
	full := encodeEntries(map[string][]byte{"k": []byte("v")})
	for i := 1; i < len(full); i++ {
		if _, err := decodeEntries(full[:i]); err == nil {
			t.Errorf("decodeEntries of a truncated blob (%d of %d bytes) = nil error, want an error", i, len(full))
		}
	}
}
