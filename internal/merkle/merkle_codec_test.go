package merkle

import (
	"bytes"
	"testing"
)

// TestMerkle_EncodeDecode_RoundTrip: a tree survives Encode→Decode — same leaf count, same
// root, and no diff against the original.
func TestMerkle_EncodeDecode_RoundTrip(t *testing.T) {
	orig := build(64, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})

	got, err := Decode(orig.Encode())
	if err != nil {
		t.Fatalf("Decode(Encode()): %v", err)
	}
	if got.Leaves() != orig.Leaves() {
		t.Errorf("leaf count = %d, want %d", got.Leaves(), orig.Leaves())
	}
	if !bytes.Equal(got.Root(), orig.Root()) {
		t.Error("decoded root differs from the original")
	}
	if d := orig.Diff(got); len(d) != 0 {
		t.Errorf("orig.Diff(decoded) = %v, want none", d)
	}
}

// TestMerkle_EncodeDecode_PreservesDiff: a real difference survives the wire — diffing against
// a decoded peer tree names the same bucket as diffing the peer tree directly.
func TestMerkle_EncodeDecode_PreservesDiff(t *testing.T) {
	local := build(64, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}})
	peer := build(64, [][2]string{{"a", "1"}, {"b", "2"}, {"c", "CHANGED"}})

	wire, err := Decode(peer.Encode())
	if err != nil {
		t.Fatalf("Decode(Encode()): %v", err)
	}

	direct := local.Diff(peer)
	viaWire := local.Diff(wire)
	if len(direct) != 1 || len(viaWire) != 1 || direct[0] != viaWire[0] {
		t.Errorf("diff via wire = %v, direct = %v, want the same single bucket", viaWire, direct)
	}
}

// TestMerkle_EncodeDecode_Empty: an empty tree round-trips to an equal root.
func TestMerkle_EncodeDecode_Empty(t *testing.T) {
	orig := New(16)

	got, err := Decode(orig.Encode())
	if err != nil {
		t.Fatalf("Decode(Encode()): %v", err)
	}
	if got.Leaves() != orig.Leaves() {
		t.Errorf("leaf count = %d, want %d", got.Leaves(), orig.Leaves())
	}
	if !bytes.Equal(got.Root(), orig.Root()) {
		t.Error("empty tree did not round-trip")
	}
}

// TestMerkle_Decode_Truncated: every proper prefix of a valid encoding is rejected, not
// panicked on — defensive parsing of bytes that arrived over the network.
func TestMerkle_Decode_Truncated(t *testing.T) {
	full := build(8, [][2]string{{"a", "1"}, {"b", "2"}}).Encode()

	for i := 1; i < len(full); i++ {
		if _, err := Decode(full[:i]); err == nil {
			t.Errorf("Decode of a truncated blob (%d of %d bytes) = nil error, want an error", i, len(full))
		}
	}
}
