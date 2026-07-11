package binenc

import (
	"bytes"
	"errors"
	"testing"
)

// TestRoundTrip: values written by the Put* helpers read back identically, in order,
// with the offset threading through correctly.
func TestRoundTrip(t *testing.T) {
	buf := new(bytes.Buffer)
	PutUint16(buf, 0x0102)
	PutUint32(buf, 0x03040506)
	PutUint64(buf, 0x0708090a0b0c0d0e)
	if err := PutString(buf, "kyria"); err != nil {
		t.Fatalf("PutString: %v", err)
	}
	buf.Write([]byte{0xde, 0xad, 0xbe, 0xef}) // raw bytes for the Bytes helper

	data := buf.Bytes()
	off := 0

	u16, off, err := Uint16(data, off)
	if err != nil || u16 != 0x0102 {
		t.Fatalf("Uint16 = %#x, %v", u16, err)
	}
	u32, off, err := Uint32(data, off)
	if err != nil || u32 != 0x03040506 {
		t.Fatalf("Uint32 = %#x, %v", u32, err)
	}
	u64, off, err := Uint64(data, off)
	if err != nil || u64 != 0x0708090a0b0c0d0e {
		t.Fatalf("Uint64 = %#x, %v", u64, err)
	}
	s, off, err := String(data, off)
	if err != nil || s != "kyria" {
		t.Fatalf("String = %q, %v", s, err)
	}
	b, off, err := Bytes(data, off, 4)
	if err != nil || !bytes.Equal(b, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("Bytes = %#x, %v", b, err)
	}
	if off != len(data) {
		t.Errorf("final offset = %d, want %d (consumed everything)", off, len(data))
	}
}

// TestTruncated: every proper prefix of a full buffer makes at least one decode fail
// with ErrMalformed rather than panicking.
func TestTruncated(t *testing.T) {
	buf := new(bytes.Buffer)
	PutUint32(buf, 0x11223344)
	_ = PutString(buf, "abc")
	full := buf.Bytes()

	for i := 0; i < len(full); i++ {
		short := full[:i]
		_, off, err := Uint32(short, 0)
		if err != nil {
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("prefix %d: Uint32 err = %v, want ErrMalformed", i, err)
			}
			continue
		}
		if _, _, err := String(short, off); !errors.Is(err, ErrMalformed) {
			t.Errorf("prefix %d: String err = %v, want ErrMalformed", i, err)
		}
	}
}

// TestBytes_Guards: a negative or over-long length is rejected, not panicked on.
func TestBytes_Guards(t *testing.T) {
	data := []byte{1, 2, 3}
	if _, _, err := Bytes(data, 0, 5); !errors.Is(err, ErrMalformed) {
		t.Errorf("Bytes over-long = %v, want ErrMalformed", err)
	}
	if _, _, err := Bytes(data, 0, -1); !errors.Is(err, ErrMalformed) {
		t.Errorf("Bytes negative n = %v, want ErrMalformed", err)
	}
}
