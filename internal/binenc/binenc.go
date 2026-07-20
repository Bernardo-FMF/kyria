// Package binenc holds the small big-endian binary primitives shared by kyria's
// hand-rolled codecs (gossip packets, versioned-value blobs). Encoders append to a
// *bytes.Buffer; decoders read from a []byte at an offset and return the advanced
// offset, bounds-checking every read so truncated or corrupt input yields
// ErrMalformed rather than a panic.
package binenc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrMalformed is returned by the decode helpers when the input is shorter than a
// declared length - truncated or corrupt data. Callers decoding untrusted input rely
// on this being an error, never a panic.
var ErrMalformed = errors.New("binenc: truncated or malformed data")

// PutUint16 appends v as two big-endian bytes.
func PutUint16(buf *bytes.Buffer, v uint16) {
	var bs [2]byte
	binary.BigEndian.PutUint16(bs[:], v)
	buf.Write(bs[:])
}

// PutUint32 appends v as four big-endian bytes.
func PutUint32(buf *bytes.Buffer, v uint32) {
	var bs [4]byte
	binary.BigEndian.PutUint32(bs[:], v)
	buf.Write(bs[:])
}

// PutUint64 appends v as eight big-endian bytes.
func PutUint64(buf *bytes.Buffer, v uint64) {
	var bs [8]byte
	binary.BigEndian.PutUint64(bs[:], v)
	buf.Write(bs[:])
}

// PutString appends a uint16 length prefix then value's bytes. It errors if value is
// longer than a uint16 can describe.
func PutString(buf *bytes.Buffer, value string) error {
	if len(value) > math.MaxUint16 {
		return fmt.Errorf("binenc: string too long to encode: %d bytes", len(value))
	}
	PutUint16(buf, uint16(len(value)))
	buf.WriteString(value)
	return nil
}

// PutBool appends value as a single byte: 1 for true, 0 for false. Bool decodes it.
func PutBool(buf *bytes.Buffer, value bool) {
	var b byte
	if value {
		b = 1
	}
	buf.WriteByte(b)
}

// Uint16 reads a big-endian uint16 at off, returning it and the advanced offset — or
// ErrMalformed if fewer than 2 bytes remain.
func Uint16(data []byte, off int) (uint16, int, error) {
	if len(data)-off < 2 {
		return 0, off, ErrMalformed
	}
	return binary.BigEndian.Uint16(data[off:]), off + 2, nil
}

// Uint32 reads a big-endian uint32 at off, returning it and the advanced offset — or
// ErrMalformed if fewer than 4 bytes remain.
func Uint32(data []byte, off int) (uint32, int, error) {
	if len(data)-off < 4 {
		return 0, off, ErrMalformed
	}
	return binary.BigEndian.Uint32(data[off:]), off + 4, nil
}

// Uint64 reads a big-endian uint64 at off, returning it and the advanced offset — or
// ErrMalformed if fewer than 8 bytes remain.
func Uint64(data []byte, off int) (uint64, int, error) {
	if len(data)-off < 8 {
		return 0, off, ErrMalformed
	}
	return binary.BigEndian.Uint64(data[off:]), off + 8, nil
}

// String reads a uint16 length prefix then that many bytes, returning the string and
// the advanced offset.
func String(data []byte, off int) (string, int, error) {
	n, off, err := Uint16(data, off)
	if err != nil {
		return "", off, err
	}
	if len(data)-off < int(n) {
		return "", off, ErrMalformed
	}
	return string(data[off : off+int(n)]), off + int(n), nil
}

// Bytes returns a COPY of the n bytes at off and the advanced offset, or ErrMalformed
// if fewer than n bytes remain. The copy means the result does not alias (and so
// cannot be corrupted by later reuse of) the input buffer.
func Bytes(data []byte, off, n int) ([]byte, int, error) {
	if n < 0 || len(data)-off < n {
		return nil, off, ErrMalformed
	}
	b := make([]byte, n)
	copy(b, data[off:off+n])
	return b, off + n, nil
}

// Bool reads a single 0/1 byte at off, returning it as a bool and the advanced
// offset - or ErrMalformed if no bytes remain or the byte isn't 0 or 1.
func Bool(data []byte, off int) (bool, int, error) {
	if len(data)-off < 1 {
		return false, off, ErrMalformed
	}
	b := data[off]
	if b != 0 && b != 1 {
		return false, off, ErrMalformed
	}

	return b == 1, off + 1, nil
}
