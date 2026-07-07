// Package protocol implements RESP (the REdis Serialization Protocol, version 2)
// — the wire format kyria speaks over TCP. It is pure: values encode to and
// decode from bytes, with no sockets. The server package wraps a net.Conn and
// calls into here.
package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP2 tags every value with a leading byte, and ends every line with CRLF
// ("\r\n"):
//
//	'+' simple string  "+OK\r\n"
//	'-' error          "-ERR bad\r\n"
//	':' integer        ":42\r\n"
//	'$' bulk string    "$5\r\nhello\r\n"     (binary-safe; "$-1\r\n" is null)
//	'*' array          "*2\r\n<elem><elem>"  (           "*-1\r\n" is null)
//
// A client sends a command as an array of bulk strings — SET foo bar becomes
// "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".
//
// Spec/tests: resp_test.go.  Run: go test ./internal/protocol/
// Full grammar: https://redis.io/docs/latest/develop/reference/protocol-spec/

// TODO(consts): name the five type bytes above (e.g. typeSimpleString = '+', …).
// A named const for CRLF ("\r\n") helps too.
const (
	typeSimpleString = '+'
	typeError        = '-'
	typeInteger      = ':'
	typeBulkString   = '$'
	typeArray        = '*'
	crlf             = "\r\n"
)

// TODO(Value): one struct that can hold any of the five RESP types; which fields
// matter depends on typ. Suggested fields:
//
//	typ     byte    // one of the type bytes
//	str     string  // simple-string / error text
//	integer int64   // integer value
//	bulk    []byte  // bulk-string bytes; nil means a NULL bulk (distinct from empty)
//	array   []Value // array elements; nil means a NULL array
//
// Keeping bulk/array nil-for-null lets one struct represent "$-1" vs "$0" cleanly.
type Value struct {
	typeTag byte
	str     string
	integer int64
	bulk    []byte
	array   []Value
}

// TODO(constructors): small helpers so callers build values without poking fields
// — the server uses these for replies. Signatures:
//
//	SimpleString(s string) Value      // typ '+'
//	Error(msg string) Value           // typ '-'
//	Integer(n int64) Value            // typ ':'
//	BulkString(b []byte) Value        // typ '$'  (pass nil for a null bulk)
//	Array(elems ...Value) Value       // typ '*'
func SimpleString(s string) Value {
	return Value{
		typeTag: typeSimpleString,
		str:     s,
	}
}

func Error(msg string) Value {
	return Value{
		typeTag: typeError,
		str:     msg,
	}
}

func Integer(i int64) Value {
	return Value{
		typeTag: typeInteger,
		integer: i,
	}
}

func BulkString(b []byte) Value {
	return Value{
		typeTag: typeBulkString,
		bulk:    b,
	}
}

func Array(elems ...Value) Value {
	return Value{
		typeTag: typeArray,
		array:   elems,
	}
}

// Decode reads exactly one RESP value from r and returns it; arrays recurse.
// Malformed input — an unknown tag, a non-numeric length, or a stream that ends
// mid-value — returns an error.
func Decode(r *bufio.Reader) (Value, error) {
	typeTag, err := r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	switch typeTag {
	case typeSimpleString:
		return SimpleString(string(line)), nil
	case typeError:
		return Error(string(line)), nil
	case typeInteger:
		parsedInt, err := parseInt(line)
		if err != nil {
			return Value{}, err
		}
		return Integer(parsedInt), nil
	case typeBulkString:
		lineLen, err := parseInt(line)
		if err != nil {
			return Value{}, err
		}
		if lineLen < 0 {
			return BulkString(nil), nil
		}
		if lineLen > maxBulkSize {
			return Value{}, protoErrorf("bulk too large: %d bytes", lineLen)
		}
		buf := make([]byte, lineLen)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return Value{}, err
		}

		if _, err := r.Discard(len(crlf)); err != nil { // drop the trailing CRLF
			return Value{}, err
		}

		return BulkString(buf), nil
	case typeArray:
		lineLen, err := parseInt(line)
		if err != nil {
			return Value{}, err
		}
		if lineLen < 0 {
			return Value{typeTag: typeArray}, nil
		}
		if lineLen > maxArrayCount {
			return Value{}, protoErrorf("array too large: %d elements", lineLen)
		}

		valueArray := make([]Value, lineLen)
		for i := range lineLen {
			value, err := Decode(r)
			if err != nil {
				return Value{}, err
			}
			valueArray[i] = value
		}
		return Array(valueArray...), nil
	default:
		return Value{}, protoErrorf("unknown type byte %q", typeTag)
	}
}

// readLine reads through the next CRLF and returns the line without the trailing
// "\r\n". The bytes point into r's internal buffer and are only valid until the
// next read, so Decode parses or copies them immediately — that's what avoids
// allocating a string for every header line.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, protoErrorf("line missing CRLF terminator")
	}
	return line[:len(line)-2], nil
}

// parseInt reads a base-10 signed integer from b without allocating — strconv
// only parses strings, which would force a []byte→string copy on every length
// prefix. Used for ':' integers and for bulk/array length prefixes.
func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, protoErrorf("empty integer")
	}
	digits, neg := b, false
	if b[0] == '-' {
		digits, neg = b[1:], true
		if len(digits) == 0 {
			return 0, protoErrorf("invalid integer %q", b)
		}
	}
	var n int64
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, protoErrorf("invalid integer %q", b)
		}
		n = n*10 + int64(c-'0')
	}
	if neg {
		n = -n
	}
	return n, nil
}

// Limits on declared sizes. A network server can't trust these length/count
// prefixes, so we reject absurd ones before allocating for them.
const (
	maxBulkSize   = 512 << 20 // 512 MiB, matching Redis's default proto-max-bulk-len
	maxArrayCount = 1 << 20   // 1,048,576 elements
)

// ProtocolError reports input that does not conform to RESP. It is a distinct
// type from the transport errors (io.EOF, a dropped connection) that Decode also
// returns, so the server can tell them apart with errors.As — replying to the
// client on a ProtocolError, but just closing the connection on a transport error.
type ProtocolError struct {
	msg string
}

func (e *ProtocolError) Error() string {
	return "protocol: " + e.msg
}

// protoErrorf builds a *ProtocolError with a formatted message.
func protoErrorf(format string, args ...any) *ProtocolError {
	return &ProtocolError{msg: fmt.Sprintf(format, args...)}
}

// Encode writes v to w in RESP2 wire form. The server wraps its connection in a
// bufio.Writer, so the small writes here are batched into one flush.
func (v Value) Encode(w io.Writer) error {
	ew := MemoizedWriter{w: w}
	v.encode(&ew)
	return ew.err
}

// encode writes v through ew, recursing for array elements. ew swallows further
// writes once one has failed, so the caller checks ew.err just once.
func (v Value) encode(ew *MemoizedWriter) {
	switch v.typeTag {
	case typeSimpleString, typeError:
		ew.writeByte(v.typeTag)
		ew.writeString(v.str)
		ew.writeString(crlf)
	case typeInteger:
		ew.writeByte(typeInteger)
		ew.writeInt(v.integer)
		ew.writeString(crlf)
	case typeBulkString:
		if v.bulk == nil { // null bulk
			ew.writeString("$-1" + crlf)
			return
		}
		ew.writeByte(typeBulkString)
		ew.writeInt(int64(len(v.bulk)))
		ew.writeString(crlf)
		ew.writeBytes(v.bulk)
		ew.writeString(crlf)
	case typeArray:
		if v.array == nil { // null array
			ew.writeString("*-1" + crlf)
			return
		}
		ew.writeByte(typeArray)
		ew.writeInt(int64(len(v.array)))
		ew.writeString(crlf)
		for i := range v.array {
			v.array[i].encode(ew)
		}
	default:
		ew.err = protoErrorf("cannot encode unknown type %q", v.typeTag)
	}
}

// MemoizedWriter batches writes to an io.Writer, remembering the first error so the
// caller checks once instead of after every write (Rob Pike, "Errors are
// values"). Its scratch buffer formats the tag byte and integers without
// allocating.
type MemoizedWriter struct {
	w             io.Writer
	err           error
	reusableArray [20]byte // room for a tag byte or a base-10 int64
}

func (ew *MemoizedWriter) writeString(s string) {
	if ew.err == nil {
		_, ew.err = io.WriteString(ew.w, s)
	}
}

func (ew *MemoizedWriter) writeBytes(b []byte) {
	if ew.err == nil {
		_, ew.err = ew.w.Write(b)
	}
}

func (ew *MemoizedWriter) writeByte(c byte) {
	ew.reusableArray[0] = c
	ew.writeBytes(ew.reusableArray[:1])
}

func (ew *MemoizedWriter) writeInt(n int64) {
	ew.writeBytes(strconv.AppendInt(ew.reusableArray[:0], n, 10))
}
