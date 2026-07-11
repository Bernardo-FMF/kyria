package protocol

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
	"testing"
)

// TestAsError: AsError reports a RESP error and its text, and rejects everything else.
func TestAsError(t *testing.T) {
	if msg, ok := Error("ERR nope").AsError(); !ok || msg != "ERR nope" {
		t.Errorf("AsError on an error = (%q, %v), want (\"ERR nope\", true)", msg, ok)
	}
	if _, ok := SimpleString("OK").AsError(); ok {
		t.Error("AsError on +OK = ok true, want false")
	}
	if _, ok := BulkString([]byte("x")).AsError(); ok {
		t.Error("AsError on a bulk string = ok true, want false")
	}
}

// TestAsBulk: AsBulk returns a present value's bytes, and reports a null bulk (a
// GET miss) or a non-bulk reply as absent.
func TestAsBulk(t *testing.T) {
	if b, ok := BulkString([]byte("hello")).AsBulk(); !ok || string(b) != "hello" {
		t.Errorf("AsBulk on a bulk = (%q, %v), want (\"hello\", true)", b, ok)
	}
	if b, ok := BulkString([]byte{}).AsBulk(); !ok || len(b) != 0 {
		t.Errorf("AsBulk on an empty (present) bulk = (%q, %v), want (\"\", true)", b, ok)
	}
	if _, ok := BulkString(nil).AsBulk(); ok {
		t.Error("AsBulk on a null bulk = ok true, want false (a miss)")
	}
	if _, ok := SimpleString("OK").AsBulk(); ok {
		t.Error("AsBulk on +OK = ok true, want false")
	}
}

// TestEncode pins the exact bytes each value type serializes to.
func TestEncode(t *testing.T) {
	tests := []struct {
		name string
		val  Value
		want string
	}{
		{"simple string", SimpleString("OK"), "+OK\r\n"},
		{"error", Error("ERR bad"), "-ERR bad\r\n"},
		{"integer", Integer(42), ":42\r\n"},
		{"negative integer", Integer(-7), ":-7\r\n"},
		{"bulk string", BulkString([]byte("hello")), "$5\r\nhello\r\n"},
		{"empty bulk", BulkString([]byte{}), "$0\r\n\r\n"},
		{"null bulk", BulkString(nil), "$-1\r\n"},
		{
			"array of bulks",
			Array(BulkString([]byte("GET")), BulkString([]byte("foo"))),
			"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tc.val.Encode(&buf); err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if got := buf.String(); got != tc.want {
				t.Errorf("Encode = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDecode pins the value each byte sequence parses into.
func TestDecode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Value
	}{
		{"simple string", "+OK\r\n", SimpleString("OK")},
		{"error", "-ERR bad\r\n", Error("ERR bad")},
		{"integer", ":42\r\n", Integer(42)},
		{"bulk string", "$5\r\nhello\r\n", BulkString([]byte("hello"))},
		{"empty bulk", "$0\r\n\r\n", BulkString([]byte{})},
		{"null bulk", "$-1\r\n", BulkString(nil)},
		{
			"command array",
			"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
			Array(BulkString([]byte("GET")), BulkString([]byte("foo"))),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Decode(bufio.NewReader(strings.NewReader(tc.in)))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Decode = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// TestRoundTrip: Encode then Decode yields an equal value — including a
// binary-unsafe bulk string (embedded CRLF and a NUL byte), which the length
// prefix must carry intact.
func TestRoundTrip(t *testing.T) {
	values := []Value{
		SimpleString("PONG"),
		Error("ERR nope"),
		Integer(-7),
		BulkString([]byte("has \r\n and \x00 inside")),
		BulkString([]byte{}),
		BulkString(nil),
		Array(BulkString([]byte("SET")), BulkString([]byte("k")), BulkString([]byte("v"))),
	}
	for _, v := range values {
		var buf bytes.Buffer
		if err := v.Encode(&buf); err != nil {
			t.Fatalf("Encode(%#v): %v", v, err)
		}
		got, err := Decode(bufio.NewReader(&buf))
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if !reflect.DeepEqual(got, v) {
			t.Errorf("round-trip = %#v, want %#v", got, v)
		}
	}
}

// TestDecode_Errors: malformed input must return an error, not a bogus value.
func TestDecode_Errors(t *testing.T) {
	inputs := map[string]string{
		"empty stream":          "",
		"unknown type tag":      "!bogus\r\n",
		"bulk shorter than len": "$5\r\nhi\r\n",
		"non-numeric integer":   ":notanumber\r\n",
		"array missing element": "*2\r\n$3\r\nGET\r\n",
	}
	for name, in := range inputs {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(bufio.NewReader(strings.NewReader(in))); err == nil {
				t.Errorf("Decode(%q) = nil error, want an error", in)
			}
		})
	}
}
