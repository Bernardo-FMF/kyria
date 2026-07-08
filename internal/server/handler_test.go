package server

import (
	"bytes"
	"strings"
	"testing"

	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// reply dispatches a command against s and returns the RESP-encoded reply as a
// string. Tests assert on the wire bytes because protocol.Value's fields are
// unexported — the encoded output is the observable behavior anyway.
func reply(t *testing.T, s store.Store, name string, args ...string) string {
	t.Helper()
	byteArgs := make([][]byte, len(args))
	for i, a := range args {
		byteArgs[i] = []byte(a)
	}

	v := NewHandler(s).Handle(protocol.Command{Name: name, Args: byteArgs})

	var buf bytes.Buffer
	if err := v.Encode(&buf); err != nil {
		t.Fatalf("Encode reply: %v", err)
	}
	return buf.String()
}

func TestHandle_Ping(t *testing.T) {
	if got := reply(t, store.New(), "PING"); got != "+PONG\r\n" {
		t.Errorf("PING = %q, want %q", got, "+PONG\r\n")
	}
}

func TestHandle_Get(t *testing.T) {
	s := store.New()
	if _, err := s.Set("foo", []byte("bar")); err != nil {
		t.Fatal(err)
	}

	if got := reply(t, s, "GET", "foo"); got != "$3\r\nbar\r\n" {
		t.Errorf("GET foo = %q, want %q", got, "$3\r\nbar\r\n")
	}
	if got := reply(t, s, "GET", "missing"); got != "$-1\r\n" {
		t.Errorf("GET missing = %q, want %q (null bulk)", got, "$-1\r\n")
	}
}

func TestHandle_Set(t *testing.T) {
	s := store.New()

	if got := reply(t, s, "SET", "k", "v"); got != "+OK\r\n" {
		t.Errorf("SET = %q, want %q", got, "+OK\r\n")
	}
	// The reply is only meaningful if the value actually landed in the store.
	if got, ok := s.Get("k"); !ok || string(got) != "v" {
		t.Errorf("after SET, Get = %q, %v; want \"v\", true", got, ok)
	}
}

func TestHandle_Del(t *testing.T) {
	s := store.New()
	if _, err := s.Set("k", []byte("v")); err != nil {
		t.Fatal(err)
	}

	if got := reply(t, s, "DEL", "k"); got != ":1\r\n" {
		t.Errorf("DEL existing = %q, want %q", got, ":1\r\n")
	}
	if got := reply(t, s, "DEL", "k"); got != ":0\r\n" {
		t.Errorf("DEL missing = %q, want %q", got, ":0\r\n")
	}
}

func TestHandle_CaseInsensitive(t *testing.T) {
	if got := reply(t, store.New(), "ping"); got != "+PONG\r\n" {
		t.Errorf("lowercase ping = %q, want +PONG", got)
	}
}

func TestHandle_Errors(t *testing.T) {
	tests := []struct {
		name string
		cmd  string
		args []string
	}{
		{"unknown command", "BOGUS", nil},
		{"GET wrong arity", "GET", nil},
		{"GET too many args", "GET", []string{"a", "b"}},
		{"SET wrong arity", "SET", []string{"k"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := reply(t, store.New(), tc.cmd, tc.args...); !strings.HasPrefix(got, "-ERR") {
				t.Errorf("reply = %q, want a -ERR error", got)
			}
		})
	}
}

// TestHandle_SetWithTTL: SET key value <EX seconds | PX milliseconds> stores the
// value with an expiry and replies +OK. The option keyword is case-insensitive.
func TestHandle_SetWithTTL(t *testing.T) {
	s := store.New()

	if got := reply(t, s, "SET", "k", "v", "PX", "10000"); got != "+OK\r\n" {
		t.Errorf("SET k v PX 10000 = %q, want %q", got, "+OK\r\n")
	}
	if got, ok := s.Get("k"); !ok || string(got) != "v" {
		t.Errorf("after SET PX, Get = %q, %v; want \"v\", true", got, ok)
	}

	if got := reply(t, s, "SET", "k2", "v2", "EX", "100"); got != "+OK\r\n" {
		t.Errorf("SET k2 v2 EX 100 = %q, want %q", got, "+OK\r\n")
	}
	if got, ok := s.Get("k2"); !ok || string(got) != "v2" {
		t.Errorf("after SET EX, Get = %q, %v; want \"v2\", true", got, ok)
	}

	if got := reply(t, s, "SET", "k3", "v3", "px", "5000"); got != "+OK\r\n" {
		t.Errorf("SET k3 v3 px 5000 (lowercase option) = %q, want %q", got, "+OK\r\n")
	}
}

// TestHandle_SetWithTTL_Errors: malformed TTL options are rejected with -ERR,
// and the argument count is bounded (SET takes 2 or 4 args).
func TestHandle_SetWithTTL_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"non-integer ttl", []string{"k", "v", "PX", "abc"}},
		{"unknown option", []string{"k", "v", "XY", "100"}},
		{"zero ttl", []string{"k", "v", "PX", "0"}},
		{"negative ttl", []string{"k", "v", "EX", "-5"}},
		{"missing ttl value", []string{"k", "v", "PX"}},
		{"too many args", []string{"k", "v", "PX", "100", "extra"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := reply(t, store.New(), "SET", tc.args...); !strings.HasPrefix(got, "-ERR") {
				t.Errorf("SET %v = %q, want a -ERR error", tc.args, got)
			}
		})
	}
}
