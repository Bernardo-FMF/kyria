package server

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// startServer binds a Server to an ephemeral loopback port, runs its accept loop
// in a goroutine, and registers cleanup that shuts it down. Serve returns nil on
// a clean Close, so the goroutine's return value is ignored.
func startServer(t *testing.T) *Server {
	t.Helper()
	srv := NewServer(store.New(), nil, nil, nil)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })
	return srv
}

// dial opens a client connection to srv, wrapped in a bufio.Reader for reading
// replies. The deadline stops a misbehaving server from hanging the test.
func dial(t *testing.T, srv *Server) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	return conn, bufio.NewReader(conn)
}

// writeCommand sends parts as a RESP array of bulk strings — the wire form of a
// client request, e.g. writeCommand(t, conn, "SET", "k", "v").
func writeCommand(t *testing.T, conn net.Conn, parts ...string) {
	t.Helper()
	bulks := make([]protocol.Value, len(parts))
	for i, p := range parts {
		bulks[i] = protocol.BulkString([]byte(p))
	}
	if err := protocol.Array(bulks...).Encode(conn); err != nil {
		t.Fatalf("write command %v: %v", parts, err)
	}
}

// readReply decodes one reply from r and returns its canonical wire bytes, so a
// test can assert on the exact RESP string the server produced.
func readReply(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	v, err := protocol.Decode(r)
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	var buf bytes.Buffer
	if err := v.Encode(&buf); err != nil {
		t.Fatalf("re-encode reply: %v", err)
	}
	return buf.String()
}

func TestServer_PingPong(t *testing.T) {
	srv := startServer(t)
	conn, r := dial(t, srv)

	writeCommand(t, conn, "PING")
	if got := readReply(t, r); got != "+PONG\r\n" {
		t.Errorf("PING = %q, want %q", got, "+PONG\r\n")
	}
}

func TestServer_SetGet(t *testing.T) {
	srv := startServer(t)
	conn, r := dial(t, srv)

	writeCommand(t, conn, "SET", "k", "v")
	if got := readReply(t, r); got != "+OK\r\n" {
		t.Errorf("SET = %q, want %q", got, "+OK\r\n")
	}

	writeCommand(t, conn, "GET", "k")
	if got := readReply(t, r); got != "$1\r\nv\r\n" {
		t.Errorf("GET k = %q, want %q", got, "$1\r\nv\r\n")
	}

	writeCommand(t, conn, "GET", "missing")
	if got := readReply(t, r); got != "$-1\r\n" {
		t.Errorf("GET missing = %q, want %q (null bulk)", got, "$-1\r\n")
	}
}

// TestServer_MultipleCommandsSameConn: many requests on one connection get their
// replies back in order — the per-connection read loop keeps serving.
func TestServer_MultipleCommandsSameConn(t *testing.T) {
	srv := startServer(t)
	conn, r := dial(t, srv)

	writeCommand(t, conn, "PING")
	writeCommand(t, conn, "SET", "a", "1")
	writeCommand(t, conn, "GET", "a")

	for i, want := range []string{"+PONG\r\n", "+OK\r\n", "$1\r\n1\r\n"} {
		if got := readReply(t, r); got != want {
			t.Errorf("reply %d = %q, want %q", i, got, want)
		}
	}
}

// TestServer_BadCommandKeepsConnectionOpen: a well-formed RESP value that isn't a
// command array (here a bare integer) leaves the stream aligned, so the server
// replies -ERR and keeps the connection usable.
func TestServer_BadCommandKeepsConnectionOpen(t *testing.T) {
	srv := startServer(t)
	conn, r := dial(t, srv)

	if err := protocol.Integer(5).Encode(conn); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := readReply(t, r); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("bad-command reply = %q, want a -ERR error", got)
	}

	// The connection must still work after a command-level error.
	writeCommand(t, conn, "PING")
	if got := readReply(t, r); got != "+PONG\r\n" {
		t.Errorf("PING after bad command = %q, want %q", got, "+PONG\r\n")
	}
}

// TestServer_ProtocolErrorClosesConnection: an unknown type byte is a framing
// error — the byte stream is desynced — so the server replies -ERR and then
// closes the connection.
func TestServer_ProtocolErrorClosesConnection(t *testing.T) {
	srv := startServer(t)
	conn, r := dial(t, srv)

	if _, err := conn.Write([]byte("!nonsense\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := readReply(t, r); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("protocol-error reply = %q, want a -ERR error", got)
	}

	// The server should have closed the connection: the next decode hits EOF.
	if _, err := protocol.Decode(r); err == nil {
		t.Error("expected the connection to be closed after a protocol error")
	}
}

// TestServer_GracefulShutdown: Close drains a live connection and returns
// promptly, and Serve returns nil. A ping/pong first guarantees the connection
// is accepted and tracked before Close runs.
func TestServer_GracefulShutdown(t *testing.T) {
	srv := NewServer(store.New(), nil, nil, nil)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve() }()

	conn, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	r := bufio.NewReader(conn)

	// Round-trip once so the server has definitely accepted and is serving it.
	writeCommand(t, conn, "PING")
	if got := readReply(t, r); got != "+PONG\r\n" {
		t.Fatalf("PING = %q, want %q", got, "+PONG\r\n")
	}

	closed := make(chan error, 1)
	go func() { closed <- srv.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Errorf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung — the live connection did not drain")
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Errorf("Serve returned %v, want nil after a clean Close", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after Close")
	}

	// Close is idempotent.
	if err := srv.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
