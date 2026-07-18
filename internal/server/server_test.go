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
	"github.com/Bernardo-FMF/kyria/internal/telemetry"
)

// startServer binds a Server to an ephemeral loopback port, runs its accept loop
// in a goroutine, and registers cleanup that shuts it down. Serve returns nil on
// a clean Close, so the goroutine's return value is ignored.
func startServer(t *testing.T) *Server {
	t.Helper()
	srv := NewServer(store.New(), nil, nil, nil, ServerOptions{})
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
	srv := NewServer(store.New(), nil, nil, nil, ServerOptions{})
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

// TestServer_MaxConnsCap: with a cap of 2, two live connections fill both slots; a third connects
// (the OS completes the handshake via the listen backlog) but no handler ever reads it — the accept
// loop is blocked on acquire() — so its PING gets no reply until one of the first two closes and
// frees a slot.
func TestServer_MaxConnsCap(t *testing.T) {
	srv := NewServer(store.New(), nil, nil, nil, ServerOptions{MaxConns: 2})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })

	// Two connections, each served a PING→PONG and then held open, so their handlers keep both slots.
	c1, r1 := dial(t, srv)
	c2, r2 := dial(t, srv)
	writeCommand(t, c1, "PING")
	if got := readReply(t, r1); got != "+PONG\r\n" {
		t.Fatalf("c1 PING = %q, want +PONG", got)
	}
	writeCommand(t, c2, "PING")
	if got := readReply(t, r2); got != "+PONG\r\n" {
		t.Fatalf("c2 PING = %q, want +PONG", got)
	}

	// A third connection at capacity: it connects, but nothing reads it, so its PING gets no reply.
	c3, r3 := dial(t, srv)
	writeCommand(t, c3, "PING")
	reply := make(chan error, 1)
	go func() { _, err := protocol.Decode(r3); reply <- err }()

	select {
	case err := <-reply:
		t.Fatalf("c3 was served while at capacity (decode err %v), want it blocked behind the cap", err)
	case <-time.After(300 * time.Millisecond):
		// good — no reply arrives; c3 is queued behind the semaphore
	}

	// Free a slot; the accept loop unblocks, accepts c3, and it finally gets its PONG.
	c1.Close()
	select {
	case err := <-reply:
		if err != nil {
			t.Fatalf("c3 decode after a slot freed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("c3 still not served 2s after a slot freed")
	}
}

// TestServer_UnlimitedConnsWhenUncapped: max-conns 0 means no cap, so many connections open at once
// are all served — none blocks behind a semaphore.
func TestServer_UnlimitedConnsWhenUncapped(t *testing.T) {
	srv := startServer(t) // max-conns 0
	const n = 16
	for i := 0; i < n; i++ {
		c, r := dial(t, srv)
		writeCommand(t, c, "PING")
		if got := readReply(t, r); got != "+PONG\r\n" {
			t.Fatalf("conn %d PING = %q, want +PONG (uncapped must serve every connection)", i, got)
		}
		// dial registers a cleanup close, so all n connections stay open at once.
	}
}

// TestServer_StatsReportsConnectionGauges: the connection gauges registered by NewServer are sampled
// live on every STATS — conns_live counts the connection issuing the command itself, and conns_max
// reports the configured cap. This is the end-to-end proof that a gauge reads current state rather
// than a value frozen at registration.
func TestServer_StatsReportsConnectionGauges(t *testing.T) {
	tel := telemetry.New(ClientCommands...)
	srv := NewServer(store.New(), nil, nil, nil, ServerOptions{MaxConns: 5, Telemetry: tel})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })

	c, r := dial(t, srv)
	writeCommand(t, c, "STATS")
	got := readReply(t, r)

	if !strings.Contains(got, "conns_live:1\r\n") {
		t.Errorf("STATS = %q, want conns_live:1 (the connection asking is itself live)", got)
	}
	if !strings.Contains(got, "conns_max:5\r\n") {
		t.Errorf("STATS = %q, want conns_max:5 (the configured cap)", got)
	}
}

// TestServer_IdleConnTimeout: with a conn-timeout set, a connection that goes idle (sends nothing)
// is closed by the server once the read deadline lapses, so a pending read returns EOF.
func TestServer_IdleConnTimeout(t *testing.T) {
	srv := NewServer(store.New(), nil, nil, nil, ServerOptions{ConnTimeout: 150 * time.Millisecond})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })

	c, r := dial(t, srv)
	writeCommand(t, c, "PING")
	if got := readReply(t, r); got != "+PONG\r\n" {
		t.Fatalf("PING = %q, want +PONG", got)
	}

	// Go idle: after ~150ms the server's read deadline fires and it closes the connection, so the
	// blocked read returns an error (EOF) rather than hanging.
	closed := make(chan error, 1)
	go func() { _, err := r.ReadByte(); closed <- err }()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("read on an idle connection succeeded, want the server to have closed it")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connection still open long after the idle timeout — server did not close it")
	}
}

// TestServer_ActiveConnStaysOpenPastTimeout: an active connection whose commands are spaced closer
// than conn-timeout keeps resetting the rolling read deadline, so it stays open well past the timeout
// window. A set-once deadline (pinned at connect time) would close it mid-stream — this is the test
// that catches that mistake.
func TestServer_ActiveConnStaysOpenPastTimeout(t *testing.T) {
	srv := NewServer(store.New(), nil, nil, nil, ServerOptions{ConnTimeout: 300 * time.Millisecond})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { srv.Close() })

	c, r := dial(t, srv)
	// Five PINGs, 100ms apart (< 300ms) and spanning 500ms total (> 300ms). Each resets the deadline,
	// so the connection must survive the whole span.
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		writeCommand(t, c, "PING")
		if got := readReply(t, r); got != "+PONG\r\n" {
			t.Fatalf("PING %d = %q, want +PONG (an active conn must stay open past conn-timeout)", i, got)
		}
	}
}
