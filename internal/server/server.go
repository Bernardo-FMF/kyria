package server

import (
	"bufio"
	"errors"
	"net"
	"sync"

	"github.com/Bernardo-FMF/kyria/internal/cluster"
	"github.com/Bernardo-FMF/kyria/internal/protocol"
	"github.com/Bernardo-FMF/kyria/internal/store"
)

// Server is kyria's TCP front door: it accepts connections, frames RESP values
// off each one, and dispatches them through a Handler. It parses no protocol and
// touches no store directly — Decode/Encode live in internal/protocol and command
// dispatch in the Handler. Two goroutine layers run it: Serve is a single accept
// loop, and handleConn is one read→dispatch→write loop per connection.
//
// conns and closed exist for graceful shutdown. A goroutine blocked in conn.Read
// does not wake just because the accept loop stops, so Close must close the live
// connections to unblock their reads — which means tracking them. The closed
// flag, written under mu, orders shutdown against trackConn so no connection is
// left tracked-but-unclosed.
type Server struct {
	handler  *Handler
	listener net.Listener
	wg       sync.WaitGroup // tracks in-flight connection goroutines, for drain

	mu     sync.Mutex            // guards conns and closed
	conns  map[net.Conn]struct{} // the set of live connections
	closed bool                  // true once Close has begun
}

// NewServer returns a Server that dispatches commands against store. Call Listen
// then Serve to run it.
func NewServer(store store.Store, members *cluster.Members) *Server {
	handler := NewHandler(store, members)
	conns := make(map[net.Conn]struct{}, 10)

	return &Server{
		handler: handler,
		conns:   conns,
	}
}

// Listen binds the server to addr, a "host:port" TCP address. It is split from
// Serve so a caller can bind first — for example to "127.0.0.1:0" for an
// OS-assigned port — and read Addr before the accept loop starts.
func (s *Server) Listen(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = l
	return nil
}

// Addr returns the address the server is listening on. It is valid after Listen.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// Serve runs the accept loop, handing each new connection to its own goroutine.
// It blocks until Close, then returns nil. Any other Accept failure tears the
// server down and returns the error.
func (s *Server) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Close closes the listener, which unblocks Accept with an error;
			// isClosing tells that expected wakeup apart from a real failure.
			if s.isClosing() {
				return nil
			}
			s.Close()
			return err
		}

		s.wg.Add(1) // count the connection before its goroutine starts
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// handleConn serves one connection until it closes, looping over decode a RESP
// value → parse a command → dispatch → write the reply. What happens on an error
// turns on where it came from:
//
//   - Decode returns a *protocol.ProtocolError: the byte framing is broken and the
//     stream can't be resynced, so reply with an error and close the connection.
//   - Decode returns any other error (io.EOF, a read failure): the transport is
//     gone, so just close — there is no one to reply to.
//   - Command returns an error: a well-formed value that isn't a command. The
//     stream is still aligned, so reply with an error and keep serving.
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	if !s.trackConn(conn) {
		return // server is already shutting down
	}
	defer s.untrackConn(conn)

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		value, err := protocol.Decode(reader)
		if err != nil {
			if perr, ok := errors.AsType[*protocol.ProtocolError](err); ok {
				errReply := protocol.Error("ERR " + perr.Error())
				err = errReply.Encode(writer)
				if err != nil {
					return
				}
				err = writer.Flush()
				if err != nil {
					return
				}
			}
			return
		}

		cmd, err := value.Command()
		if err != nil {
			errReply := protocol.Error("ERR " + err.Error())
			err = errReply.Encode(writer)
			if err != nil {
				return
			}
			err = writer.Flush()
			if err != nil {
				return
			}
			continue
		}

		reply := s.handler.Handle(cmd)
		err = reply.Encode(writer)
		if err != nil {
			return
		}
		err = writer.Flush()
		if err != nil {
			return
		}
	}
}

// trackConn registers conn as live, returning false if the server is already
// closing (in which case the caller must not serve it). Recording conn under the
// same lock Close uses is what closes the shutdown race: conn is either added
// before Close walks the set, or trackConn sees the closed flag and bails.
func (s *Server) trackConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	s.conns[conn] = struct{}{}
	return true
}

// untrackConn removes conn from the live set as its handler exits.
func (s *Server) untrackConn(conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.conns, conn)
}

// isClosing reports whether Close has begun. Serve reads the shutdown state
// through this so the access is synchronized with Close's write to s.closed.
func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

// Close shuts the server down gracefully and is safe to call more than once. It
// closes the listener (unblocking Serve's Accept) and every live connection
// (unblocking each handler's Read), then waits for the connection goroutines to
// drain. The wait happens after releasing the lock, because an exiting handler
// calls untrackConn, which needs it.
func (s *Server) Close() error {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		s.wg.Wait()
		return nil
	}

	s.closed = true
	err := s.listener.Close()

	// Closing each live connection unblocks its handler's Read so it can exit.
	// These close errors are uninteresting during shutdown; keep the listener's.
	for c := range s.conns {
		c.Close()
	}

	s.mu.Unlock()
	s.wg.Wait()

	return err
}
