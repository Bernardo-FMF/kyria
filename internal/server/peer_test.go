package server

import (
	"testing"
	"time"
)

// TestPeer_Replication drives the peer client against a real server: Set/Get/Del
// over a socket, exercising the internal RSET/RGET/RDEL verbs end to end.
func TestPeer_Replication(t *testing.T) {
	srv := startServer(t)
	addr := srv.Addr().String()
	peer := NewPeer(2 * time.Second)

	// Set then Get round-trips the value.
	if err := peer.Set(addr, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, found, err := peer.Get(addr, "k"); err != nil || !found || string(got) != "v" {
		t.Fatalf("Get after Set = (%q, %v, %v), want (\"v\", true, nil)", got, found, err)
	}

	// A TTL'd Set (the PX path) is readable before it expires.
	if err := peer.Set(addr, "t", []byte("x"), time.Hour); err != nil {
		t.Fatalf("Set with ttl: %v", err)
	}
	if got, found, _ := peer.Get(addr, "t"); !found || string(got) != "x" {
		t.Errorf("Get of a TTL'd key = (%q, %v), want (\"x\", true)", got, found)
	}

	// Del removes it; a subsequent Get is a clean miss.
	if err := peer.Del(addr, "k"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, found, err := peer.Get(addr, "k"); err != nil || found {
		t.Errorf("Get after Del = (found %v, err %v), want (false, nil)", found, err)
	}
}
