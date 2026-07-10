package cluster

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"
)

// TestGossipMarshalRoundTrip: a []Node survives marshal → unmarshal unchanged.
func TestGossipMarshalRoundTrip(t *testing.T) {
	want := []Node{
		{ID: "a", Addr: "127.0.0.1:7000", State: Alive, Incarnation: 3},
		{ID: "b", Addr: "127.0.0.1:7001", State: Dead, Incarnation: 7},
	}

	data, err := marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := unmarshal(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip = %+v, want %+v", got, want)
	}
}

// TestUnmarshal_Truncated: every truncated prefix of a valid packet must return
// errMalformed, never panic — the bounds checks in unmarshal and its decode helpers
// are what protect the receive loop from crafted or corrupt input.
func TestUnmarshal_Truncated(t *testing.T) {
	full, err := marshal([]Node{
		{ID: "a", Addr: "127.0.0.1:7000", State: Alive, Incarnation: 3},
		{ID: "b", Addr: "127.0.0.1:7001", State: Dead, Incarnation: 7},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	for cut := 0; cut < len(full); cut++ {
		if _, err := unmarshal(full[:cut]); !errors.Is(err, errMalformed) {
			t.Errorf("unmarshal(full[:%d]) error = %v, want errMalformed", cut, err)
		}
	}
}

// TestPickPeers: pickPeers returns up to k distinct addresses from its input.
func TestPickPeers(t *testing.T) {
	addrs := []string{"a", "b", "c", "d"}

	if got := pickPeers(addrs, 2); len(got) != 2 {
		t.Errorf("pickPeers(k=2) len = %d, want 2", len(got))
	} else {
		assertDistinctSubset(t, got, addrs)
	}

	if got := pickPeers(addrs, 10); len(got) != len(addrs) {
		t.Errorf("pickPeers(k=10) len = %d, want %d (capped at input size)", len(got), len(addrs))
	}

	if got := pickPeers(addrs, 0); len(got) != 0 {
		t.Errorf("pickPeers(k=0) len = %d, want 0", len(got))
	}
}

func assertDistinctSubset(t *testing.T, got, universe []string) {
	t.Helper()
	valid := map[string]bool{}
	for _, a := range universe {
		valid[a] = true
	}
	seen := map[string]bool{}
	for _, g := range got {
		if seen[g] {
			t.Errorf("duplicate %q in pickPeers result", g)
		}
		seen[g] = true
		if !valid[g] {
			t.Errorf("pickPeers returned %q, not in the input set", g)
		}
	}
}

// TestGossip_Converges spins up three gossipers on real loopback UDP sockets and
// checks they converge on a full membership view. The seeding is a star — only n0
// is a seed — so n1 and n2 can only learn about each other transitively through
// n0's gossip, which exercises real dissemination rather than direct seeding.
func TestGossip_Converges(t *testing.T) {
	const n = 3

	members := make([]*Members, n)
	gossipers := make([]*Gossiper, n)
	conns := make([]net.PacketConn, n)

	for i := 0; i < n; i++ {
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("ListenPacket: %v", err)
		}
		conns[i] = conn
		members[i] = NewMembers(Node{
			ID:          fmt.Sprintf("n%d", i),
			Addr:        conn.LocalAddr().String(),
			State:       Alive,
			Incarnation: 1,
		})
	}

	seed := conns[0].LocalAddr().String()
	for i := 0; i < n; i++ {
		var seeds []string
		if i != 0 {
			seeds = []string{seed} // only n0 is a seed
		}
		gossipers[i] = NewGossiper(members[i], conns[i],
			WithSeeds(seeds),
			WithGossipInterval(50*time.Millisecond),
			WithFailTimeout(2*time.Second),
			WithFanout(n),
		)
		gossipers[i].Start()
	}
	defer func() {
		for _, g := range gossipers {
			g.Stop()
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		converged := true
		for i := 0; i < n; i++ {
			if len(members[i].Alive()) != n {
				converged = false
				break
			}
		}
		if converged {
			return // every node sees all n members
		}
		if time.Now().After(deadline) {
			for i := 0; i < n; i++ {
				t.Logf("n%d knows %d/%d members", i, len(members[i].Alive()), n)
			}
			t.Fatal("cluster did not converge within 5s")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
