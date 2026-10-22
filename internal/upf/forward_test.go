package upf

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/midu/telco-upf-gen/internal/gtpu"
	"github.com/midu/telco-upf-gen/internal/telemetry"
)

func ifaceStat(t *testing.T, snap telemetry.NodeSnap, name string) telemetry.IfaceSnap {
	t.Helper()
	for _, f := range snap.Ifaces {
		if f.Name == name {
			return f
		}
	}
	t.Fatalf("iface %s missing in snapshot %+v", name, snap)
	return telemetry.IfaceSnap{}
}

// startTestUPF boots one node bound to explicit ports and wires its N9 peer
// address straight to the other test node's N9 endpoint (host:port form of
// PeerDNSTemplate), emulating the headless-service DNS on one machine.
func startTestUPF(t *testing.T, id, n3, n5, n9 int, peer string) *UPF {
	t.Helper()
	u := New(Config{
		NodeID:          uint8(id),
		NodeName:        fmt.Sprintf("upf-%d", id),
		Nodes:           2,
		PeerDNSTemplate: peer,
		N3Port:          n3,
		N5Port:          n5,
		N9Port:          n9,
		Sessions:        8,
		Workers:         2,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := u.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("upf %d: %v", id, err)
		}
	}()
	time.Sleep(150 * time.Millisecond)
	return u
}

func dudp(t *testing.T, port int) *net.UDPConn {
	t.Helper()
	c, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		t.Fatalf("dial udp/:%d: %v", port, err)
	}
	return c
}

func sendGPDU(t *testing.T, port int, teid uint32, n int) {
	t.Helper()
	c := dudp(t, port)
	defer c.Close()
	payload := make([]byte, 20) // minimal synthetic IPv4 datagram
	payload[0] = 0x45
	payload[9] = 17
	for i := 0; i < n; i++ {
		pkt := gtpu.NewPacket(teid, payload)
		if _, err := c.Write(pkt.Marshal()); err != nil {
			t.Fatalf("write udp/:%d: %v", port, err)
		}
	}
}

// waitUp fires a liveness sentinel (invalid namespace: counted, never routed)
// at each node and waits until both report having received it, so the real
// traffic below starts only after every socket is bound.
func waitUp(t *testing.T, a *UPF, aN3Port int, b *UPF, bN9Port int) {
	t.Helper()
	sendGPDU(t, aN3Port, 0x00000001, 1)
	sendGPDU(t, bN9Port, 0x00000002, 1)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ai := ifaceStat(t, a.Stats().Snapshot(), "N3")
		bi := ifaceStat(t, b.Stats().Snapshot(), "N9")
		if ai.RxPackets >= 1 && bi.RxPackets >= 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("listeners did not come up in time")
}

// TestForwardingFabric boots two in-process UPF nodes and verifies:
//  1. a locally anchored N3 uplink is decapped onto the DN on the same node
//  2. a cross-node N3 uplink (session anchored at the peer) is retagged into
//     the N9 namespace, crossed the fabric, and decapped at the anchor
//  3. an N9-direct injection is decapped at the receiving node
//  4. namespace/session validation drops illegal ingress
func TestForwardingFabric(t *testing.T) {
	a := startTestUPF(t, 0, 3152, 3153, 3154, "127.0.0.1:4254")
	b := startTestUPF(t, 1, 3252, 3253, 4254, "127.0.0.1:3154")
	waitUp(t, a, 3152, b, 4254)

	// EW: anchored at node 1, injected through node 0's N3.
	sendGPDU(t, 3152, NewTeid(TeidNSN3, 1, 3), 5)
	// N9 direct into node 1.
	sendGPDU(t, 4254, NewTeid(TeidNSN9, 1, 4), 3)
	// Locally anchored uplink on node 0.
	sendGPDU(t, 3152, NewTeid(TeidNSN3, 0, 2), 4)
	// Bad namespace on N3 ingress + unknown session: must be dropped.
	sendGPDU(t, 3152, NewTeid(TeidNSN5, 0, 2), 3)
	sendGPDU(t, 3152, NewTeid(TeidNSN3, 0, 9), 3)

	deadline := time.Now().Add(5 * time.Second)
	var as, bs telemetry.NodeSnap
	for time.Now().Before(deadline) {
		as = a.Stats().Snapshot()
		bs = b.Stats().Snapshot()
		aN3 := ifaceStat(t, as, "N3")
		aN5 := ifaceStat(t, as, "N5")
		aN9 := ifaceStat(t, as, "N9")
		bN9 := ifaceStat(t, bs, "N9")
		bN5 := ifaceStat(t, bs, "N5")
		// a: 1 sentinel + 5 ew-inject + 4 local + 3 + 3 dropped
		// b: 1 sentinel + 5 ew-arrive + 3 direct-arrive
		if aN3.RxPackets >= 16 && aN3.Dropped >= 6 && aN9.TxPackets >= 5 &&
			aN5.Decapsulated >= 4 && bN9.RxPackets >= 9 && bN5.Decapsulated >= 8 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("counters did not converge:\n A=%+v\n B=%+v", as, bs)
}

// TestForwardingNonGTPU verifies non-GTP-U datagrams are counted and dropped.
func TestForwardingNonGTPU(t *testing.T) {
	a := startTestUPF(t, 0, 3552, 3553, 3554, "127.0.0.1:4654")
	b := startTestUPF(t, 1, 3652, 3653, 4654, "127.0.0.1:3554")
	waitUp(t, a, 3552, b, 4654)

	c := dudp(t, 3552)
	defer c.Close()
	for i := 0; i < 3; i++ {
		if _, err := c.Write([]byte("not gtpu")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap := a.Stats().Snapshot()
		n3 := ifaceStat(t, snap, "N3")
		if n3.RxPackets >= 4 && n3.Dropped >= 4 { // 3 garbage + 1 sentinel
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("garbage not dropped: %+v", a.Stats().Snapshot())
}
