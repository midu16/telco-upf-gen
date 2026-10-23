package traffic

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/midu/telco-upf-gen/internal/dpi"
	"github.com/midu/telco-upf-gen/internal/gtpu"
)

func TestLoadConfigDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tg.yaml")
	if err := os.WriteFile(path, []byte("upf:\n  - {host: 192.0.2.10, node: 0, sessions: 8}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !c.External() {
		t.Fatalf("explicit endpoints must be external mode")
	}
	wantDefaults := map[string]bool{
		"name/trafficgen":  c.Name == "trafficgen",
		"interval":         c.IntervalMS == 20,
		"pps":              c.PPS == 100,
		"throttle":         c.Throttle == 100,
		"max-sessions":     c.MaxSessions == 32,
		"discover-default": c.Discover != nil && c.Discover.Nodes == 2,
	}
	for k, ok := range wantDefaults {
		if !ok {
			t.Fatalf("default %q not applied: %+v", k, c)
		}
	}
}

func TestLoadConfigValidation(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("name: x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(bad); err == nil {
		t.Fatalf("missing upf/discover must error")
	}
	bad2 := filepath.Join(dir, "bad2.yaml")
	if err := os.WriteFile(bad2, []byte("upf:\n  - {host: \"\", node: 0}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(bad2); err == nil {
		t.Fatalf("empty host must error")
	}
}

// TestMockDatagramsClassify is the cross-package contract: every mock app a
// flow can emit must classify to the DPI class it pretends to be. That is
// what makes the per-app dashboard honest.
func TestMockDatagramsClassify(t *testing.T) {
	local := &net.UDPAddr{IP: net.IPv4(10, 1, 2, 3), Port: 12345}
	apps := []struct {
		seq  uint32
		want dpi.AppID
	}{
		{0, dpi.AppHTTP},
		{1, dpi.AppHTTPS},
		{2, dpi.AppDNS},
		{3, dpi.AppQUIC},
		{4, dpi.AppOther},
	}
	for _, tc := range apps {
		teid := uint32(0x10000000 | tc.seq)
		for _, seq := range []uint32{0, 1, 42} {
			dg := fakeIP(local, teid, seq)
			spi := dpi.Shallow(dg)
			if !spi.Valid {
				t.Fatalf("seq %d teid %x: malformed inner datagram", seq, teid)
			}
			app, detail := dpi.Classify(spi, l4of(dg, spi))
			if app != tc.want {
				t.Fatalf("teid %x seq %d: app = %v, want %v (detail %q)", teid, seq, app, tc.want, detail)
			}
		}
	}
}

func l4of(dg []byte, s dpi.SPI) []byte {
	if len(dg) < 20 {
		return nil
	}
	ihl := int(dg[0]&0x0f) * 4
	if len(dg) < ihl {
		return nil
	}
	hdr := dg[ihl:]
	switch s.Proto {
	case 17:
		if len(hdr) >= 8 {
			return hdr[8:]
		}
	case 6:
		if len(hdr) >= 20 {
			return hdr[20:]
		}
	}
	return nil
}

// TestFlowThrottle is a live token-bucket check against a local UDP sink:
// throttle 0 emits nothing, throttle 100 emits, and the clamp holds.
func TestFlowThrottle(t *testing.T) {
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	sinkPort := sink.LocalAddr().(*net.UDPAddr).Port

	gen := NewGenerator("t", 5*time.Millisecond)
	f := &Flow{
		Name:  "f0",
		Iface: "n3",
		Dest:  net.JoinHostPort("127.0.0.1", strconv.Itoa(sinkPort)),
		Teid:  0x10000001,
		PPS:   2000, // high enough to be deterministic in 100ms
		Size:  64,
	}
	gen.AddFlow(f)
	if err := gen.Dial(); err != nil {
		t.Fatal(err)
	}
	defer gen.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	gen.Start(ctx)

	gen.SetThrottle(0)
	time.Sleep(120 * time.Millisecond)
	sink.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, _, err := sink.ReadFromUDP(make([]byte, 1500)); err == nil {
		t.Fatalf("throttle 0 must emit zero packets")
	}

	v := gen.SetThrottle(250) // clamp up
	if v != 100 {
		t.Fatalf("SetThrottle(250) = %v, want 100", v)
	}
	v = gen.SetThrottle(-5) // clamp down
	if v != 0 {
		t.Fatalf("SetThrottle(-5) = %v, want 0", v)
	}
	gen.SetThrottle(100)
	time.Sleep(120 * time.Millisecond)
	sink.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, _, err := sink.ReadFromUDP(make([]byte, 1500)); err != nil {
		t.Fatalf("throttle 100 must emit: %v", err)
	}

	// the GTP-U wrapper itself decodes cleanly (hot-loop contract)
	pkt := f.buildPacket(nil)
	p, err := gtpu.Parse(pkt)
	if err != nil {
		t.Fatalf("marshal/parse roundtrip: %v", err)
	}
	if p.Hdr.Teid != f.Teid {
		t.Fatalf("teid %x != %x", p.Hdr.Teid, f.Teid)
	}
}
