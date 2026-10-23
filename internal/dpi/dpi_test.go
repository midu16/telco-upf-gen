package dpi

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// datagram builders (independent of the traffic gen, minimal headers)

func udpDatagram(src, dst string, sport, dport uint16, payload []byte) []byte {
	total := 20 + 8 + len(payload)
	p := make([]byte, total)
	p[0] = 0x45
	p[8] = 64
	p[9] = 17
	p[2] = byte(total >> 8)
	p[3] = byte(total)
	copy(p[12:16], net.ParseIP(src).To4())
	copy(p[16:20], net.ParseIP(dst).To4())
	q := p[20:]
	q[0] = byte(sport >> 8)
	q[1] = byte(sport)
	q[2] = byte(dport >> 8)
	q[3] = byte(dport)
	ul := 8 + len(payload)
	q[4] = byte(ul >> 8)
	q[5] = byte(ul)
	copy(q[8:], payload)
	return p
}

func tcpDatagram(src, dst string, sport, dport uint16, payload []byte) []byte {
	total := 20 + 20 + len(payload)
	p := make([]byte, total)
	p[0] = 0x45
	p[8] = 64
	p[9] = 6
	p[2] = byte(total >> 8)
	p[3] = byte(total)
	copy(p[12:16], net.ParseIP(src).To4())
	copy(p[16:20], net.ParseIP(dst).To4())
	q := p[20:]
	q[0] = byte(sport >> 8)
	q[1] = byte(sport)
	q[2] = byte(dport >> 8)
	q[3] = byte(dport)
	q[12] = 5 << 4
	q[13] = 0x18
	copy(q[20:], payload)
	return p
}

func dnsMsg(name string) []byte {
	q := make([]byte, 0, 32)
	q = append(q, 0x12, 0x34, 0x01, 0x00)
	q = append(q, 0x00, 0x01, 0, 0, 0, 0, 0, 0)
	for _, part := range strings.Split(name, ".") {
		q = append(q, byte(len(part)))
		q = append(q, part...)
	}
	q = append(q, 0, 0x00, 0x01, 0x00, 0x01)
	return q
}

// tiny ClientHello whose only extension is SNI for host (wire-exact layout)
func clientHello(host string) []byte {
	entry := append([]byte{0x00, 0x00, byte(len(host))}, host...) // name type + 2B len + name
	extData := make([]byte, 0, 2+len(entry))
	extData = append(extData, byte(len(entry)>>8), byte(len(entry))) // server_name_list len
	extData = append(extData, entry...)
	ext := make([]byte, 0, 2+2+len(extData))
	ext = append(ext, 0x00, 0x02)
	ext = append(ext, byte(len(extData)>>8), byte(len(extData)))
	ext = append(ext, extData...)

	inner := 2 + 32 + 1 + 5 + len(ext) // version + random + sess + suites + exts
	rec := make([]byte, 0, 5+inner)
	rec = append(rec, 0x16, 0x03, 0x01, byte(inner>>8), byte(inner))
	rec = append(rec, 0x03, 0x03)
	rec = append(rec, make([]byte, 32)...)
	rec = append(rec, 0x00)
	rec = append(rec, 0x00, 0x02, 0x13, 0x01)
	rec = append(rec, ext...)
	return rec
}

func TestShallow(t *testing.T) {
	dg := udpDatagram("10.0.0.1", "10.99.5.7", 4321, 53, dnsMsg("example.com"))
	s := Shallow(dg)
	if !s.Valid {
		t.Fatalf("invalid datagram")
	}
	if s.Proto != 17 || s.SrcPort != 4321 || s.DstPort != 53 {
		t.Fatalf("bad SPI: %+v", s)
	}
	if s.Src != "10.0.0.1" || s.Dst != "10.99.5.7" {
		t.Fatalf("bad addresses: %+v", s)
	}
	if s.PayloadLen != len(dnsMsg("example.com")) {
		t.Fatalf("bad payload len %d", s.PayloadLen)
	}
	if got := Shallow([]byte{0x45, 0}); got.Valid {
		t.Fatalf("truncated datagram must be invalid")
	}
}

func TestClassify(t *testing.T) {
	sni := "example.com"
	cases := []struct {
		name    string
		dg      []byte
		wantApp AppID
		wantSub string
	}{
		{"dns", udpDatagram("10.0.0.1", "10.99.5.7", 4321, 53, dnsMsg("example.com")), AppDNS, "example.com"},
		{"http", tcpDatagram("10.0.0.1", "10.99.5.7", 4321, 80, []byte("GET /index.html HTTP/1.1\r\nHost: example.com\r\n\r\n")), AppHTTP, "GET /index.html"},
		{"https", tcpDatagram("10.0.0.1", "10.99.5.7", 4321, 443, clientHello(sni)), AppHTTPS, sni},
		{"quic", udpDatagram("10.0.0.1", "10.99.5.7", 4321, 443, append([]byte{0x40, 0x00, 0x00, 0x00, 0x01}, make([]byte, 10)...)), AppQUIC, "long-header"},
		{"icmp", icmpDatagram("10.0.0.1", "10.99.5.7"), AppICMP, "type 8"},
		{"other-udp", udpDatagram("10.0.0.1", "10.99.5.7", 4321, 6000, []byte{1, 2, 3}), AppOther, "udp"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := Shallow(c.dg)
			if !s.Valid {
				t.Fatalf("invalid datagram")
			}
			l4 := l4Payload(c.dg, s)
			app, detail := Classify(s, l4)
			if app != c.wantApp {
				t.Fatalf("app = %v, want %v (detail %q)", app, c.wantApp, detail)
			}
			if detail != c.wantSub {
				t.Fatalf("detail = %q, want %q", detail, c.wantSub)
			}
		})
	}
}

func icmpDatagram(src, dst string) []byte {
	total := 20 + 4
	p := make([]byte, total)
	p[0] = 0x45
	p[8] = 64
	p[9] = 1
	p[2] = byte(total >> 8)
	p[3] = byte(total)
	copy(p[12:16], net.ParseIP(src).To4())
	copy(p[16:20], net.ParseIP(dst).To4())
	p[20] = 8 // echo request
	return p
}

func TestPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	yaml := `
rules:
  - id: drop-dns
    description: block recursive DNS to protect the resolver
    apps: [dns]
    action: drop
  - id: observe-quic
    proto: udp
    ports:
      - {from: 443, to: 443}
    apps: [quic]
    action: observe
  - id: watch-blocked-net
    nets: ["10.66.0.0/16"]
    action: drop
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := LoadPolicy(path)
	if err != nil {
		t.Fatal(err)
	}

	dns := Shallow(udpDatagram("10.0.0.1", "10.99.5.7", 4321, 53, dnsMsg("example.com")))
	if _, a := p.Evaluate(dns, AppDNS); a != Drop {
		t.Fatalf("dns: action = %v, want drop", a)
	}
	quic := Shallow(udpDatagram("10.0.0.1", "10.99.5.7", 4321, 443, []byte{0x40, 0}))
	if rule, a := p.Evaluate(quic, AppQUIC); a != Observe || rule.ID != "observe-quic" {
		t.Fatalf("quic: rule %v action %v, want observe-quic/observe", rule, a)
	}
	http := Shallow(tcpDatagram("10.0.0.1", "10.66.1.9", 4321, 80, []byte("GET / HTTP/1.1")))
	if _, a := p.Evaluate(http, AppHTTP); a != Drop {
		t.Fatalf("blocked net: action = %v, want drop", a)
	}
	clean := Shallow(tcpDatagram("10.0.0.1", "10.99.5.7", 4321, 80, []byte("GET / HTTP/1.1")))
	if _, a := p.Evaluate(clean, AppHTTP); a != Allow {
		t.Fatalf("clean: action = %v, want allow", a)
	}

	if _, err := LoadPolicy("  "); err == nil {
		t.Fatalf("nonexistent file must error")
	}
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("rules:\n  - {id: x, action: explode}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPolicy(bad); err == nil {
		t.Fatalf("bad action must error")
	}
}

func TestLIRing(t *testing.T) {
	r := NewLIRing(4)
	for i := 0; i < 10; i++ {
		r.Push(LIEvent{SessionID: uint32(i), Ts: time.Now()})
	}
	out := r.Drain()
	if len(out) != 4 {
		t.Fatalf("ring size = %d, want 4", len(out))
	}
	if out[0].SessionID != 6 || out[3].SessionID != 9 {
		t.Fatalf("order broken: %v..%v", out[0].SessionID, out[3].SessionID)
	}
	if len(r.Drain()) != 0 {
		t.Fatalf("drain must empty the ring")
	}
}

func TestEngine(t *testing.T) {
	policy, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	e := NewEngine(policy, "0-1", 16)
	payload := udpDatagram("10.0.0.1", "10.99.5.7", 4321, 53, dnsMsg("example.com"))

	v := e.Inspect("N3", "UL", 0, 0x10000001, "192.0.2.1:2152", "upf-0", payload)
	if !v.Allow || v.AppID != AppDNS {
		t.Fatalf("verdict: %+v", v)
	}
	dropped := e.Inspect("N3", "UL", 2, 0x10000002, "192.0.2.1:2152", "upf-0", payload)
	if !dropped.Allow {
		t.Fatalf("no policy: must allow")
	}
	_ = dropped

	rows := e.Agg().Snapshot()
	if len(rows) != 1 || rows[0].Pkts != 2 || rows[0].Dir != "UL" || rows[0].IFace != "N3" {
		t.Fatalf("agg rows: %+v", rows)
	}
	li := e.LIEvents()
	if len(li) != 1 || li[0].SessionID != 0 || li[0].AppName != "dns" {
		t.Fatalf("li events: %+v", li)
	}
	pol := e.PolicyEvents()
	if len(pol) != 0 {
		t.Fatalf("policy events must be empty: %+v", pol)
	}
}

func TestEnginePolicyDrop(t *testing.T) {
	rule := &Policy{Rules: []Rule{{ID: "all-drop", Action: "drop"}}}
	if err := rule.Rules[0].compile(); err != nil {
		t.Fatal(err)
	}
	e := NewEngine(rule, "", 16)
	payload := udpDatagram("10.0.0.1", "10.99.5.7", 4321, 53, dnsMsg("example.com"))
	v := e.Inspect("N3", "UL", 0, 0x10000001, "192.0.2.1:2152", "upf-0", payload)
	if v.Allow || v.RuleID != "all-drop" {
		t.Fatalf("verdict: %+v", v)
	}
	pol := e.PolicyEvents()
	if len(pol) != 1 || pol[0].Action != Drop || pol[0].SPI.DstPort != 53 {
		t.Fatalf("policy events: %+v", pol)
	}
	rows := e.Agg().Snapshot()
	if len(rows) != 1 || rows[0].Dropped != 1 {
		t.Fatalf("agg rows: %+v", rows)
	}
}

func TestParseSelection(t *testing.T) {
	sets := map[string]map[uint32]bool{
		"":          {},
		"5":         {5: true},
		"0-3,42":    {0: true, 1: true, 2: true, 3: true, 42: true},
		"garbage,7": {7: true},
	}
	for spec, want := range sets {
		got := parseSelection(spec)
		if len(got) != len(want) {
			t.Fatalf("%q: got %v want %v", spec, got, want)
		}
		for k := range want {
			if !got[k] {
				t.Fatalf("%q: missing %d", spec, k)
			}
		}
	}
}
