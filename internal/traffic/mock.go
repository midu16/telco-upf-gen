package traffic

// Synthetic user datagrams with realistic inner stacks, so the UPF's
// SPI/DPI pipeline classifies genuine-looking traffic instead of filler.
//
// Each flow carries one application profile (chosen from the F-TEID), and
// per-packet fields (DNS transaction id, HTTP path, TCP/QUIC sequence) vary
// with the packet sequence number. Checksums are computed so the inner
// datagrams are wire-valid IPv4/TCP/UDP.

import (
	"encoding/binary"
	"fmt"
	"net"
)

// MockApp is the application a flow's datagrams emulate.
type MockApp int

const (
	MockHTTP MockApp = iota
	MockHTTPS
	MockDNS
	MockQUIC
	MockPlain
)

// mockAppOf derives the flow's application from the F-TEID. Consecutive
// sessions in the shared space therefore cycle through the app set, which
// is exactly what a per-app DPI dashboard wants to see.
func mockAppOf(teid uint32) MockApp { return MockApp(teidSeq16(teid) % 5) }

// fakeIP builds one inner user datagram for the flow identified by teid.
// The IPv4 source is the generator's real egress address (so SPI sees a
// genuine sender) and the destination is deterministic per session.
func fakeIP(local net.Addr, teid uint32, packetSeq uint32) []byte {
	src := net.IPv4(10, 10, 9, 9)
	if la, ok := local.(*net.UDPAddr); ok && la.IP.To4() != nil {
		src = la.IP.To4()
	}
	dst := net.IPv4(10, 99, byte(teidSeq16(teid)>>8), byte(teidSeq16(teid)))

	var l4 []byte
	var proto uint8
	src4 := src.To4()
	dst4 := dst.To4()
	switch app := mockAppOf(teid); app {
	case MockHTTP:
		proto = 6
		l4 = tcpSegment(src4, dst4, 41000+spread(packetSeq), 80, httpRequest(packetSeq), 0x18)
	case MockHTTPS:
		proto = 6
		l4 = tcpSegment(src4, dst4, 41000+spread(packetSeq), 443, tlsClientHello("example.com"), 0x18)
	case MockDNS:
		proto = 17
		l4 = udpSegment(src4, dst4, 53000+spread(packetSeq), 53, dnsQuery("example.com", packetSeq))
	case MockQUIC:
		proto = 17
		l4 = udpSegment(src4, dst4, 42000+spread(packetSeq), 443, quicLongHeader(packetSeq))
	default:
		proto = 17
		l4 = udpSegment(src4, dst4, 5000+spread(packetSeq), 6000, plainPayload(packetSeq))
	}

	total := 20 + len(l4)
	hdr := ipHeader(src, dst, proto, total)
	return append(hdr, l4...)
}

// spread keeps mock ports inside their ranges while varying per packet.
func spread(seq uint32) uint16 { return uint16(seq % 1000) }

// ipHeader builds a 20-byte IPv4 header with a correct checksum.
func ipHeader(src, dst net.IP, proto uint8, totalLength int) []byte {
	h := make([]byte, 20)
	h[0] = 0x45
	h[8] = 64 // ttl
	h[9] = proto
	binary.BigEndian.PutUint16(h[2:4], uint16(totalLength))
	binary.BigEndian.PutUint16(h[6:8], 0x4000) // DF
	copy(h[12:16], src.To4())
	copy(h[16:20], dst.To4())
	ck := uint16(^ipSum(h))
	h[4] = byte(ck >> 8)
	h[5] = byte(ck)
	return h
}

// tcpSegment builds a 20-byte TCP header (no options) + payload with a
// valid header-and-payload checksum over the IPv4 pseudo-header.
func tcpSegment(src, dst net.IP, srcPort, dstPort uint16, payload []byte, flags byte) []byte {
	total := 20 + len(payload)
	seg := make([]byte, total)
	binary.BigEndian.PutUint16(seg[0:2], srcPort)
	binary.BigEndian.PutUint16(seg[2:4], dstPort)
	binary.BigEndian.PutUint16(seg[12:14], 5<<12) // data offset = 5 words
	seg[13] = flags
	binary.BigEndian.PutUint16(seg[14:16], 65535) // window
	copy(seg[20:], payload)
	binary.BigEndian.PutUint16(seg[16:18], tcpUDPChecksum(src, dst, 6, seg))
	return seg
}

// udpSegment builds an 8-byte UDP header + payload with length and
// checksum filled over the IPv4 pseudo-header.
func udpSegment(src, dst net.IP, srcPort, dstPort uint16, payload []byte) []byte {
	total := 8 + len(payload)
	seg := make([]byte, total)
	binary.BigEndian.PutUint16(seg[0:2], srcPort)
	binary.BigEndian.PutUint16(seg[2:4], dstPort)
	binary.BigEndian.PutUint16(seg[4:6], uint16(total))
	copy(seg[8:], payload)
	binary.BigEndian.PutUint16(seg[6:8], tcpUDPChecksum(src, dst, 17, seg))
	return seg
}

// tcpUDPChecksum computes the one's-complement TCP/UDP checksum including
// the IPv4 pseudo-header. A zero result is transmitted as 0xffff.
func tcpUDPChecksum(src, dst net.IP, proto uint8, seg []byte) uint16 {
	pseudo := make([]byte, 12)
	copy(pseudo[0:4], src.To4())
	copy(pseudo[4:8], dst.To4())
	pseudo[8] = proto
	binary.BigEndian.PutUint16(pseudo[10:12], uint16(len(seg)))
	total := uint32(ipSum(pseudo)) + uint32(ipSum(seg))
	for total>>16 != 0 {
		total = (total & 0xffff) + (total >> 16)
	}
	ck := ^uint16(total)
	if ck == 0 {
		ck = 0xffff
	}
	return ck
}

// httpRequest is one HTTP/1.1 request; the path varies per packet.
func httpRequest(seq uint32) []byte {
	return []byte(fmt.Sprintf("GET /page-%05d HTTP/1.1\r\nHost: example.com\r\nUser-Agent: telco-upf-gen/1.0\r\n\r\n", seq%100000))
}

// dnsQuery is one A-record query for name, transaction id from seq.
func dnsQuery(name string, seq uint32) []byte {
	var labels []byte
	for _, part := range splitLabel(name) {
		labels = append(labels, byte(len(part)))
		labels = append(labels, part...)
	}
	labels = append(labels, 0)
	q := make([]byte, 12, 12+len(labels)+4)
	binary.BigEndian.PutUint16(q[0:2], uint16(seq)&0xffff)
	q[2], q[3] = 0x01, 0x00               // flags: RD
	binary.BigEndian.PutUint16(q[4:6], 1) // qdcount
	q = append(q, labels...)
	q = append(q, 0x00, 0x01, 0x00, 0x01) // type A class IN
	return q
}

// tlsClientHello is a truncated TLS 1.2 ClientHello carrying SNI for host.
// It follows the real byte layout (header, version, random, session id,
// cipher suites, extensions) closely enough for a bounded SNI scan.
func tlsClientHello(host string) []byte {
	const randLen = 32
	sniName := []byte(host)
	// server_name entry: name_type(1)=0, name_length(2), name
	// Wire-exact SNI extension, matching the scanner in internal/dpi:
	//   [00 02]              extension type
	//   [ext len = 17]       length of extension data
	//   [list len = 15]      server_name_list length
	//   [00] [name len = 2B] name type + length
	//   <name>
	entry := append([]byte{0x00, 0x00, byte(len(sniName))}, sniName...)
	extData := make([]byte, 0, 2+len(entry))
	extData = append(extData, byte(len(entry)>>8), byte(len(entry)))
	extData = append(extData, entry...)
	ext := make([]byte, 0, 2+2+len(extData))
	ext = append(ext, 0x00, 0x02)
	ext = append(ext, byte(len(extData)>>8), byte(len(extData)))
	ext = append(ext, extData...)

	suites := []uint16{0x1301, 0x1302, 0x1303, 0xc02b, 0xc02c, 0xc030, 0x002f, 0x009c}
	suiteBytes := make([]byte, 2+len(suites)*2)
	binary.BigEndian.PutUint16(suiteBytes[0:2], uint16(len(suites)*2))
	for i, s := range suites {
		binary.BigEndian.PutUint16(suiteBytes[2+i*2:], s)
	}

	inner := 2 + randLen + 1 + len(suiteBytes) + 2 + len(ext)
	handshake := make([]byte, 0, 4+inner+2)
	handshake = append(handshake, 0x01, 0x00, byte(inner>>8), byte(inner))
	handshake = append(handshake, 0x03, 0x03) // client version TLS1.2
	handshake = append(handshake, randBytes(randLen, 0xC0FFEE)...)
	handshake = append(handshake, 0x00) // empty session id
	handshake = append(handshake, suiteBytes...)
	handshake = append(handshake, byte(len(ext)>>8), byte(len(ext)))
	handshake = append(handshake, ext...)

	rec := make([]byte, 0, 5+len(handshake)+2)
	rec = append(rec, 0x16, 0x03, 0x01) // handshake, ver
	binary.BigEndian.PutUint16(rec, uint16(len(handshake)+2))
	rec = append(rec, 0x00, 0x00) // legacy record version
	rec = append(rec, handshake...)
	return rec
}

// quicLongHeader is a minimal QUIC v1 long-header packet (C-version
// prefix, 2-byte DCID) plus filler, first byte marked long-header.
func quicLongHeader(seq uint32) []byte {
	p := make([]byte, 24)
	p[0] = 0x40                           // long header (v1 form)
	binary.BigEndian.PutUint32(p[1:5], 1) // version v1
	p[5] = 0x02                           // DCID length
	p[6] = byte(seq >> 8)
	p[7] = byte(seq)
	for i := 8; i < len(p); i++ {
		p[i] = byte(i)
	}
	return p
}

// plainPayload is an incrementing low-bit pattern (no protocol); the first
// byte stays < 0x40 so it can never masquerade as a QUIC long header.
func plainPayload(seq uint32) []byte {
	p := make([]byte, 64)
	for i := range p {
		p[i] = byte((seq + uint32(i)) & 0x1f)
	}
	return p
}

// randBytes is a deterministic LCG (mock random, no crypto needed).
func randBytes(n int, seed uint32) []byte {
	out := make([]byte, n)
	for i := range out {
		seed = seed*1664525 + 1013904223
		out[i] = byte(seed >> 16)
	}
	return out
}

// splitLabel splits a dotted name into labels.
func splitLabel(name string) []string {
	var out []string
	cur := ""
	for _, c := range name {
		if c == '.' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(c)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
