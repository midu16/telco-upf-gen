// Shallow Packet Inspection (SPI) for the GTP-U user plane.
//
// SPI decodes the headers only — IPv4, then L4 (TCP/UDP/ICMP) — without
// touching the payload. The result (a 5-tuple plus a few L4 facts) feeds
// the DPI classifier and the policy engine. All access is bounds-checked;
// Shallow never allocates and never fails (bad input yields Valid=false).
package dpi

import (
	"encoding/binary"
	"net"
)

// IP protocol numbers.
const (
	ProtoTCP  = 6
	ProtoUDP  = 17
	ProtoICMP = 1
)

// SPI is the parsed header view of one user (inner) IPv4 datagram.
type SPI struct {
	Valid      bool   `json:"valid,omitempty"`
	Proto      uint8  `json:"proto"`   // IP protocol: 6/TCP 17/UDP 1/ICMP
	Src        string `json:"src"`     // inner source, dotted quad
	Dst        string `json:"dst"`     // inner destination, dotted quad
	SrcPort    uint16 `json:"srcPort"` // 0 for ICMP
	DstPort    uint16 `json:"dstPort"` // 0 for ICMP
	ProtoName  string `json:"protoName"`
	ICMPType   uint8  `json:"icmpType,omitempty"`
	PayloadLen int    `json:"payloadLen"` // L4 payload length in octets
}

// Shallow decodes the header fields of pkt. It accepts packets shorter than
// a full L4 header; those are reported with what was decodable and Valid
// false.
func Shallow(pkt []byte) SPI {
	s := SPI{}
	if len(pkt) < 20 || pkt[0]>>4 != 4 {
		return s
	}
	ihl := int(pkt[0]&0x0f) * 4
	if ihl < 20 || len(pkt) < ihl {
		return s
	}
	s.Proto = pkt[9]
	s.Src = net.IPv4(pkt[12], pkt[13], pkt[14], pkt[15]).String()
	s.Dst = net.IPv4(pkt[16], pkt[17], pkt[18], pkt[19]).String()
	s.ProtoName = protoName(pkt[9])

	rest := pkt[ihl:]
	switch pkt[9] {
	case ProtoTCP:
		if len(rest) < 20 {
			return s
		}
		// TCP total length is not on the wire; use IP total length minus
		// headers (accurate for the single-packet datagrams this plane
		// carries; GTP-U G-PDUs are per-packet, no fragmentation in sim).
		dataOff := int(rest[12]>>4&0x0f) * 4
		if dataOff < 20 {
			return s
		}
		s.SrcPort = binary.BigEndian.Uint16(rest[0:2])
		s.DstPort = binary.BigEndian.Uint16(rest[2:4])
		pl := len(rest) - dataOff
		if pl < 0 {
			pl = 0
		}
		s.PayloadLen = pl
		s.Valid = true
	case ProtoUDP:
		if len(rest) < 8 {
			return s
		}
		s.SrcPort = binary.BigEndian.Uint16(rest[0:2])
		s.DstPort = binary.BigEndian.Uint16(rest[2:4])
		ul := binary.BigEndian.Uint16(rest[4:6])
		pl := int(ul) - 8
		if pl < 0 || len(rest) < 8+pl {
			// Declared UDP length inconsistent with packet; trust the wire.
			pl = len(rest) - 8
		}
		s.PayloadLen = pl
		s.Valid = true
	case ProtoICMP:
		if len(rest) < 4 {
			return s
		}
		s.ICMPType = rest[0]
		s.PayloadLen = len(rest) - 4
		s.Valid = true
	}
	return s
}

func protoName(p uint8) string {
	switch p {
	case ProtoTCP:
		return "tcp"
	case ProtoUDP:
		return "udp"
	case ProtoICMP:
		return "icmp"
	default:
		return "ip"
	}
}
