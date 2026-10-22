// Package gtpu implements the bare GTP-U (user plane) protocol used on the
// 3GPP 5G N3, N5/N6 and N9 tunnels (3GPP TS 29.281, RFC 8180).
//
// Wire layout of a G-PDU (base header, no extension headers):
//
//	octet 0  : VER(3)=001 | T/U(1)=1 | P(1)=0 | S(1)=0 | R(4)=0   -> 0x30
//	octet 1  : type (0xFF = G-PDU / user data)
//	octet 2-3: length of the G-PDU payload in octets, big-endian
//	octet 4-7: F-TEID, big-endian
//	octet 8+ : payload (an IPv4 user datagram)
//
// With the S flag set, one 6-octet sequence-number extension header is
// appended after the F-TEID:
//
//	[type=4] [length-in-4-octet-units=1] [2 reserved] [seq u16]
//
// The package is allocation-light on the decode path (Parse works in place
// over the caller's buffer) so it is safe to use inside per-packet hot loops.
package gtpu

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Version is the 3-bit VER field value for GTPv1 (covers GTPv1-U).
	Version uint8 = 0x01
	// TUUserPlane is the T/U bit value marking user-plane messages.
	TUUserPlane uint8 = 0x01
	// BaseHeaderOctet0 is the fully populated first header octet for a
	// base user-plane G-PDU.
	BaseHeaderOctet0 byte = (Version<<5 | TUUserPlane<<4) & 0xff // 0x30

	// MessageGPDU marks a G-PDU (user data) message.
	MessageGPDU uint8 = 0xFF

	// BaseHeaderLen is the GTP-U header size without extension headers.
	BaseHeaderLen = 8
	// SeqExtLen is the size in octets of the sequence-number extension header.
	SeqExtLen = 6
	// SeqExtType is the extension header type of the GTP-U sequence header.
	SeqExtType uint8 = 4
)

// flagS is the S bit of octet 0: a sequence-number extension header follows.
const flagS byte = 0x04

var (
	ErrTooShort  = errors.New("gtpu: packet shorter than base header")
	ErrVersion   = errors.New("gtpu: not a GTPv1-U user-plane packet")
	ErrNotGPDU   = errors.New("gtpu: message type is not a G-PDU (0xFF)")
	ErrExtShort  = errors.New("gtpu: sequence extension header truncated")
	ErrExtType   = errors.New("gtpu: unexpected first extension header type")
	ErrLengthBad = errors.New("gtpu: declared G-PDU length does not match packet")
)

// Header is a parsed GTP-U header.
type Header struct {
	Teid uint32
	Seq  *uint16 // non-nil only when the S flag is set
}

func (h Header) totalLen() int {
	n := BaseHeaderLen
	if h.Seq != nil {
		n += SeqExtLen
	}
	return n
}

// Packet is a full G-PDU: header plus payload.
type Packet struct {
	Hdr     Header
	Payload []byte
}

// NewPacket builds a G-PDU for the given F-TEID and user payload.
func NewPacket(teid uint32, payload []byte) *Packet {
	return &Packet{Hdr: Header{Teid: teid}, Payload: payload}
}

// Size is the on-wire size of the encoded packet in octets.
func (p *Packet) Size() int {
	return p.Hdr.totalLen() + len(p.Payload)
}

// Marshal encodes the full G-PDU into a fresh buffer.
func (p *Packet) Marshal() []byte {
	return p.MarshalTo(nil)
}

// MarshalTo encodes the full G-PDU, appending to buf and returning the whole
// packet as a fresh slice (buf may be reused as capacity).
func (p *Packet) MarshalTo(buf []byte) []byte {
	start := len(buf)
	h := p.Hdr

	buf = append(buf, BaseHeaderOctet0)
	if h.Seq != nil {
		buf[len(buf)-1] |= flagS
	}
	buf = append(buf, MessageGPDU)
	l := uint16(len(p.Payload))
	buf = append(buf, byte(l>>8), byte(l))
	var teid [4]byte
	binary.BigEndian.PutUint32(teid[:], h.Teid)
	buf = append(buf, teid[:]...)
	if h.Seq != nil {
		buf = append(buf, SeqExtType, 0x01, 0x00, 0x00)
		var s [2]byte
		binary.BigEndian.PutUint16(s[:], *h.Seq)
		buf = append(buf, s[:]...)
	}
	buf = append(buf, p.Payload...)
	return buf[start:]
}

// Parse decodes b as a GTP-U G-PDU. The returned payload references the
// caller's buffer (no copy), so the caller must keep b alive for as long as
// it uses the returned Packet.
func Parse(b []byte) (Packet, error) {
	if len(b) < BaseHeaderLen {
		return Packet{}, ErrTooShort
	}
	if b[0]&0xe0 != (Version<<5) || b[0]&(TUUserPlane<<4) == 0 {
		return Packet{}, ErrVersion
	}
	if b[1] != MessageGPDU {
		return Packet{}, ErrNotGPDU
	}
	length := binary.BigEndian.Uint16(b[2:4])
	p := Packet{Hdr: Header{Teid: binary.BigEndian.Uint32(b[4:8])}}
	off := BaseHeaderLen
	if b[0]&flagS != 0 {
		if len(b) < off+SeqExtLen {
			return Packet{}, ErrExtShort
		}
		if b[off] != SeqExtType {
			return Packet{}, fmt.Errorf("%w: 0x%02x (want 0x%02x)", ErrExtType, b[off], SeqExtType)
		}
		seq := binary.BigEndian.Uint16(b[off+4 : off+6])
		p.Hdr.Seq = &seq
		off += SeqExtLen
	}
	if int(length) != len(b)-off {
		return Packet{}, ErrLengthBad
	}
	p.Payload = b[off:]
	return p, nil
}

// IsGTPU reports whether b looks like a well-formed GTP-U G-PDU. It is a
// cheap validity probe used for sniffer-style checks; full decoding is Parse.
func IsGTPU(b []byte) bool {
	if len(b) < BaseHeaderLen {
		return false
	}
	if b[0]&0xe0 != (Version<<5) || b[0]&(TUUserPlane<<4) == 0 {
		return false
	}
	if b[1] != MessageGPDU {
		return false
	}
	declared := int(binary.BigEndian.Uint16(b[2:4]))
	off := BaseHeaderLen
	if b[0]&flagS != 0 {
		if len(b) < off+SeqExtLen {
			return false
		}
		off += SeqExtLen
	}
	return declared == len(b)-off
}
