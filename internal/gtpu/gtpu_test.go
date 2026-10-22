package gtpu

import (
	"bytes"
	"testing"
)

func TestRoundTripBase(t *testing.T) {
	payload := bytes.Repeat([]byte{0xab}, 148)
	p := NewPacket(0x00010203, payload)

	buf := p.Marshal()
	if len(buf) != BaseHeaderLen+len(payload) {
		t.Fatalf("size = %d, want %d", len(buf), BaseHeaderLen+len(payload))
	}
	if buf[0] != 0x30 || buf[1] != 0xFF {
		t.Fatalf("header bytes = %02x %02x, want 30 ff", buf[0], buf[1])
	}
	if !IsGTPU(buf) {
		t.Fatal("IsGTPU rejected our own packet")
	}

	got, err := Parse(buf)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.Hdr.Teid != 0x00010203 {
		t.Fatalf("teid = %#x, want 0x00010203", got.Hdr.Teid)
	}
	if got.Hdr.Seq != nil {
		t.Fatal("seq header present but not expected")
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatal("payload mismatch after round trip")
	}
}

func TestRoundTripSequence(t *testing.T) {
	payload := bytes.Repeat([]byte{0xcd}, 64)
	seq := uint16(0xbeef)
	p := &Packet{Hdr: Header{Teid: 0x10000042, Seq: &seq}, Payload: payload}

	buf := p.Marshal()
	if len(buf) != BaseHeaderLen+SeqExtLen+len(payload) {
		t.Fatalf("size = %d, want %d", len(buf), BaseHeaderLen+SeqExtLen+len(payload))
	}
	if buf[0]&(flagS) == 0 {
		t.Fatal("S flag not set on encode")
	}

	got, err := Parse(buf)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got.Hdr.Seq == nil || *got.Hdr.Seq != seq {
		t.Fatalf("seq = %v, want %d", got.Hdr.Seq, seq)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatal("payload mismatch after round trip")
	}
	if !IsGTPU(buf) {
		t.Fatal("IsGTPU rejected seq packet")
	}
}

func TestParseTruncated(t *testing.T) {
	if _, err := Parse([]byte{0x30, 0xff, 0x00}); err != ErrTooShort {
		t.Fatalf("err = %v, want ErrTooShort", err)
	}
}

func TestParseWrongVersion(t *testing.T) {
	buf := bytes.Repeat([]byte{0}, 16)
	buf[0] = 0x00 // VER = 000
	if _, err := Parse(buf); err != ErrVersion {
		t.Fatalf("err = %v, want ErrVersion", err)
	}
	// Control-plane version bits.
	buf[0] = 0x40
	if _, err := Parse(buf); err != ErrVersion {
		t.Fatalf("err = %v, want ErrVersion", err)
	}
}

func TestParseNotGPDU(t *testing.T) {
	buf := bytes.Repeat([]byte{0}, 8)
	buf[0] = 0x30
	buf[1] = 0x10 // not a G-PDU
	if _, err := Parse(buf); err != ErrNotGPDU {
		t.Fatalf("err = %v, want ErrNotGPDU", err)
	}
}

func TestParseBadLength(t *testing.T) {
	p := NewPacket(1, []byte{1, 2, 3, 4})
	buf := p.Marshal()
	buf[3]++ // corrupt declared payload length
	if _, err := Parse(buf); err != ErrLengthBad {
		t.Fatalf("err = %v, want ErrLengthBad", err)
	}
	if IsGTPU(buf) {
		t.Fatal("IsGTPU accepted a length-corrupted packet")
	}
}

func TestParseZeroLength(t *testing.T) {
	p := NewPacket(7, nil)
	buf := p.Marshal()
	got, err := Parse(buf)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(got.Payload))
	}
}

func TestIsGTPURejectsGarbage(t *testing.T) {
	for _, b := range [][]byte{
		nil,
		{},
		{0x30},
		{0x30, 0xff, 0x00, 0x00, 0x00, 0x00},
		bytes.Repeat([]byte{0xde}, 20),
	} {
		if IsGTPU(b) {
			t.Fatalf("IsGTPU accepted % x", b)
		}
	}
}
