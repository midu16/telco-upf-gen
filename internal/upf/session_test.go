package upf

import "testing"

func TestTeidArithmetic(t *testing.T) {
	cases := []struct {
		ns, node, seq uint32
		want          uint32
	}{
		{TeidNSN3, 0, 1, 0x10000001},
		{TeidNSN3, 3, 0x1234, 0x10031234},
		{TeidNSN5, 255, 0xffff, 0x20ffffff},
		{TeidNSN9, 1, 42, 0x4001002a},
	}
	for _, c := range cases {
		tt := NewTeid(c.ns, c.node, c.seq)
		if tt != c.want {
			t.Fatalf("NewTeid(%#x,%d,%#x) = %#x, want %#x", c.ns, c.node, c.seq, tt, c.want)
		}
		if teidNS(tt) != c.ns || teidNode(tt) != c.node || teidSeq(tt) != c.seq {
			t.Fatalf("decode(%#x) mismatch", tt)
		}
	}
}

func TestTableProvisionsIdenticalSpace(t *testing.T) {
	a := NewTable(64)
	b := NewTable(64)
	if a.Size() != 64 || b.Size() != 64 {
		t.Fatal("size mismatch")
	}
	sa, ok := a.Get(41)
	if !ok || sa.PDU != "pdu-0041" {
		t.Fatalf("Get(41) = %v, %v", sa, ok)
	}
	sb, _ := b.Get(41)
	if sa.N3Teid(2) != sb.N3Teid(2) || sa.N9Teid(2) != sb.N9Teid(2) {
		t.Fatal("two nodes derived different TEIDs for the same session")
	}
	if sa.N3Teid(0) == sa.N3Teid(1) {
		t.Fatal("TEID must encode the node identity")
	}
	if _, ok := a.Get(65); ok {
		t.Fatal("out-of-range session must not exist")
	}
}
