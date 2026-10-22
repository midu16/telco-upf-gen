package upf

import (
	"fmt"
	"sync"
)

// F-TEID namespace layout (32 bits):
//
//	bits 31-24 : namespace (N3 = 0x10, N5 = 0x20, N9 = 0x40)
//	bits 23-16 : UPF node identity (StatefulSet ordinal, 0-255)
//	bits 15-0  : session sequence (shared session index)
//
// Every UPF in the fabric provisions the *same* session space (seq 0..N-1).
// The node bits therefore act as a "session anchor" address: an inter-UPF
// packet on N9 carries the destination node, and the receiving node can
// resolve the session locally without any control-plane exchange. This is a
// deliberate simplification of the N11/anchor-UPF model, kept deterministic
// so the whole plane can be reproduced from (nodes, sessions) alone.
const (
	// TeidNSN3 marks tunnel endpoints of the 3GPP N3 interface (gNB direction).
	TeidNSN3 uint32 = 0x10
	// TeidNSN5 marks tunnel endpoints of the DN-facing interface.
	TeidNSN5 uint32 = 0x20
	// TeidNSN9 marks tunnel endpoints of the N9 interface (peer UPF direction).
	TeidNSN9 uint32 = 0x40
)

// NewTeid synthesizes an F-TEID for the given namespace, node and session.
func NewTeid(ns, node, seq uint32) uint32 {
	return ns<<24 | node<<16 | seq&0xffff
}

func teidNS(t uint32) uint32   { return t >> 24 }
func teidNode(t uint32) uint32 { return (t >> 16) & 0xff }
func teidSeq(t uint32) uint32  { return t & 0xffff }

// Session is one PDU session. F-TEIDs are derived, not stored: the same
// session has a different TEID in the N3/N5/N9 namespace of each node.
type Session struct {
	Seq uint32
	PDU string
}

func (s *Session) N3Teid(node uint32) uint32 { return NewTeid(TeidNSN3, node, s.Seq) }
func (s *Session) N5Teid(node uint32) uint32 { return NewTeid(TeidNSN5, node, s.Seq) }
func (s *Session) N9Teid(node uint32) uint32 { return NewTeid(TeidNSN9, node, s.Seq) }

// Table is the session database. It is tiny (a few thousand entries) and
// read-mostly, so a single RWMutex keeps lock contention on the hot path
// effectively zero.
type Table struct {
	mu   sync.RWMutex
	m    map[uint32]*Session
	size int
}

// NewTable provisions the shared session space, identical on every node.
func NewTable(count int) *Table {
	if count < 1 {
		count = 1
	}
	m := make(map[uint32]*Session, count)
	for i := uint32(0); i < uint32(count); i++ {
		m[i] = &Session{Seq: i, PDU: fmt.Sprintf("pdu-%04d", i)}
	}
	return &Table{m: m, size: count}
}

func (t *Table) Get(seq uint32) (*Session, bool) {
	t.mu.RLock()
	s, ok := t.m[seq]
	t.mu.RUnlock()
	return s, ok
}

func (t *Table) Size() int { return t.size }
