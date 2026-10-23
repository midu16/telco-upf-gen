// Package usage tracks per-session data volume and duration, and closes
// sessions into CDRs (Call Detail Records) when they go idle. It is the
// billing-side counterpart of the DPI engine: the engine classifies, the
// ledger accounts.
//
// Design notes:
//   - Sessions are keyed by F-TEID session sequence (seq 0..255 in the
//     default fabric). Every node provisions the same shared session space,
//     so a CDR attributes volume to (pod, seq).
//   - A packet is accounted exactly once, at the UPF node that sees it on
//     N3 (uplink, dir=UL) or N5 (downlink, dir=DL). N9 is pure transit and
//     never re-accounts, so cross-node sessions do not double count.
//   - A session closes when it stays idle for longer than the ledger's
//     idle timeout (and lived at least min duration). Closed CDRs are
//     buffered and drained by the control-lite /api/cdrs endpoint, which the
//     webui ships to the database. CDRs upsert idempotently on the natural
//     key, so a re-report is always safe.
package usage

import (
	"sync"
	"time"
)

// DirUL / DirDL are the traffic directions recorded per session.
const (
	DirUL = "UL"
	DirDL = "DL"
)

// CDR is one closed session billing record (billing format).
type CDR struct {
	SessionID  uint64    `json:"sessionId"`
	NodeID     int16     `json:"nodeId"`
	AnchorPod  string    `json:"anchorPod"`
	UeAddr     string    `json:"ueAddr,omitempty"`
	DnAddr     string    `json:"dnAddr,omitempty"`
	UpPkts     uint64    `json:"upPkts"`
	DownPkts   uint64    `json:"downPkts"`
	UpBytes    uint64    `json:"upBytes"`
	DownBytes  uint64    `json:"downBytes"`
	StartedAt  time.Time `json:"startedAt"`
	EndedAt    time.Time `json:"endedAt"`
	DurationMS int64     `json:"durationMs"`
	// Apps maps app ID to [pkts, bytes], both directions combined.
	Apps   map[uint16][2]uint64 `json:"apps"`
	Rating string               `json:"rating"`
	Status string               `json:"status"`
}

type session struct {
	seq       uint32
	startedAt time.Time
	lastSeen  time.Time
	ueAddr    string
	dnAddr    string

	upPkts, downPkts   uint64
	upBytes, downBytes uint64
	apps               map[uint16][2]uint64
}

type shard struct {
	mu sync.Mutex
	m  map[uint32]*session
}

// Ledger holds the in-flight session accounts for one UPF node.
type Ledger struct {
	podName string
	nodeID  int16
	idle    time.Duration
	min     time.Duration

	shards [8]shard

	pendingMu sync.Mutex
	pending   []CDR
	closed    uint64
}

// maxPending bounds buffered CDRs between drains so a long webui outage
// cannot grow memory without limit (oldest CDRs are dropped first).
const maxPending = 4096

// NewLedger builds a ledger for one node. idle is the inactivity window that
// closes a session; min is the minimum lifetime before a session may close
// (guards against accounting a flash-of-one-packet).
func NewLedger(podName string, nodeID int16, idle, min time.Duration) *Ledger {
	if idle <= 0 {
		idle = 15 * time.Second
	}
	if min <= 0 {
		min = time.Second
	}
	l := &Ledger{podName: podName, nodeID: nodeID, idle: idle, min: min}
	for i := range l.shards {
		l.shards[i].m = make(map[uint32]*session)
	}
	return l
}

func (l *Ledger) shardFor(seq uint32) *shard {
	return &l.shards[(seq*2654435761)%uint32(len(l.shards))]
}

// Touch records one accounted user packet for the session. dir is UL or DL;
// app is the DPI classification id; pkBytes the user-payload length; srcIP
// and dstIP the inner endpoints (empty = unknown).
func (l *Ledger) Touch(seq uint32, dir string, app uint16, pkBytes uint64, srcIP, dstIP string) {
	sh := l.shardFor(seq)
	now := time.Now()
	sh.mu.Lock()
	s, ok := sh.m[seq]
	if !ok {
		s = &session{seq: seq, startedAt: now, apps: make(map[uint16][2]uint64)}
		sh.m[seq] = s
	}
	s.lastSeen = now
	if dir == DirUL {
		s.upPkts++
		s.upBytes += pkBytes
		if s.ueAddr == "" && srcIP != "" {
			s.ueAddr = srcIP
		}
		if s.dnAddr == "" && dstIP != "" {
			s.dnAddr = dstIP
		}
	} else {
		s.downPkts++
		s.downBytes += pkBytes
		if s.dnAddr == "" && srcIP != "" {
			s.dnAddr = srcIP
		}
		if s.ueAddr == "" && dstIP != "" {
			s.ueAddr = dstIP
		}
	}
	a := s.apps[app]
	a[0]++
	a[1] += pkBytes
	s.apps[app] = a
	sh.mu.Unlock()
}

// Active returns the number of in-flight sessions.
func (l *Ledger) Active() int {
	total := 0
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		total += len(sh.m)
		sh.mu.Unlock()
	}
	return total
}

// CloseIdle sweeps the ledger once and appends newly closed sessions to the
// pending CDR queue. Call it periodically (2-5 s cadence is enough: the
// idle resolution is coarse by design).
func (l *Ledger) CloseIdle(now time.Time) {
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for seq, s := range sh.m {
			alive := now.Sub(s.startedAt)
			if alive < l.min || now.Sub(s.lastSeen) < l.idle {
				continue
			}
			cdr := CDR{
				SessionID:  uint64(seq),
				NodeID:     l.nodeID,
				AnchorPod:  l.podName,
				UeAddr:     s.ueAddr,
				DnAddr:     s.dnAddr,
				UpPkts:     s.upPkts,
				DownPkts:   s.downPkts,
				UpBytes:    s.upBytes,
				DownBytes:  s.downBytes,
				StartedAt:  s.startedAt,
				EndedAt:    s.lastSeen,
				DurationMS: alive.Milliseconds(),
				Apps:       s.apps,
				Rating:     "sim-rated",
				Status:     "closed",
			}
			delete(sh.m, seq)
			l.pendingMu.Lock()
			l.pending = append(l.pending, cdr)
			if len(l.pending) > maxPending {
				l.pending = l.pending[len(l.pending)-maxPending:]
			}
			l.pendingMu.Unlock()
		}
		sh.mu.Unlock()
	}
}

// HasPending reports whether closed CDRs await drain.
func (l *Ledger) HasPending() bool {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	return len(l.pending) > 0
}

// PendingCount returns the buffered CDR count.
func (l *Ledger) PendingCount() int {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	return len(l.pending)
}

// DrainCDRs returns and clears the buffered closed CDRs.
func (l *Ledger) DrainCDRs() []CDR {
	l.pendingMu.Lock()
	out := l.pending
	l.pending = nil
	l.closed += uint64(len(out))
	l.pendingMu.Unlock()
	return out
}

// ClosedTotal returns the lifetime closed-CDR count.
func (l *Ledger) ClosedTotal() uint64 {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	return l.closed
}
