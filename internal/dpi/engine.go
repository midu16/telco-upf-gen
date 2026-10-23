// Engine wires SPI, DPI classification, the policy and the Lawful
// Interception capture into one per-packet call executed by the UPF
// forwarding workers. It is allocation-conscious on the hot path:
// SPI/classify are value-scoped, counters are sharded, and event capture
// is ring-bounded.
package dpi

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PolicyEv is one policy enforcement occurrence (observe or drop).
type PolicyEv struct {
	IFace      string    `json:"iface"`
	Dir        string    `json:"direction"` // UL | DL
	OuterSrc   string    `json:"outerSrc"`
	OuterDst   string    `json:"outerDst"`
	SPI        SPI       `json:"spi"`
	AppID      AppID     `json:"appId"`
	AppName    string    `json:"appName"`
	RuleID     string    `json:"ruleId"`
	Action     Action    `json:"action"`
	Detail     string    `json:"detail,omitempty"`
	PayloadLen int32     `json:"payloadLen"`
	Ts         time.Time `json:"ts"`
}

// LIEvent is one captured header-metadata record for a selected session.
type LIEvent struct {
	SessionID  uint32    `json:"sessionId"`
	TEID       uint32    `json:"teid"`
	Dir        string    `json:"direction"`
	IFace      string    `json:"iface"`
	SPI        SPI       `json:"spi"`
	AppID      AppID     `json:"appId"`
	AppName    string    `json:"appName"`
	Detail     string    `json:"detail,omitempty"`
	PayloadLen int32     `json:"payloadLen"`
	Ts         time.Time `json:"ts"`
}

// AggRow is one (iface, direction, app) aggregate in a Snapshot.
type AggRow struct {
	IFace   string `json:"iface"`
	Dir     string `json:"direction"`
	AppID   AppID  `json:"appId"`
	AppName string `json:"appName"`
	Pkts    uint64 `json:"pkts"`
	Bytes   uint64 `json:"bytes"`
	Dropped uint64 `json:"dropped"`
}

type aggKey struct {
	iface string
	dir   string
	app   AppID
}

type aggCell struct {
	pkts    uint64
	bytes   uint64
	dropped uint64
}

// Aggregator holds cumulative per-(iface,dir,app) inspection counters.
// Sharded so hot-path Add takes a small mutex.
type Aggregator struct {
	shards [8]struct {
		mu sync.Mutex
		m  map[aggKey]*aggCell
	}
}

// NewAggregator builds an empty aggregator.
func NewAggregator() *Aggregator {
	a := &Aggregator{}
	for i := range a.shards {
		a.shards[i].m = make(map[aggKey]*aggCell)
	}
	return a
}

// hashStr is a small fixed-point string hash for shard selection.
func hashStr(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
}

// Add records one inspected packet.
func (a *Aggregator) Add(iface, dir string, app AppID, pktBytes uint64, dropped bool) {
	sh := &a.shards[(hashStr(iface)^hashStr(dir)|uint32(app))&7]
	sh.mu.Lock()
	c, ok := sh.m[aggKey{iface, dir, app}]
	if !ok {
		c = &aggCell{}
		sh.m[aggKey{iface, dir, app}] = c
	}
	c.pkts++
	c.bytes += pktBytes
	if dropped {
		c.dropped++
	}
	sh.mu.Unlock()
}

// Total returns the cumulative inspected packet count across all shards.
func (a *Aggregator) TotalPkts() uint64 {
	var t uint64
	for i := range a.shards {
		sh := &a.shards[i]
		sh.mu.Lock()
		for _, c := range sh.m {
			t += c.pkts
		}
		sh.mu.Unlock()
	}
	return t
}

// Snapshot returns the current aggregates sorted for stable rendering.
func (a *Aggregator) Snapshot() []AggRow {
	var out []AggRow
	for i := range a.shards {
		sh := &a.shards[i]
		sh.mu.Lock()
		for k, c := range sh.m {
			out = append(out, AggRow{
				IFace:   k.iface,
				Dir:     k.dir,
				AppID:   k.app,
				AppName: k.app.Name(),
				Pkts:    c.pkts,
				Bytes:   c.bytes,
				Dropped: c.dropped,
			})
		}
		sh.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IFace != out[j].IFace {
			return out[i].IFace < out[j].IFace
		}
		if out[i].Dir != out[j].Dir {
			return out[i].Dir < out[j].Dir
		}
		return out[i].AppID < out[j].AppID
	})
	return out
}

// LIRing is a bounded, ordered capture ring for selected sessions.
type LIRing struct {
	mu  sync.Mutex
	buf []LIEvent
	pos int
	cap int
}

// NewLIRing creates a ring of the given capacity (rounded up to >= 1).
func NewLIRing(cap int) *LIRing {
	if cap < 1 {
		cap = 1
	}
	return &LIRing{buf: make([]LIEvent, cap), cap: cap}
}

// Push appends an event, evicting the oldest when full.
func (r *LIRing) Push(e LIEvent) {
	r.mu.Lock()
	r.buf[r.pos] = e
	r.pos = (r.pos + 1) % r.cap
	r.mu.Unlock()
}

// Drain returns the captured events oldest-first and empties the ring.
func (r *LIRing) Drain() []LIEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LIEvent, 0, r.cap)
	for i := 0; i < r.cap; i++ {
		idx := (r.pos + i) % r.cap
		e := r.buf[idx]
		if e.Ts.IsZero() {
			continue
		}
		out = append(out, e)
	}
	// reset
	for i := range r.buf {
		var zero LIEvent
		r.buf[i] = zero
	}
	r.pos = 0
	return out
}

// Engine is the per-node inspection state.
type Engine struct {
	policy     *Policy
	agg        *Aggregator
	liSessions map[uint32]bool
	liRing     *LIRing

	polMu     sync.Mutex
	pol       []PolicyEv
	polMax    int
	inspected atomic.Uint64
}

// polMaxDefault bounds the buffered policy events between drains.
const polMaxDefault = 4096

// NewEngine builds an engine. liSpec is a session selector string like
// "0-7,42" (empty = no sessions selected). policy may be nil (allow-all).
func NewEngine(policy *Policy, liSpec string, liRingSize int) *Engine {
	if policy == nil {
		policy = &Policy{}
	}
	e := &Engine{
		policy:     policy,
		agg:        NewAggregator(),
		liSessions: parseSelection(liSpec),
		liRing:     NewLIRing(liRingSize),
		polMax:     polMaxDefault,
	}
	return e
}

// parseSelection expands "0-7,42" (or "42") into a set of session seqs.
// Malformed fragments are skipped silently; capacity is bounded to
// 2^16 - 1 session space.
func parseSelection(spec string) map[uint32]bool {
	set := make(map[uint32]bool)
	if strings.TrimSpace(spec) == "" {
		return set
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		lo, hi, ok := strings.Cut(part, "-")
		if !ok {
			if n, err := strconv.ParseUint(strings.TrimSpace(part), 10, 16); err == nil {
				set[uint32(n)] = true
			}
			continue
		}
		loN, err1 := strconv.ParseUint(strings.TrimSpace(lo), 10, 16)
		hiN, err2 := strconv.ParseUint(strings.TrimSpace(hi), 10, 16)
		if err1 != nil || err2 != nil || hiN < loN {
			continue
		}
		for n := loN; n <= hiN; n++ {
			set[uint32(n)] = true
		}
	}
	return set
}

// Selected reports whether session seq falls in the LI selection.
func (e *Engine) Selected(seq uint32) bool {
	return e.liSessions[seq]
}

// l4Payload returns the application payload of an IPv4 datagram, i.e. the
// bytes after the IP header AND the transport header (UDP 8 / TCP dataoff /
// ICMP 4). Classify works on these bytes, never the transport header.
func l4Payload(dg []byte, s SPI) []byte {
	if len(dg) < 20 || dg[0]>>4 != 4 || !s.Valid {
		return nil
	}
	ihl := int(dg[0]&0x0f) * 4
	if ihl < 20 || len(dg) < ihl {
		return nil
	}
	hdr := dg[ihl:]
	switch s.Proto {
	case ProtoUDP:
		if len(hdr) >= 8 {
			return hdr[8:]
		}
	case ProtoTCP:
		if len(hdr) >= 20 {
			do := int(hdr[12]>>4&0x0f) * 4
			if do >= 20 && len(hdr) >= do {
				return hdr[do:]
			}
		}
	case ProtoICMP:
		if len(hdr) >= 4 {
			return hdr[4:]
		}
	}
	return nil
}

// Verdict is the engine's outcome for one packet.
type Verdict struct {
	Allow   bool
	Action  Action
	RuleID  string
	SPI     SPI
	AppID   AppID
	AppName string
	Detail  string
}

// Inspect runs SPI + DPI + policy (+LI capture) on one user datagram.
// iface is the UPF interface the G-PDU arrived on (N3/N5/N9), dir the
// traffic direction (UL/DL), seq the session sequence number.
func (e *Engine) Inspect(iface, dir string, seq uint32, teid uint32, outerSrc, outerDst string, payload []byte) Verdict {
	e.inspected.Add(1)
	spi := Shallow(payload)
	l4 := l4Payload(payload, spi)
	app, detail := Classify(spi, l4)
	v := Verdict{Allow: true, Action: Allow, SPI: spi, AppID: app, AppName: app.Name(), Detail: detail}

	if rule, action := e.policy.Evaluate(spi, app); action != Allow {
		v.Action = action
		v.Allow = action != Drop
		v.RuleID = rule.ID
		e.polMu.Lock()
		// ring the buffer: drop the oldest once full
		if len(e.pol) >= e.polMax {
			e.pol = e.pol[len(e.pol)-e.polMax/2:]
		}
		e.pol = append(e.pol, PolicyEv{
			IFace:      iface,
			Dir:        dir,
			OuterSrc:   outerSrc,
			OuterDst:   outerDst,
			SPI:        spi,
			AppID:      app,
			AppName:    app.Name(),
			RuleID:     rule.ID,
			Action:     action,
			Detail:     detail,
			PayloadLen: int32(len(payload)),
			Ts:         time.Now(),
		})
		e.polMu.Unlock()
	}

	dropped := !v.Allow
	e.agg.Add(iface, dir, app, uint64(len(payload)), dropped)

	if e.Selected(seq) {
		e.liRing.Push(LIEvent{
			SessionID:  seq,
			TEID:       teid,
			Dir:        dir,
			IFace:      iface,
			SPI:        spi,
			AppID:      app,
			AppName:    app.Name(),
			Detail:     detail,
			PayloadLen: int32(len(payload)),
			Ts:         time.Now(),
		})
	}
	return v
}

// Inspected returns the cumulative inspected packet count.
func (e *Engine) Inspected() uint64 { return e.inspected.Load() }

// Agg exposes the aggregate counters.
func (e *Engine) Agg() *Aggregator { return e.agg }

// PolicyEvents drains the buffered policy events.
func (e *Engine) PolicyEvents() []PolicyEv {
	e.polMu.Lock()
	out := e.pol
	e.pol = nil
	e.polMu.Unlock()
	return out
}

// LIEvents drains the lawful-interception capture ring.
func (e *Engine) LIEvents() []LIEvent { return e.liRing.Drain() }

// Summary is the /api/dpi payload for the dashboard.
type Summary struct {
	Inspected  uint64     `json:"inspected"`
	LISelected int        `json:"liSessions"`
	Rows       []AggRow   `json:"rows"`
	Rules      []RuleInfo `json:"rules"`
}

// RuleInfo is the rendered rule for UIs.
type RuleInfo struct {
	ID     string `json:"id"`
	Desc   string `json:"description,omitempty"`
	Apps   string `json:"apps,omitempty"`
	Proto  string `json:"proto,omitempty"`
	Action string `json:"action"`
}

// Snapshot renders the current state.
func (e *Engine) Snapshot() Summary {
	sum := Summary{
		Inspected:  e.inspected.Load(),
		LISelected: len(e.liSessions),
		Rows:       e.agg.Snapshot(),
	}
	for i := range e.policy.Rules {
		r := &e.policy.Rules[i]
		info := RuleInfo{ID: r.ID, Desc: r.Description, Action: r.Action}
		if len(r.apps) > 0 {
			var names []string
			for _, a := range r.apps {
				names = append(names, a.Name())
			}
			info.Apps = strings.Join(names, ",")
		}
		if !r.anyProto {
			info.Proto = protoName(r.proto)
		}
		sum.Rules = append(sum.Rules, info)
	}
	return sum
}
