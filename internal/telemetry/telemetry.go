// Package telemetry provides lock-free per-node and per-interface counters
// shared by the UPF core and the traffic generator, plus a JSON-friendly
// snapshot consumed by the Web UI.
package telemetry

import (
	"sync"
	"sync/atomic"
	"time"
)

// Iface holds atomic counters for one logical UPF interface (N3, N5, N9, ...).
type Iface struct {
	Name string `json:"name"`

	RxPackets    atomic.Uint64
	RxBytes      atomic.Uint64
	TxPackets    atomic.Uint64
	TxBytes      atomic.Uint64
	Encapsulated atomic.Uint64
	Decapsulated atomic.Uint64
	Dropped      atomic.Uint64
	Errors       atomic.Uint64
}

// Gauge is an atomic int64 used for small status values (active sessions,
// open flows, ...).
type Gauge int64

func (g *Gauge) Set(v int64) { atomic.StoreInt64((*int64)(g), v) }
func (g *Gauge) Add(v int64) { atomic.AddInt64((*int64)(g), v) }
func (g *Gauge) Load() int64 { return atomic.LoadInt64((*int64)(g)) }

type gaugeEntry struct {
	mu sync.RWMutex
	g  map[string]*Gauge
}

type ifaceEntry struct {
	mu    sync.RWMutex
	iface map[string]*Iface
	order []string
}

// Node is the telemetry root of one pod (a UPF node or a traffic generator).
type Node struct {
	Name string
	Role string

	ifc   ifaceEntry
	gauge gaugeEntry
}

// NewNode creates a node with the given named interfaces (in display order).
func NewNode(name, role string, ifaces ...string) *Node {
	m := make(map[string]*Iface, len(ifaces))
	order := make([]string, 0, len(ifaces))
	for _, n := range ifaces {
		if _, ok := m[n]; ok {
			continue
		}
		m[n] = &Iface{Name: n}
		order = append(order, n)
	}
	return &Node{
		Name:  name,
		Role:  role,
		ifc:   ifaceEntry{iface: m, order: order},
		gauge: gaugeEntry{g: map[string]*Gauge{}},
	}
}

// Iface returns the counter set for the named interface, creating it on first
// use. Safe for concurrent use.
func (n *Node) Iface(name string) *Iface {
	n.ifc.mu.RLock()
	f, ok := n.ifc.iface[name]
	n.ifc.mu.RUnlock()
	if ok {
		return f
	}

	n.ifc.mu.Lock()
	defer n.ifc.mu.Unlock()
	if f, ok = n.ifc.iface[name]; ok {
		return f
	}
	f = &Iface{Name: name}
	n.ifc.iface[name] = f
	n.ifc.order = append(n.ifc.order, name)
	return f
}

// Gauge returns the named gauge, creating it on first use.
func (n *Node) Gauge(name string) *Gauge {
	n.gauge.mu.RLock()
	g, ok := n.gauge.g[name]
	n.gauge.mu.RUnlock()
	if ok {
		return g
	}

	n.gauge.mu.Lock()
	defer n.gauge.mu.Unlock()
	if g, ok = n.gauge.g[name]; ok {
		return g
	}
	g = new(Gauge)
	n.gauge.g[name] = g
	return g
}

type IfaceSnap struct {
	Name         string `json:"name"`
	RxPackets    uint64 `json:"rxPkts"`
	RxBytes      uint64 `json:"rxBytes"`
	TxPackets    uint64 `json:"txPkts"`
	TxBytes      uint64 `json:"txBytes"`
	Encapsulated uint64 `json:"encaps"`
	Decapsulated uint64 `json:"decaps"`
	Dropped      uint64 `json:"dropped"`
	Errors       uint64 `json:"errors"`
}

type NodeSnap struct {
	Node   string           `json:"node"`
	Role   string           `json:"role"`
	Ts     int64            `json:"ts"`
	Ifaces []IfaceSnap      `json:"ifaces"`
	Gauges map[string]int64 `json:"gauges,omitempty"`
}

// Snapshot captures a consistent-ish (still race-free per counter) view of
// the node for the stats API.
func (n *Node) Snapshot() NodeSnap {
	n.ifc.mu.RLock()
	ifaces := make([]IfaceSnap, 0, len(n.ifc.iface))
	for _, name := range n.ifc.order {
		f := n.ifc.iface[name]
		ifaces = append(ifaces, IfaceSnap{
			Name:         f.Name,
			RxPackets:    f.RxPackets.Load(),
			RxBytes:      f.RxBytes.Load(),
			TxPackets:    f.TxPackets.Load(),
			TxBytes:      f.TxBytes.Load(),
			Encapsulated: f.Encapsulated.Load(),
			Decapsulated: f.Decapsulated.Load(),
			Dropped:      f.Dropped.Load(),
			Errors:       f.Errors.Load(),
		})
	}
	n.ifc.mu.RUnlock()

	var gauges map[string]int64
	n.gauge.mu.RLock()
	if len(n.gauge.g) > 0 {
		gauges = make(map[string]int64, len(n.gauge.g))
		for name, g := range n.gauge.g {
			gauges[name] = g.Load()
		}
	}
	n.gauge.mu.RUnlock()

	return NodeSnap{
		Node:   n.Name,
		Role:   n.Role,
		Ts:     time.Now().UnixMilli(),
		Ifaces: ifaces,
		Gauges: gauges,
	}
}
