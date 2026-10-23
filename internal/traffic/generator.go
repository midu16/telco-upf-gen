// Package traffic implements the GTP-U traffic generator: a set of
// token-bucket flows of synthetic user datagrams, wrapped in GTP-U, toward
// the UPF fabric (N3, N5 and N9 ingress paths; east-west flow exercised
// through N9 retagging by the UPFs).
package traffic

import (
	"context"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midu/telco-upf-gen/internal/gtpu"
	"github.com/midu/telco-upf-gen/internal/telemetry"
)

// MaxTickBurst caps packets emitted in a single tick (protects the socket
// buffer if the process stalls and the bucket overflows).
const MaxTickBurst = 50000

// Flow is one token-bucket GTP-U stream toward a single UPF endpoint.
type Flow struct {
	Name  string
	Iface string // "N3" | "N5" (ingress path targeted on the UPF)
	Dest  string // "host:port"
	Teid  uint32
	PPS   float64
	Size  int // user payload size in octets (the synthetic IP datagram)

	conn  *net.UDPConn
	carry float64

	sentPkts  atomic.Uint64
	sentBytes atomic.Uint64
	dropped   atomic.Uint64
	nextSeq   atomic.Uint32
}

// FlowSnap is the JSON view of a flow.
type FlowSnap struct {
	Name       string  `json:"name"`
	Iface      string  `json:"iface"`
	Dest       string  `json:"dest"`
	Teid       string  `json:"teid"`
	AnchorNode uint32  `json:"anchorNode"`
	PPS        float64 `json:"pps"`
	Size       int     `json:"size"`
	Pkts       uint64  `json:"pkts"`
	Bytes      uint64  `json:"bytes"`
}

func (f *Flow) capSize() int {
	return gtpu.BaseHeaderLen + gtpu.SeqExtLen + f.Size
}

// Tick injects up to the accumulated token bucket and returns emitted
// packets/bytes. throttlePct scales the flow's target PPS (0-100). The
// flow must be dialed first.
func (f *Flow) Tick(dtSec, throttlePct float64) (uint64, uint64) {
	if f.conn == nil {
		return 0, 0
	}
	f.carry += f.PPS * (throttlePct / 100.0) * dtSec
	if f.carry < 1 {
		return 0, 0
	}
	n := int(f.carry)
	f.carry -= float64(n)
	if n > MaxTickBurst {
		n = MaxTickBurst
	}

	var pkts, bytes uint64
	buf := make([]byte, 0, f.capSize())
	for i := 0; i < n; i++ {
		buf = f.buildPacket(buf[:0])
		w, err := f.conn.Write(buf)
		if err != nil {
			f.dropped.Add(1)
			continue
		}
		pkts++
		bytes += uint64(w)
	}
	f.sentPkts.Add(pkts)
	f.sentBytes.Add(bytes)
	return pkts, bytes
}

// buildPacket encodes one GTP-U G-PDU into dst (reused capacity) and
// returns the packet slice. Half the packets carry a sequence-number
// extension header to exercise the full header decoder; the inner user
// datagram carries a realistic per-flow application stack for DPI.
func (f *Flow) buildPacket(dst []byte) []byte {
	seq := f.nextSeq.Add(1)
	payload := fakeIP(f.conn.LocalAddr(), f.Teid, seq)
	hdr := gtpu.Header{Teid: f.Teid}
	if seq&1 == 1 {
		s16 := uint16(seq)
		hdr.Seq = &s16
	}
	p := gtpu.Packet{Hdr: hdr, Payload: payload}
	return p.MarshalTo(dst)
}

// Dial binds the flow's egress socket.
func (f *Flow) Dial() error {
	host, port, err := net.SplitHostPort(f.Dest)
	if err != nil {
		return fmt.Errorf("traffic: bad dest %q: %w", f.Dest, err)
	}
	ip, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("traffic: resolve %s: %w", host, err)
	}
	pn, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("traffic: bad port in %q: %w", f.Dest, err)
	}
	c, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: ip[0], Port: pn})
	if err != nil {
		return fmt.Errorf("traffic: dial %s: %w", f.Dest, err)
	}
	f.conn = c
	return nil
}

func (f *Flow) Close() {
	if f.conn != nil {
		_ = f.conn.Close()
		f.conn = nil
	}
}

// Generator drives all flows of one traffic-generator pod.
type Generator struct {
	name    string
	period  time.Duration
	stats   *telemetry.Node
	mu      sync.Mutex
	flows   []*Flow
	flowsBy map[string]*net.UDPConn // dest -> shared egress socket

	// throttle is the output level in percent (0-100), stored as
	// math.Float64bits so it can be changed without the flow lock.
	throttle atomic.Uint64
}

// NewGenerator creates a generator pod. period is the token-bucket tick.
func NewGenerator(name string, period time.Duration) *Generator {
	if period < time.Millisecond {
		period = 10 * time.Millisecond
	}
	g := &Generator{
		name:    name,
		period:  period,
		stats:   telemetry.NewNode(name, "traffic", "TX"),
		flowsBy: map[string]*net.UDPConn{},
	}
	g.throttle.Store(math.Float64bits(100))
	return g
}

func (g *Generator) Stats() *telemetry.Node { return g.stats }

// AddFlow registers a flow. Flow sockets are shared per destination, so any
// number of flows can target the same UPF endpoint.
func (g *Generator) AddFlow(f *Flow) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.flows = append(g.flows, f)
}

// Dial connects every flow, reusing one socket per destination.
func (g *Generator) Dial() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	var errs []string
	for _, f := range g.flows {
		if c, ok := g.flowsBy[f.Dest]; ok {
			f.conn = c
			continue
		}
		probe := &Flow{Dest: f.Dest}
		if err := probe.Dial(); err != nil {
			errs = append(errs, f.Name)
			continue
		}
		g.flowsBy[f.Dest] = probe.conn
		f.conn = probe.conn
	}
	if len(errs) > 0 {
		return fmt.Errorf("traffic: flows not dialed: %s", strings.Join(errs, ", "))
	}
	return nil
}

// SetThrottle sets the output level (0-100) clamped to range, and returns
// the effective value. Takes effect on the next bucket tick.
func (g *Generator) SetThrottle(pct float64) float64 {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	g.throttle.Store(math.Float64bits(pct))
	g.stats.Gauge("throttle").Set(int64(pct))
	return pct
}

// Throttle returns the current output level.
func (g *Generator) Throttle() float64 {
	return math.Float64frombits(g.throttle.Load())
}

// Start runs the token-bucket loop until ctx is cancelled.
func (g *Generator) Start(ctx context.Context) {
	go func() {
		t := time.NewTicker(g.period)
		defer t.Stop()
		g.stats.Gauge("flows").Set(int64(len(g.flows)))
		g.stats.Gauge("throttle").Set(int64(g.Throttle()))

		dt := g.period.Seconds()
		for {
			select {
			case <-ctx.Done():
				g.Close()
				return
			case <-t.C:
				g.tick(dt)
			}
		}
	}()
}

func (g *Generator) tick(dtSec float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	tx := g.stats.Iface("TX")
	throttle := g.Throttle()
	var pkts, bytes uint64
	for _, f := range g.flows {
		p, b := f.Tick(dtSec, throttle)
		pkts += p
		bytes += b
	}
	tx.TxPackets.Add(pkts)
	tx.TxBytes.Add(bytes)
	tx.Encapsulated.Add(pkts)
}

// Close tears down the shared sockets (idempotent).
func (g *Generator) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, c := range g.flowsBy {
		_ = c.Close()
	}
	g.flowsBy = map[string]*net.UDPConn{}
	for _, f := range g.flows {
		f.conn = nil
	}
}

// FlowsSnapshot returns a point-in-time view of every flow.
func (g *Generator) FlowsSnapshot() []FlowSnap {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]FlowSnap, 0, len(g.flows))
	for _, f := range g.flows {
		out = append(out, FlowSnap{
			Name:       f.Name,
			Iface:      f.Iface,
			Dest:       f.Dest,
			Teid:       fmt.Sprintf("0x%08x", f.Teid),
			AnchorNode: (f.Teid >> 16) & 0xff,
			PPS:        f.PPS,
			Size:       f.Size,
			Pkts:       f.sentPkts.Load(),
			Bytes:      f.sentBytes.Load(),
		})
	}
	return out
}

func teidSeq16(t uint32) uint32 { return t & 0xffff }

// ipSum returns the ones-complement 16-bit sum of b (b length must be even).
func ipSum(b []byte) uint32 {
	var s uint32
	for i := 0; i+1 < len(b); i += 2 {
		s += uint32(b[i])<<8 | uint32(b[i+1])
	}
	for s>>16 != 0 {
		s = (s & 0xffff) + (s >> 16)
	}
	return s
}
