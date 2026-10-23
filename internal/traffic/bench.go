// RFC 2544 benchmarking for the traffic generator.
//
// The generator closes the measurement loop by being both ends of the path:
// it injects G-PDUs into one UPF's N3 ingress (locally anchored, so they are
// decapsulated and delivered as plain IP) and listens as the DN sink to which
// the UPF's DN egress is pointed for the duration of the run. A dedicated
// bench session (the reserved top sequence of the node's session space) is
// used, so benchmark volume never mixes with the normal flow accounting.
//
// Each bench frame carries, in its user data, a monotonic index and a
// transmit timestamp (both big-endian, first 16 octets after the UDP
// header). The sink matches frames by count within a drained window and,
// for the latency test, reads the timestamp to compute one-way latency
// (sender and sink share the generator's clock).
//
// Implemented procedures (RFC 2544.1 / 2544.2):
//   - loss:       offered-rate sweep, loss % per rate
//   - throughput: geometric bisection to the maximum lossless rate
//   - latency:    one-way delay at a light fixed rate (min/avg/max/p50/p95/p99)
//   - back2back:  largest lossless burst at minimum inter-frame gap
//
// The bench depends on the UPF exposing a runtime DN-target switch
// (POST /api/dn-target); the host process provides that as BenchDeps.
package traffic

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midu/telco-upf-gen/internal/gtpu"
)

// benchTeidNS is the N3 namespace byte of the F-TEID scheme (defined here
// rather than imported from internal/upf to keep the traffic package free
// of any dependency on the UPF implementation).
const benchTeidNS = 0x10

// Bench frame layout (no GTP-U sequence extension, so size == advertised):
//
//	off 0 : GTP-U base header (8)
//	off 8 : IPv4 datagram (proto UDP), exactly size-8 octets
//	off 28: UDP header (8)
//	off 36: user data: [8B frame index BE][8B tx timestamp ns BE][filler]
const (
	benchDataOff = gtpu.BaseHeaderLen + 20 + 8
	benchMinSize = 64
	benchMaxSize = 65000
)

// BenchTest names one RFC 2544 procedure.
type BenchTest string

const (
	TestLoss       BenchTest = "loss"
	TestLatency    BenchTest = "latency"
	TestThroughput BenchTest = "throughput"
	TestBackToBack BenchTest = "back2back"
)

var allBenchTests = []BenchTest{TestLoss, TestLatency, TestThroughput, TestBackToBack}

// BenchProfile configures one benchmark run. Everything is data: frame
// sizes, rates, durations and which procedures execute. Zero fields fall
// back to the procedure's built-in default.
type BenchProfile struct {
	// FrameSizes are full G-PDU sizes in octets (64..65000).
	FrameSizes []int `yaml:"frame_sizes"`
	// CandidateRates are offered pps for the loss sweep; empty = automatic
	// logarithmic ladder (6 points) beneath MaxRate.
	CandidateRates []float64 `yaml:"candidate_rates"`
	// LatencyRate is the light offered rate (pps) for the latency test.
	LatencyRate float64 `yaml:"latency_rate"`
	// MinRate / MaxRate bound the throughput search (pps).
	MinRate float64 `yaml:"min_rate"`
	MaxRate float64 `yaml:"max_rate"`
	// DurationMs is the per-measurement window (loss/throughput probes use
	// half of it clamped to 250-1500ms; latency uses it clamped to 500-5000ms).
	DurationMs int `yaml:"duration_ms"`
	// B2BSizes are candidate burst lengths (frame counts) for back-to-back,
	// tried ascending; larger sizes are skipped once one bursts.
	B2BSizes []int `yaml:"b2b_sizes"`
	// Tests selects the procedures; empty = all four.
	Tests []BenchTest `yaml:"tests"`
}

// setDefaults fills a usable profile from a minimal (or empty) one.
func (p *BenchProfile) setDefaults() {
	if len(p.FrameSizes) == 0 {
		p.FrameSizes = []int{64, 256, 1518}
	}
	if p.LatencyRate <= 0 {
		p.LatencyRate = 64
	}
	if p.MinRate <= 0 {
		p.MinRate = 64
	}
	if p.MaxRate <= 0 {
		p.MaxRate = 250000
	}
	if p.DurationMs <= 0 {
		p.DurationMs = 2000
	}
	if p.DurationMs > 10000 {
		p.DurationMs = 10000
	}
	if len(p.B2BSizes) == 0 {
		p.B2BSizes = []int{256, 1024, 4096, 16384, 65536}
	}
	if len(p.Tests) == 0 {
		p.Tests = append(p.Tests, allBenchTests...)
	}
}

// validate checks the profile for executable values.
func (p BenchProfile) validate() error {
	if len(p.FrameSizes) == 0 {
		return errors.New("bench: no frame sizes")
	}
	for _, s := range p.FrameSizes {
		if s < benchMinSize || s > benchMaxSize {
			return fmt.Errorf("bench: frame size %d out of range %d..%d", s, benchMinSize, benchMaxSize)
		}
	}
	if p.MinRate < 1 || p.MaxRate < p.MinRate {
		return fmt.Errorf("bench: bad rate window min=%v max=%v", p.MinRate, p.MaxRate)
	}
	if p.LatencyRate < 1 {
		return fmt.Errorf("bench: latency_rate must be >= 1 pps")
	}
	for _, t := range p.Tests {
		switch t {
		case TestLoss, TestLatency, TestThroughput, TestBackToBack:
		default:
			return fmt.Errorf("bench: unknown test %q (want loss|latency|throughput|back2back)", t)
		}
	}
	return nil
}

// BenchResult is one completed measurement point of a run.
type BenchResult struct {
	RunID       string  `json:"runId"`
	Ts          string  `json:"ts"`
	Test        string  `json:"test"`
	FrameSize   int     `json:"frameSize"`
	OfferedPPS  float64 `json:"offeredPps"`
	OfferedBPS  float64 `json:"offeredBps"` // Mbps
	Sent        uint64  `json:"sent"`
	Received    uint64  `json:"received"`
	LossPct     float64 `json:"lossPct"`
	LatMinUS    float64 `json:"latMinUs"`
	LatAvgUS    float64 `json:"latAvgUs"`
	LatMaxUS    float64 `json:"latMaxUs"`
	LatP50US    float64 `json:"latP50Us"`
	LatP95US    float64 `json:"latP95Us"`
	LatP99US    float64 `json:"latP99Us"`
	B2BLossless int     `json:"b2bLossless"` // back2back: largest lossless burst
}

// BenchRun is one complete benchmark run (all sizes x all tests). The
// targeting fields are unexported on purpose: they are operational state,
// not part of the published view.
type BenchRun struct {
	RunID    string        `json:"runId"`
	Started  string        `json:"started"`
	Finished string        `json:"finished,omitempty"`
	Target   string        `json:"target"` // display, e.g. "upf-0 127.0.0.1:2152"
	Profile  BenchProfile  `json:"profile"`
	Results  []BenchResult `json:"results"`
	Error    string        `json:"error,omitempty"`

	n3   string
	node int
	seq  int
}

// BenchStatus is the live view for dashboards.
type BenchStatus struct {
	Active    bool      `json:"active"`
	Phase     string    `json:"phase,omitempty"`
	Progress  float64   `json:"progress"` // 0..1
	Current   *BenchRun `json:"current,omitempty"`
	LastError string    `json:"lastError,omitempty"`
}

// BenchDeps is the environment the runner needs from its host process:
// switching the anchor UPF's DN egress target. SetDNTarget returns the
// previously configured target (possibly empty) so the runner can restore.
type BenchDeps struct {
	SetDNTarget func(target string) (previous string, err error)
}

// Bench runs RFC 2544 procedures on demand. It is safe for concurrent use;
// only one run executes at a time. The default profile comes from YAML;
// callers may override fields per start (see Start).
type Bench struct {
	gen *Generator
	def BenchProfile

	mu     sync.Mutex
	deps   BenchDeps
	status BenchStatus
	runs   []*BenchRun // history, bounded, oldest dropped first
	cancel context.CancelFunc
}

const maxRunHistory = 10

// NewBench binds a bench to a generator (the bench pauses the normal flows
// while measuring) and a default profile.
func NewBench(gen *Generator, profile BenchProfile) *Bench {
	profile.setDefaults()
	return &Bench{gen: gen, def: profile}
}

// SetDeps sets the environment closure used by the next run. Must be called
// right before Start with the anchor's control endpoint binding.
func (b *Bench) SetDeps(d BenchDeps) {
	b.mu.Lock()
	b.deps = d
	b.mu.Unlock()
}

// Profile returns the configured default profile.
func (b *Bench) Profile() BenchProfile {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.def
}

// Status is the live view for dashboards.
func (b *Bench) Status() BenchStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.status
}

// History returns past runs, newest first.
func (b *Bench) History() []BenchRun {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]BenchRun, 0, len(b.runs))
	for i := len(b.runs) - 1; i >= 0; i-- {
		out = append(out, *b.runs[i])
	}
	return out
}

// Start launches a run in the background. n3Addr is the anchor UPF's GTP-U
// N3 "host:port" (FQDN allowed), node its fabric identity and sessions the
// node's session space (the bench reserves the top sequence for itself).
// The profile must have FrameSizes set; tests/sizes may be overridden here
// (empty tests = all four). The runner pauses the normal output level for
// the duration and restores it on exit.
func (b *Bench) Start(profile BenchProfile, n3Addr string, node, sessions int) error {
	if node < 0 || node > 255 {
		return fmt.Errorf("bench: node %d out of range 0..255", node)
	}
	if n3Addr == "" {
		return errors.New("bench: empty N3 address")
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.status.Active {
		return errors.New("bench: a run is already active")
	}
	if len(profile.Tests) == 0 && len(profile.FrameSizes) == 0 {
		profile = b.def
	} else {
		profile.setDefaults()
	}
	if err := profile.validate(); err != nil {
		return err
	}
	seq := 0
	if sessions > 1 {
		seq = sessions - 1
	}
	if seq >= 1<<16 {
		seq = 1<<16 - 1
	}
	run := &BenchRun{
		RunID:   fmt.Sprintf("r%d", time.Now().UnixNano()),
		Started: time.Now().UTC().Format(time.RFC3339Nano),
		Target:  fmt.Sprintf("upf-%d %s", node, n3Addr),
		Profile: profile,
		n3:      n3Addr,
		node:    node,
		seq:     seq,
	}
	ctx, cancel := context.WithCancel(context.Background())
	b.status = BenchStatus{Active: true, Phase: "starting", Progress: 0, Current: run}
	b.cancel = cancel
	go b.execute(ctx, run)
	return nil
}

// Cancel requests the active run to stop (cooperative). Returns whether a
// run was active.
func (b *Bench) Cancel() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.status.Active || b.cancel == nil {
		return false
	}
	b.cancel()
	return true
}

func (b *Bench) setPhase(run *BenchRun, phase string, progress float64) {
	if progress < 0 {
		progress = 0
	}
	if progress > 1 {
		progress = 1
	}
	b.mu.Lock()
	b.status.Phase = phase
	b.status.Progress = progress
	b.mu.Unlock()
}

func (b *Bench) finish(run *BenchRun, err error) {
	if err != nil {
		run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		run.Error = err.Error()
	}
	b.mu.Lock()
	b.status.Active = false
	b.status.Phase = ""
	b.status.Progress = 0
	b.status.Current = nil
	b.status.LastError = run.Error
	b.runs = append(b.runs, run)
	for len(b.runs) > maxRunHistory {
		b.runs = b.runs[len(b.runs)-maxRunHistory:]
	}
	b.cancel = nil
	b.mu.Unlock()
}

// execute is the driver for one run: build the measurement circuit, then
// iterate sizes x tests under a zeroed throttle knob.
func (b *Bench) execute(ctx context.Context, run *BenchRun) {
	b.mu.Lock()
	deps := b.deps
	b.mu.Unlock()
	if deps.SetDNTarget == nil {
		b.finish(run, errors.New("bench: DN-target dependency not configured"))
		return
	}

	e, cleanup, err := newRunEngine(deps, run)
	if err != nil {
		b.finish(run, err)
		return
	}
	defer cleanup()

	// Silence the normal flows while measuring, restore afterwards.
	saved := b.gen.Throttle()
	b.gen.SetThrottle(0)
	defer b.gen.SetThrottle(saved)

	p := run.Profile
	sizes := append([]int(nil), p.FrameSizes...)
	sort.Ints(sizes)
	total := len(sizes) * len(p.Tests)
	done := 0
	for _, size := range sizes {
		fb, err := newFrameBuilder(e.teid, size)
		if err != nil {
			b.finish(run, err)
			return
		}
		for _, t := range p.Tests {
			if ctx.Err() != nil {
				b.finish(run, context.Canceled)
				return
			}
			b.setPhase(run, fmt.Sprintf("%s @ %d octets", string(t), size), float64(done)/float64(total))
			res, err := e.runTest(ctx, run, t, size, fb)
			if err != nil {
				if ctx.Err() != nil {
					b.finish(run, context.Canceled)
					return
				}
				b.finish(run, err)
				return
			}
			run.Results = append(run.Results, res...)
			done++
		}
	}
	run.Finished = time.Now().UTC().Format(time.RFC3339Nano)
	b.finish(run, nil)
}

// runEngine holds the live measurement circuit for one run: sockets, DN
// re-pointing state, the receive window and the latency samples.
type runEngine struct {
	deps BenchDeps

	conn *net.UDPConn // bench egress: G-PDUs to the UPF N3 port
	sink *net.UDPConn // receives the plain-IP frames the UPF egresses
	teid uint32

	rx         atomic.Uint64
	collectLat atomic.Bool
	latMu      sync.Mutex
	lats       []float64 // one-way delay in microseconds since last reset
}

// newRunEngine wires the measurement circuit: resolve the anchor N3
// endpoint, discover the source IP toward it, bind the DN sink there,
// re-point the UPF's DN egress at the sink, open the bench egress socket.
// The returned cleanup undoes everything exactly once.
func newRunEngine(deps BenchDeps, run *BenchRun) (*runEngine, func(), error) {
	dst, err := benchUDPAddr(run.n3)
	if err != nil {
		return nil, nil, err
	}

	e := &runEngine{
		teid: benchTeidNS<<24 | uint32(run.node)<<16 | uint32(run.seq),
	}

	// Source IP used toward the fabric: the sink must be reachable there,
	// because that is exactly the address the UPF's DN egress writes to.
	probe, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		return nil, nil, fmt.Errorf("bench: probe dial %s: %w", run.n3, err)
	}
	srcIP := probe.LocalAddr().(*net.UDPAddr).IP
	_ = probe.Close()

	var bind *net.UDPAddr
	if srcIP != nil {
		bind = &net.UDPAddr{IP: srcIP}
	}
	sink, err := net.ListenUDP("udp", bind)
	if err != nil {
		return nil, nil, fmt.Errorf("bench: sink bind: %w", err)
	}
	e.sink = sink

	prev, err := deps.SetDNTarget(sink.LocalAddr().String())
	if err != nil {
		_ = sink.Close()
		return nil, nil, fmt.Errorf("bench: set DN target: %w", err)
	}

	conn, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		_, _ = deps.SetDNTarget(prev)
		_ = sink.Close()
		return nil, nil, fmt.Errorf("bench: dial N3 %s: %w", run.n3, err)
	}
	e.conn = conn

	go e.readLoop()

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			_ = conn.Close()
			_ = sink.Close()
			if _, rerr := deps.SetDNTarget(prev); rerr != nil {
				fmt.Printf("bench: restore DN target %q: %v\n", prev, rerr)
			}
		})
	}
	return e, cleanup, nil
}

// benchUDPAddr resolves "host:port" to a concrete UDP address (FQDN or IP).
func benchUDPAddr(addr string) (*net.UDPAddr, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("bench: bad N3 address %q: %w", addr, err)
	}
	pn, err := strconv.Atoi(port)
	if err != nil || pn < 1 || pn > 65535 {
		return nil, fmt.Errorf("bench: bad N3 port in %q", addr)
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("bench: resolve %s: %w", host, err)
	}
	return &net.UDPAddr{IP: pickIP(ips), Port: pn}, nil
}

// pickIP prefers an IPv4 address (the fabric is IPv4-only by definition).
func pickIP(ips []net.IP) net.IP {
	for _, ip := range ips {
		if v4 := ip.To4(); v4 != nil {
			return v4
		}
	}
	return ips[0]
}

// frameBuilder keeps one reusable wire template for a frame size and stamps
// per-frame fields in place (index + transmit timestamp).
type frameBuilder struct {
	tmpl []byte
	idx  uint64
}

// newFrameBuilder encodes the template G-PDU for one frame size: base
// header + IPv4/UDP inner datagram, checksums computed, zero user fields.
func newFrameBuilder(teid uint32, size int) (*frameBuilder, error) {
	if size < benchMinSize || size > benchMaxSize {
		return nil, fmt.Errorf("bench: frame size %d out of range %d..%d", size, benchMinSize, benchMaxSize)
	}
	inner := size - gtpu.BaseHeaderLen
	if inner < 28+16 { // IP hdr + UDP hdr + the idx/tx fields
		return nil, fmt.Errorf("bench: frame size %d too small for measurement fields", size)
	}

	dg := make([]byte, inner)
	dg[0] = 0x45                             // IPv4, IHL 5
	dg[6] = 0x40                             // DF
	dg[8] = 64                               // TTL
	dg[9] = 17                               // UDP
	copy(dg[12:16], []byte{10, 254, 254, 1}) // bench UE address
	copy(dg[16:20], []byte{10, 254, 254, 2}) // bench DN address
	binary.BigEndian.PutUint16(dg[2:4], uint16(inner))
	binary.BigEndian.PutUint16(dg[10:12], checksum(dg[:20]))

	// UDP header (checksum 0 placeholder, filled with the pseudo-header sum).
	binary.BigEndian.PutUint16(dg[20:22], 32768)
	binary.BigEndian.PutUint16(dg[22:24], 32769)
	binary.BigEndian.PutUint16(dg[24:26], uint16(inner-20))

	// Filler: deterministic non-zero pattern after the measurement fields,
	// so the payload is realistic and never all-zero.
	for i := benchDataOff - gtpu.BaseHeaderLen + 16; i < inner; i++ {
		dg[i] = byte((i * 7) % 251)
	}

	return &frameBuilder{tmpl: gtpu.NewPacket(teid, dg).Marshal()}, nil
}

// stamp patches the template in place with the next index and the current
// transmit timestamp (nanoseconds).
func (f *frameBuilder) stamp() {
	f.idx++
	binary.BigEndian.PutUint64(f.tmpl[benchDataOff:], f.idx)
	binary.BigEndian.PutUint64(f.tmpl[benchDataOff+8:], uint64(time.Now().UnixNano()))
}

// checksum returns the ones-complement 16-bit fold of b (b length even).
func checksum(b []byte) uint16 {
	return uint16(^ipSum(b) & 0xffff)
}

// readLoop consumes the sink: every delivered frame bumps the receive
// window; when latency collection is on, the tx timestamp is read back for
// a one-way delay sample. Runs until the socket closes.
func (e *runEngine) readLoop() {
	buf := make([]byte, benchMaxSize+16)
	for {
		n, _, err := e.sink.ReadFromUDP(buf)
		if err != nil {
			return // socket closed by cleanup
		}
		e.rx.Add(1)
		if e.collectLat.Load() && n >= benchDataOff-gtpu.BaseHeaderLen+16 && buf[0]>>4 == 4 {
			ihl := int(buf[0]&0x0f) * 4
			data := buf[ihl+8:] // user data: [8B idx][8B tx ns][filler]
			if len(data) >= 16 {
				tx := int64(binary.BigEndian.Uint64(data[8:16]))
				d := time.Now().UnixNano() - tx
				if d >= 0 && d < int64(time.Hour) {
					e.latMu.Lock()
					e.lats = append(e.lats, float64(d)/1000)
					e.latMu.Unlock()
				}
			}
		}
	}
}

// resetLats clears the latency sample window.
func (e *runEngine) resetLats() {
	e.latMu.Lock()
	e.lats = e.lats[:0]
	e.latMu.Unlock()
}

// latsCopy returns the collected latency samples (microseconds).
func (e *runEngine) latsCopy() []float64 {
	e.latMu.Lock()
	defer e.latMu.Unlock()
	out := make([]float64, len(e.lats))
	copy(out, e.lats)
	return out
}

// drain waits until the sink has been quiet for a while, so leftovers from
// the previous measurement cannot leak into the next window.
func (e *runEngine) drain() {
	deadline := time.Now().Add(1500 * time.Millisecond)
	last := e.rx.Load()
	quiet := time.Now()
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		cur := e.rx.Load()
		if cur != last {
			last = cur
			quiet = time.Now()
			continue
		}
		if time.Since(quiet) > 200*time.Millisecond {
			return
		}
	}
}

// pacedSend offers `rate` frames per second for `dur` using a token-bucket
// tick (100us). Returns the number of frames actually written; a write
// error aborts the measurement.
func (e *runEngine) pacedSend(ctx context.Context, fb *frameBuilder, rate float64, dur time.Duration) (uint64, error) {
	end := time.Now().Add(dur)
	var sent uint64
	carry := 0.0
	last := time.Now()
	for {
		now := time.Now()
		if now.After(end) {
			return sent, nil
		}
		select {
		case <-ctx.Done():
			return sent, ctx.Err()
		default:
		}
		carry += rate * now.Sub(last).Seconds()
		last = now
		n := int(carry)
		carry -= float64(n)
		for i := 0; i < n; i++ {
			fb.stamp()
			if _, err := e.conn.Write(fb.tmpl); err != nil {
				return sent, err
			}
			sent++
		}
		next := last.Add(100 * time.Microsecond)
		if time.Until(next) > 0 && next.Before(end) {
			time.Sleep(time.Until(next))
		}
	}
}

// settle waits until the sink has received `expect` frames since base (full
// delivery) or the receive window stalls; returns the final count.
func (e *runEngine) settle(ctx context.Context, base, expect uint64) uint64 {
	start := time.Now()
	deadline := start.Add(5 * time.Second)
	var last uint64
	quiet := start
	for {
		got := e.rx.Load() - base
		if expect > 0 && got >= expect {
			return got
		}
		if e.rx.Load() != last {
			last = e.rx.Load()
			quiet = time.Now()
		}
		if time.Since(start) > 100*time.Millisecond && time.Since(quiet) > 250*time.Millisecond {
			return got
		}
		if time.Now().After(deadline) {
			return got
		}
		select {
		case <-ctx.Done():
			return got
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// result builds one measurement point.
func (e *runEngine) result(run *BenchRun, t BenchTest, size int, rate float64, sent, got uint64) BenchResult {
	r := BenchResult{
		RunID:      run.RunID,
		Ts:         time.Now().UTC().Format(time.RFC3339Nano),
		Test:       string(t),
		FrameSize:  size,
		OfferedPPS: rate,
		OfferedBPS: rate * float64(size) * 8 / 1e6,
		Sent:       sent,
		Received:   got,
	}
	if sent > 0 {
		r.LossPct = 100 * float64(sent-got) / float64(sent)
		if r.LossPct < 0 {
			r.LossPct = 0
		}
	}
	return r
}

// runTest dispatches one procedure. fb is reused across tests of the same
// size; each test owns its own drained receive window.
func (e *runEngine) runTest(ctx context.Context, run *BenchRun, t BenchTest, size int, fb *frameBuilder) ([]BenchResult, error) {
	switch t {
	case TestLoss:
		return e.testLoss(ctx, run, size, fb)
	case TestThroughput:
		return e.testThroughput(ctx, run, size, fb)
	case TestLatency:
		return e.testLatency(ctx, run, size, fb)
	case TestBackToBack:
		return e.testBackToBack(ctx, run, size, fb)
	default:
		return nil, fmt.Errorf("bench: unknown test %q", t)
	}
}

// lossRates returns the offered-rate ladder for the loss sweep: the
// configured candidates, or a 6-point logarithmic ladder up to MaxRate.
func lossRates(p BenchProfile) []float64 {
	if len(p.CandidateRates) > 0 {
		out := append([]float64(nil), p.CandidateRates...)
		sort.Float64s(out)
		return out
	}
	lo := p.MaxRate / 256
	if lo > p.MaxRate {
		lo = p.MaxRate
	}
	const n = 6
	out := make([]float64, n)
	for i := range out {
		out[i] = lo * math.Pow(p.MaxRate/lo, float64(i)/float64(n-1))
	}
	return out
}

// probeWindow is the per-probe duration: half of DurationMs, clamped.
func probeWindow(p BenchProfile) time.Duration {
	return clampDur(int64(p.DurationMs)/2, 250, 1500)
}

func clampDur(ms, lo, hi int64) time.Duration {
	if ms < lo {
		ms = lo
	}
	if ms > hi {
		ms = hi
	}
	return time.Duration(ms) * time.Millisecond
}

// testLoss sweeps the offered rates and reports loss% per rate.
func (e *runEngine) testLoss(ctx context.Context, run *BenchRun, size int, fb *frameBuilder) ([]BenchResult, error) {
	probe := probeWindow(run.Profile)
	out := make([]BenchResult, 0, len(run.Profile.CandidateRates)+6)
	for _, rate := range lossRates(run.Profile) {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		e.drain()
		base := e.rx.Load()
		sent, err := e.pacedSend(ctx, fb, rate, probe)
		if err != nil {
			return out, err
		}
		got := e.settle(ctx, base, sent)
		out = append(out, e.result(run, TestLoss, size, rate, sent, got))
	}
	return out, nil
}

// testThroughput bisects geometrically between MinRate and MaxRate for the
// maximum offered rate with zero loss.
func (e *runEngine) testThroughput(ctx context.Context, run *BenchRun, size int, fb *frameBuilder) ([]BenchResult, error) {
	p := run.Profile
	probe := probeWindow(p)
	measure := func(rate float64) (sent, got uint64, err error) {
		e.drain()
		base := e.rx.Load()
		sent, err = e.pacedSend(ctx, fb, rate, probe)
		if err != nil {
			return sent, 0, err
		}
		return sent, e.settle(ctx, base, sent), nil
	}

	sentHi, gotHi, err := measure(p.MaxRate)
	if err != nil {
		return nil, err
	}
	if gotHi == sentHi {
		// The ceiling itself is lossless: take it and stop.
		return []BenchResult{e.result(run, TestThroughput, size, p.MaxRate, sentHi, gotHi)}, nil
	}
	lo, hi := p.MinRate, p.MaxRate
	for i := 0; i < 10; i++ {
		if (hi-lo)/lo < 0.05 {
			break
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		mid := math.Sqrt(lo * hi)
		sentMid, gotMid, err := measure(mid)
		if err != nil {
			return nil, err
		}
		if gotMid == sentMid {
			lo = mid
		} else {
			hi = mid
		}
	}
	// Final confirmation measurement at the lossless rate.
	sentF, gotF, err := measure(lo)
	if err != nil {
		return nil, err
	}
	return []BenchResult{e.result(run, TestThroughput, size, lo, sentF, gotF)}, nil
}

// testLatency measures one-way delay at a light fixed rate.
func (e *runEngine) testLatency(ctx context.Context, run *BenchRun, size int, fb *frameBuilder) ([]BenchResult, error) {
	dur := clampDur(int64(run.Profile.DurationMs), 500, 5000)
	e.drain()
	e.resetLats()
	e.collectLat.Store(true)
	defer e.collectLat.Store(false)

	base := e.rx.Load()
	sent, err := e.pacedSend(ctx, fb, run.Profile.LatencyRate, dur)
	if err != nil {
		return nil, err
	}
	got := e.settle(ctx, base, sent)
	lats := e.latsCopy()

	r := e.result(run, TestLatency, size, run.Profile.LatencyRate, sent, got)
	if len(lats) > 0 {
		sort.Float64s(lats)
		var sum float64
		for _, v := range lats {
			sum += v
		}
		r.LatMinUS = lats[0]
		r.LatAvgUS = sum / float64(len(lats))
		r.LatMaxUS = lats[len(lats)-1]
		r.LatP50US = percentile(lats, 0.50)
		r.LatP95US = percentile(lats, 0.95)
		r.LatP99US = percentile(lats, 0.99)
	}
	return []BenchResult{r}, nil
}

// testBackToBack sends unpaced bursts of ascending candidate sizes and
// reports the largest burst delivered without loss.
func (e *runEngine) testBackToBack(ctx context.Context, run *BenchRun, size int, fb *frameBuilder) ([]BenchResult, error) {
	sizes := append([]int(nil), run.Profile.B2BSizes...)
	sort.Ints(sizes)

	lossless := 0
	var sent, got uint64
	for _, b := range sizes {
		if ctx.Err() != nil {
			break
		}
		e.drain()
		base := e.rx.Load()
		for i := 0; i < b; i++ {
			if _, err := e.conn.Write(fb.tmpl); err != nil {
				return nil, err
			}
		}
		sent = uint64(b)
		got = e.settle(ctx, base, sent)
		if got >= sent {
			lossless = b
		} else {
			break // first lossy burst: monotonic assumption, stop here
		}
	}
	r := e.result(run, TestBackToBack, size, 0, sent, got)
	r.B2BLossless = lossless
	return []BenchResult{r}, nil
}

// percentile returns the q-quantile (0..1] of a sorted slice.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
