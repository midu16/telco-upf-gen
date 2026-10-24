package traffic

import (
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/midu/telco-upf-gen/internal/gtpu"
)

// fakeUPF is an in-process stand-in for one UPF N3 port: it decodes G-PDUs
// and, whenever a DN target is configured, egresses the inner datagram
// there — exactly the observable behavior the real data plane exposes
// (ingress -> decap -> DN delivery). DN-target switching mirrors the
// runtime switch API (SetDNTarget returns the previous target).
type fakeUPF struct {
	n3     *net.UDPConn
	mu     sync.Mutex
	dn     *net.UDPConn
	target string
}

func newFakeUPF(t *testing.T) *fakeUPF {
	f := &fakeUPF{}
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("fake upf listen: %v", err)
	}
	f.n3 = c
	go f.readLoop()
	t.Cleanup(f.close)
	return f
}

func (f *fakeUPF) port() int { return f.n3.LocalAddr().(*net.UDPAddr).Port }

func (f *fakeUPF) readLoop() {
	buf := make([]byte, 65536)
	for {
		n, _, err := f.n3.ReadFromUDP(buf)
		if err != nil {
			return
		}
		p, err := gtpu.Parse(buf[:n])
		if err != nil {
			continue
		}
		f.mu.Lock()
		dn := f.dn
		f.mu.Unlock()
		if dn != nil {
			_, _ = dn.Write(p.Payload)
		}
	}
}

// setDN mirrors the UPF's runtime DN-target semantics: previous value
// returned, cached egress socket dropped on change.
func (f *fakeUPF) setDN(target string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if target == f.target {
		return f.target, nil
	}
	prev := f.target
	if f.dn != nil {
		_ = f.dn.Close()
		f.dn = nil
		f.target = ""
	}
	if target != "" {
		la, err := net.ResolveUDPAddr("udp", target)
		if err != nil {
			return "", err
		}
		dn, err := net.DialUDP("udp", nil, la)
		if err != nil {
			return "", err
		}
		f.dn = dn
		f.target = target
	}
	return prev, nil
}

func (f *fakeUPF) close() {
	_ = f.n3.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dn != nil {
		_ = f.dn.Close()
		f.dn = nil
	}
}

// fastProfile is a small-but-complete profile for the in-process run
// (sizes x tests total must stay within a couple of dozen probes).
func fastProfile() BenchProfile {
	return BenchProfile{
		FrameSizes:  []int{64, 256},
		DurationMs:  400,
		MinRate:     200,
		MaxRate:     8000,
		LatencyRate: 200,
		B2BSizes:    []int{64, 512, 4096},
		SendWorkers: 2, // exercise the concurrent send/receive paths
		RxWorkers:   2,
		Tests:       []BenchTest{TestLoss, TestThroughput, TestLatency, TestBackToBack},
	}
}

// TestBenchRunProcedures exercises one full run against the fake UPF and
// asserts procedure outputs plus the pause/restore side effects.
func TestBenchRunProcedures(t *testing.T) {
	upf := newFakeUPF(t)
	gen := NewGenerator("bench-test", 10*time.Millisecond)
	if err := gen.Dial(); err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer gen.Close()

	b := NewBench(gen, fastProfile())
	b.SetDeps(BenchDeps{SetDNTarget: upf.setDN})
	n3 := net.JoinHostPort("127.0.0.1", strconv.Itoa(upf.port()))
	if err := b.Start(BenchProfile{}, n3, 0, 16); err != nil {
		t.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	for b.Status().Active {
		if time.Now().After(deadline) {
			t.Fatal("bench run timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}

	runs := b.History()
	if len(runs) == 0 {
		t.Fatal("no runs recorded")
	}
	run := runs[0]
	if run.Error != "" {
		t.Fatalf("run failed: %s", run.Error)
	}
	if run.RunID == "" || run.Started == "" || run.Finished == "" {
		t.Fatalf("run metadata incomplete: %+v", run)
	}
	if run.Profile.MinRate != 200 || run.Profile.MaxRate != 8000 {
		t.Fatalf("profile overrides not applied: %+v", run.Profile)
	}

	byTest := map[BenchTest][]BenchResult{}
	for _, r := range run.Results {
		byTest[BenchTest(r.Test)] = append(byTest[BenchTest(r.Test)], r)
	}
	// loss: automatic 6-point ladder per size
	if got := len(byTest[TestLoss]); got != 2*6 {
		t.Fatalf("loss results = %d, want 12", got)
	}
	for _, r := range byTest[TestLoss] {
		if r.Sent == 0 {
			t.Fatalf("loss probe sent nothing: %+v", r)
		}
	}
	// throughput: one row per size, lossless and above MinRate
	for _, r := range byTest[TestThroughput] {
		if r.Received != r.Sent || r.Sent == 0 {
			t.Fatalf("throughput not lossless: %+v", r)
		}
		if r.OfferedPPS < 200 {
			t.Fatalf("throughput below MinRate: %+v", r)
		}
	}
	// latency: one row per size with positive samples
	for _, r := range byTest[TestLatency] {
		if len(byTest[TestLatency]) != 2 || r.LatAvgUS <= 0 || r.LatP99US < r.LatP50US {
			t.Fatalf("latency row bad: %+v", r)
		}
	}
	// back2back: at least the smallest candidate burst is lossless on
	// loopback; larger ones may legitimately burst-drop.
	for _, r := range byTest[TestBackToBack] {
		if r.B2BLossless < 64 || r.Received > r.Sent {
			t.Fatalf("b2b row bad: %+v", r)
		}
	}

	// Side effects: throttle restored to its pre-run level.
	if got := gen.Throttle(); got != 100 {
		t.Fatalf("throttle not restored: %v", got)
	}
	// DN target restored (empty original).
	upf.mu.Lock()
	target := upf.target
	upf.mu.Unlock()
	if target != "" {
		t.Fatalf("DN target not restored: %q", target)
	}
}

// TestBenchStartValidation covers profile/target validation and the
// single-run invariant.
func TestBenchStartValidation(t *testing.T) {
	upf := newFakeUPF(t)
	gen := NewGenerator("bench-v", 10*time.Millisecond)
	b := NewBench(gen, BenchProfile{})

	n3 := net.JoinHostPort("127.0.0.1", strconv.Itoa(upf.port()))

	// bad frame size
	p := fastProfile()
	p.FrameSizes = []int{32}
	if err := b.Start(p, n3, 0, 16); err == nil {
		t.Fatal("expected error for frame size 32")
	}
	// unknown test name
	p = fastProfile()
	p.Tests = []BenchTest{"flood"}
	if err := b.Start(p, n3, 0, 16); err == nil {
		t.Fatal("expected error for unknown test")
	}
	// bad node
	if err := b.Start(fastProfile(), n3, 300, 16); err == nil {
		t.Fatal("expected error for node 300")
	}
	// empty address
	if err := b.Start(fastProfile(), "", 0, 16); err == nil {
		t.Fatal("expected error for empty n3 address")
	}

	// a real run occupies the bench: second Start must be rejected.
	if err := b.Start(fastProfile(), n3, 0, 16); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := b.Start(fastProfile(), n3, 0, 16); err == nil {
		t.Fatal("expected busy error while a run is active")
	}
	b.Cancel()
}

// TestBenchReservedSession verifies the bench claims the node's top
// sequence and encodes ns=N3 | node | seq into the F-TEID.
func TestBenchReservedSession(t *testing.T) {
	const node, sessions = 3, 256
	seq := sessions - 1
	teid := uint32(benchTeidNS)<<24 | uint32(node)<<16 | uint32(seq)
	if teid>>24 != 0x10 || (teid>>16)&0xff != node || teid&0xffff != uint32(seq) {
		t.Fatalf("bench teid layout wrong: 0x%08x", teid)
	}
}

// TestPercentile checks the quantile helper on a tiny slice.
func TestPercentile(t *testing.T) {
	s := []float64{1, 2, 3, 4, 5}
	cases := map[string]struct {
		q    float64
		want float64
	}{
		"p50": {0.50, 3},
		"p95": {0.95, 5},
		"p99": {0.99, 5},
		"min": {0.0001, 1},
	}
	for name, c := range cases {
		if got := percentile(s, c.q); got != c.want {
			t.Fatalf("%s = %v, want %v", name, got, c.want)
		}
	}
}
