package upf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/midu/telco-upf-gen/internal/dpi"
	"github.com/midu/telco-upf-gen/internal/gtpu"
	"github.com/midu/telco-upf-gen/internal/telemetry"
	"github.com/midu/telco-upf-gen/internal/usage"
)

var errLoop = errors.New("upf: F-TEID points at own node on N9")

// Config configures one UPF node.
type Config struct {
	NodeID          uint8
	NodeName        string
	Nodes           uint8  // total UPF nodes in the fabric (peers + self)
	PeerDNSTemplate string // e.g. "upf-{n}.upf-hs.telco.svc.cluster.local"
	N3Port          int
	N5Port          int
	N9Port          int
	Sessions        int
	DNTarget        string // "host:port" for plain-IP delivery; empty = count only
	Workers         int

	// SPI/DPI pipeline (Inspection=false disables it entirely; the data
	// plane then forwards without inspection, zero overhead).
	Inspection  bool
	PolicyPath  string        // optional YAML policy file (ConfigMap-mounted)
	LISelection string        // session selector "0-7,42" ("" = none selected)
	LIRing      int           // bounded LI/policy event rings
	CDRIdle     time.Duration // session inactivity that closes a CDR
	CDRMin      time.Duration // minimum session lifetime before closing
}

// UPF is one node of the distributed user plane.
type UPF struct {
	cfg      Config
	stats    *telemetry.Node
	sessions *Table
	engine   *dpi.Engine
	ledger   *usage.Ledger

	mu       sync.Mutex
	conns    map[string]*net.UDPConn // per-destination egress tunnels
	dn       *net.UDPConn
	dnKey    string                 // DN target the cached dn socket points at
	dnTarget atomic.Pointer[string] // runtime DN egress target ("host:port", "" = count only)
	cache    map[uint8]*net.UDPAddr // N9 peer address cache
}

// New builds the node (listeners are started by Run).
func New(cfg Config) *UPF {
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.Sessions < 1 {
		cfg.Sessions = 256
	}
	if cfg.N3Port == 0 {
		cfg.N3Port = 2152
	}
	if cfg.N5Port == 0 {
		cfg.N5Port = 2153
	}
	if cfg.N9Port == 0 {
		cfg.N9Port = 2154
	}
	u := &UPF{
		cfg:      cfg,
		stats:    telemetry.NewNode(cfg.NodeName, "upf", "N3", "N5", "N9"),
		sessions: NewTable(cfg.Sessions),
		conns:    map[string]*net.UDPConn{},
		cache:    map[uint8]*net.UDPAddr{},
	}
	u.dnTarget.Store(&cfg.DNTarget)
	if cfg.Inspection {
		policy, err := dpi.LoadPolicy(cfg.PolicyPath)
		if err != nil {
			log.Printf("upf %s: policy load failed, running allow-all: %v", cfg.NodeName, err)
			policy = &dpi.Policy{}
		}
		ring := cfg.LIRing
		if ring <= 0 {
			ring = 1024
		}
		u.engine = dpi.NewEngine(policy, cfg.LISelection, ring)
		u.ledger = usage.NewLedger(cfg.NodeName, int16(cfg.NodeID), cfg.CDRIdle, cfg.CDRMin)
	}
	return u
}

// Stats exposes the node telemetry root.
func (u *UPF) Stats() *telemetry.Node { return u.stats }

// Teids advertises the node's F-TEID space to the traffic generators, which
// derive F-TEIDs with the same deterministic scheme (internal/upf).
func (u *UPF) Teids() map[string]any {
	return map[string]any{
		"node":     int(u.cfg.NodeID),
		"name":     u.cfg.NodeName,
		"sessions": u.cfg.Sessions,
		"ports": map[string]int{
			"n3": u.cfg.N3Port,
			"n5": u.cfg.N5Port,
			"n9": u.cfg.N9Port,
		},
	}
}

// n9Addr resolves (and caches) a peer node's N9 endpoint.
func (u *UPF) n9Addr(id uint8) (*net.UDPAddr, error) {
	if id == u.cfg.NodeID {
		return nil, errLoop
	}
	u.mu.Lock()
	if da, ok := u.cache[id]; ok {
		u.mu.Unlock()
		return da, nil
	}
	u.mu.Unlock()

	name := strings.ReplaceAll(u.cfg.PeerDNSTemplate, "{n}", strconv.Itoa(int(id)))
	host := name
	port := u.cfg.N9Port
	if h, p, err := net.SplitHostPort(name); err == nil {
		host = h
		if pn, aerr := strconv.Atoi(p); aerr == nil {
			port = pn
		}
	}
	ip, err := net.LookupIP(host)
	if err != nil {
		return nil, fmt.Errorf("upf: resolve %s: %w", host, err)
	}
	da := &net.UDPAddr{IP: ip[0], Port: port}

	u.mu.Lock()
	u.cache[id] = da
	u.mu.Unlock()
	return da, nil
}

func (u *UPF) connTo(dst *net.UDPAddr) (*net.UDPConn, error) {
	key := dst.String()
	u.mu.Lock()
	if c, ok := u.conns[key]; ok {
		u.mu.Unlock()
		return c, nil
	}
	u.mu.Unlock()

	c, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		return nil, err
	}
	u.mu.Lock()
	u.conns[key] = c
	u.mu.Unlock()
	return c, nil
}

// dnConn returns the cached DN egress socket, dialing (or re-dialing after
// a target switch) as needed.
func (u *UPF) dnConn() (*net.UDPConn, error) {
	target := *u.dnTarget.Load()

	u.mu.Lock()
	defer u.mu.Unlock()
	if u.dnKey == target && u.dn != nil {
		return u.dn, nil
	}
	if u.dn != nil { // stale socket after a SetDNTarget switch
		_ = u.dn.Close()
		u.dn = nil
		u.dnKey = ""
	}
	if target == "" {
		return nil, errors.New("upf: no DN target configured")
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return nil, fmt.Errorf("upf: bad DN target %q: %w", target, err)
	}
	ip, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	pn, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("upf: bad DN port in %q: %w", target, err)
	}
	c, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: ip[0], Port: pn})
	if err != nil {
		return nil, err
	}
	u.dn = c
	u.dnKey = target
	return c, nil
}

// SetDNTarget switches the plain-IP DN egress target ("host:port") at
// runtime; empty reverts to count-only mode. Safe against the forwarding
// hot path: the cached socket is dropped and re-dialed lazily.
func (u *UPF) SetDNTarget(target string) error {
	if target != "" {
		host, port, err := net.SplitHostPort(target)
		if err != nil || host == "" {
			return fmt.Errorf("upf: bad DN target %q (want host:port)", target)
		}
		if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
			return fmt.Errorf("upf: bad DN port in %q", target)
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.dnKey == target && (u.dn != nil || target == "") {
		u.dnTarget.Store(&target)
		return nil
	}
	if u.dn != nil {
		_ = u.dn.Close()
		u.dn = nil
		u.dnKey = ""
	}
	u.dnTarget.Store(&target)
	return nil
}

// DNTarget is the current runtime DN egress target ("host:port"; empty =
// count only).
func (u *UPF) DNTarget() string { return *u.dnTarget.Load() }

type job struct {
	buf   []byte
	src   *net.UDPAddr
	iface string
}

var bufPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 65536)
	return &b
}}

// Run starts the three UDP interfaces, the CDR sweeper and the forwarding
// worker pool. It blocks until ctx is cancelled or a listener fails.
func (u *UPF) Run(ctx context.Context) error {
	if u.ledger != nil {
		go u.cdrSweeper(ctx)
	}
	ifaces := []struct {
		name string
		port int
	}{
		{"N3", u.cfg.N3Port},
		{"N5", u.cfg.N5Port},
		{"N9", u.cfg.N9Port},
	}

	queue := make(chan job, 8192)
	var wg sync.WaitGroup
	for w := 0; w < u.cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case j := <-queue:
					u.ingress(j)
				}
			}
		}()
	}

	errc := make(chan error, len(ifaces))
	for _, ifc := range ifaces {
		wg.Add(1)
		go func(name string, port int) {
			defer wg.Done()
			conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: port})
			if err != nil {
				errc <- fmt.Errorf("upf: listen %s: %w", name, err)
				return
			}
			log.Printf("upf %s: %s ready on udp/:%d", u.cfg.NodeName, name, port)
			defer conn.Close()

			// Reads run on a short deadline so ctx cancellation always
			// unblocks this goroutine (UDP has no shutdown).
			for {
				if err := conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
					errc <- err
					return
				}
				bp := bufPool.Get().(*[]byte)
				n, src, err := conn.ReadFromUDP((*bp)[:cap(*bp)])
				if n > 0 {
					*bp = (*bp)[:n]
					select {
					case queue <- job{buf: *bp, src: src, iface: name}:
						continue
					case <-ctx.Done():
					}
					*bp = (*bp)[:0]
					bufPool.Put(bp)
					if ctx.Err() != nil {
						return
					}
					continue
				}
				bufPool.Put(bp)
				if err != nil {
					var opErr *net.OpError
					if errors.As(err, &opErr) && opErr.Timeout() {
						if ctx.Err() != nil {
							return
						}
						continue
					}
					log.Printf("upf %s: %s read: %v", u.cfg.NodeName, name, err)
					errc <- err
					return
				}
			}
		}(ifc.name, ifc.port)
	}

	select {
	case <-ctx.Done():
		wg.Wait()
		log.Printf("upf %s: shut down", u.cfg.NodeName)
		return nil
	case err := <-errc:
		wg.Wait()
		return err
	}
}

// ingress is the forwarding state machine, executed by the worker pool.
func (u *UPF) ingress(j job) {
	defer func() {
		b := j.buf[:0]
		bufPool.Put(&b)
	}()

	f := u.stats.Iface(j.iface)
	f.RxPackets.Add(1)
	f.RxBytes.Add(uint64(len(j.buf)))

	pkt, err := gtpu.Parse(j.buf)
	if err != nil {
		f.Dropped.Add(1)
		f.Errors.Add(1)
		return
	}

	teid := pkt.Hdr.Teid
	ns := teidNS(teid)
	seq := teidSeq(teid)
	tnode := int(teidNode(teid))
	if _, ok := u.sessions.Get(seq); !ok {
		f.Dropped.Add(1) // unknown session: PDR mismatch
		return
	}

	switch j.iface {
	case "N3":
		// Uplink: UE/gNB direction.
		if ns != TeidNSN3 || tnode < 0 || tnode >= int(u.cfg.Nodes) {
			f.Dropped.Add(1)
			f.Errors.Add(1)
			return
		}
		if u.engine != nil && !u.inspect("N3", usage.DirUL, seq, teid, j.src, pkt.Payload) {
			f.Dropped.Add(1)
			return
		}
		if tnode == int(u.cfg.NodeID) {
			// Locally anchored session: decap, deliver plain IP to the DN.
			u.deliverDN(pkt.Payload)
		} else {
			// Anchored at a peer UPF: retag into the N9 namespace and cross
			// the east-west fabric.
			u.forwardN9(uint8(tnode), teid, pkt.Payload)
		}

	case "N9":
		// Arriving from a peer UPF.
		if ns != TeidNSN9 || tnode != int(u.cfg.NodeID) {
			f.Dropped.Add(1)
			f.Errors.Add(1)
			return
		}
		// Strip the tunnel and deliver the user payload to the DN. This is
		// also the loop guard: N9 ingress never re-forwards on N9.
		u.deliverDN(pkt.Payload)

	case "N5":
		// Downlink: DN direction.
		if ns != TeidNSN5 || tnode < 0 || tnode >= int(u.cfg.Nodes) {
			f.Dropped.Add(1)
			f.Errors.Add(1)
			return
		}
		if u.engine != nil && !u.inspect("N5", usage.DirDL, seq, teid, j.src, pkt.Payload) {
			f.Dropped.Add(1)
			return
		}
		if tnode == int(u.cfg.NodeID) {
			// Locally anchored: cap the user datagram into the N3 tunnel
			// and send it back towards the source gNB/UE.
			sess, _ := u.sessions.Get(seq)
			n3 := u.stats.Iface("N3")
			out := gtpu.NewPacket(sess.N3Teid(uint32(u.cfg.NodeID)), pkt.Payload)
			dst := &net.UDPAddr{IP: j.src.IP, Port: u.cfg.N3Port}
			c, err := u.connTo(dst)
			if err != nil {
				f.Errors.Add(1)
				return
			}
			buf := out.Marshal()
			if _, err := c.Write(buf); err != nil {
				f.Errors.Add(1)
				return
			}
			n3.Encapsulated.Add(1)
			n3.TxPackets.Add(1)
			n3.TxBytes.Add(uint64(len(buf)))
		} else {
			// Anchored at a peer: retag into the N9 namespace and forward.
			u.forwardN9(uint8(tnode), teid, pkt.Payload)
		}

	default:
		f.Errors.Add(1)
	}
}

// forwardN9 retags the packet for the N9 namespace of the destination node
// and transmits one G-PDU to that peer's N9 endpoint.
func (u *UPF) forwardN9(dst uint8, teid uint32, payload []byte) {
	n9 := u.stats.Iface("N9")
	da, err := u.n9Addr(dst)
	if err != nil {
		n9.Dropped.Add(1)
		n9.Errors.Add(1)
		return
	}
	out := gtpu.NewPacket(NewTeid(TeidNSN9, uint32(dst), teidSeq(teid)), payload)
	c, err := u.connTo(da)
	if err != nil {
		n9.Dropped.Add(1)
		n9.Errors.Add(1)
		return
	}
	buf := out.Marshal()
	if _, err := c.Write(buf); err != nil {
		n9.Dropped.Add(1)
		n9.Errors.Add(1)
		return
	}
	n9.Encapsulated.Add(1)
	n9.TxPackets.Add(1)
	n9.TxBytes.Add(uint64(len(buf)))
}

// deliverDN strips the tunnel and egresses the plain-IP user datagram onto
// the data network (N5/N6 direction). Without a DN target the packet is
// counted and absorbed (synthetic internet).
func (u *UPF) deliverDN(payload []byte) {
	n5 := u.stats.Iface("N5")
	n5.Decapsulated.Add(1)
	n5.TxPackets.Add(1)
	n5.TxBytes.Add(uint64(len(payload)))
	if u.DNTarget() == "" {
		return
	}
	c, err := u.dnConn()
	if err != nil {
		n5.Errors.Add(1)
		return
	}
	if _, err := c.Write(payload); err != nil {
		n5.Errors.Add(1)
	}
}

// inspect runs the SPI/DPI/policy pipeline over one incoming G-PDU payload
// on an ingress interface (N3=UL, N5=DL) and returns false if the policy
// dropped the packet. Usage (CDR) accounting happens here as well, exactly
// once per packet: N9 is pure transit and never re-accounts.
func (u *UPF) inspect(iface, dir string, seq uint32, teid uint32, src *net.UDPAddr, payload []byte) bool {
	v := u.engine.Inspect(iface, dir, seq, teid, src.String(), u.cfg.NodeName, payload)
	if !v.Allow {
		u.stats.Gauge("policyDrops").Add(1)
	}
	if v.SPI.Valid {
		u.ledger.Touch(seq, dir, uint16(v.AppID), uint64(len(payload)), v.SPI.Src, v.SPI.Dst)
	}
	return v.Allow
}

// cdrSweeper closes idle sessions into CDRs on a coarse cadence.
func (u *UPF) cdrSweeper(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			u.ledger.CloseIdle(time.Now()) // final sweep on shutdown
			return
		case <-t.C:
			u.ledger.CloseIdle(time.Now())
			u.stats.Gauge("activeSessions").Set(int64(u.ledger.Active()))
			u.stats.Gauge("cdrsPending").Set(int64(u.ledger.PendingCount()))
		}
	}
}

// Handler exposes the per-node control-lite endpoints.
func (u *UPF) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(u.stats.Snapshot())
	})
	mux.HandleFunc("/api/teids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(u.Teids())
	})
	// Runtime DN-egress switch, used by the traffic generator's RFC 2544
	// bench to point the DN target at its own sink for a measurement run.
	mux.HandleFunc("/api/dn-target", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]string{"target": u.DNTarget()})
		case http.MethodPost:
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var req struct {
				Target string `json:"target"`
			}
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, "expected {\"target\":\"host:port\" or \"\"}", http.StatusBadRequest)
				return
			}
			if err := u.SetDNTarget(req.Target); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			log.Printf("upf %s: DN target set to %q", u.cfg.NodeName, req.Target)
			_ = json.NewEncoder(w).Encode(map[string]string{"target": u.DNTarget()})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/dpi", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if u.engine == nil {
			_ = json.NewEncoder(w).Encode(map[string]any{"enabled": false})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"enabled": true, "engine": u.engine.Snapshot()})
	})
	mux.HandleFunc("/api/cdrs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if u.ledger == nil {
			_ = json.NewEncoder(w).Encode([]any{})
			return
		}
		_ = json.NewEncoder(w).Encode(u.ledger.DrainCDRs())
	})
	mux.HandleFunc("/api/events/policy", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if u.engine == nil {
			_ = json.NewEncoder(w).Encode([]any{})
			return
		}
		_ = json.NewEncoder(w).Encode(u.engine.PolicyEvents())
	})
	mux.HandleFunc("/api/events/li", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if u.engine == nil {
			_ = json.NewEncoder(w).Encode([]any{})
			return
		}
		_ = json.NewEncoder(w).Encode(u.engine.LIEvents())
	})
	return mux
}
