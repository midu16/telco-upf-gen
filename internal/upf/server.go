package upf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/midu/telco-upf-gen/internal/gtpu"
	"github.com/midu/telco-upf-gen/internal/telemetry"
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
}

// UPF is one node of the distributed user plane.
type UPF struct {
	cfg      Config
	stats    *telemetry.Node
	sessions *Table

	mu    sync.Mutex
	conns map[string]*net.UDPConn // per-destination egress tunnels
	dn    *net.UDPConn
	cache map[uint8]*net.UDPAddr // N9 peer address cache
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
	return &UPF{
		cfg:      cfg,
		stats:    telemetry.NewNode(cfg.NodeName, "upf", "N3", "N5", "N9"),
		sessions: NewTable(cfg.Sessions),
		conns:    map[string]*net.UDPConn{},
		cache:    map[uint8]*net.UDPAddr{},
	}
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

func (u *UPF) dnConn() (*net.UDPConn, error) {
	if u.cfg.DNTarget == "" {
		return nil, errors.New("upf: no DN target configured")
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.dn != nil {
		return u.dn, nil
	}
	host, port, err := net.SplitHostPort(u.cfg.DNTarget)
	if err != nil {
		return nil, fmt.Errorf("upf: bad DN target %q: %w", u.cfg.DNTarget, err)
	}
	ip, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	pn, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("upf: bad DN port in %q: %w", u.cfg.DNTarget, err)
	}
	c, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: ip[0], Port: pn})
	if err != nil {
		return nil, err
	}
	u.dn = c
	return c, nil
}

type job struct {
	buf   []byte
	src   *net.UDPAddr
	iface string
}

var bufPool = sync.Pool{New: func() any {
	b := make([]byte, 0, 65536)
	return &b
}}

// Run starts the three UDP interfaces and the forwarding worker pool. It
// blocks until ctx is cancelled or a listener fails.
func (u *UPF) Run(ctx context.Context) error {
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
	if u.cfg.DNTarget == "" {
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
	return mux
}
