// Command trafficgen runs one traffic-generator instance.
//
// Modes:
//   - in-cluster : discovers the UPF fabric via each node's /api/teids
//     endpoint over the headless StatefulSet DNS (default; overridable in
//     the YAML config), then streams token-bucket GTP-U flows covering all
//     three ingress paths (N3 uplink, N5 downlink, N9 east-west).
//   - external   : a YAML config lists explicit UPF endpoints (host +
//     ports, IP:NodePort supported). Discovery is skipped, so the same
//     binary / container runs on any external machine that can reach the
//     fabric.
//
// The output level is a dynamic knob (0-100%) changeable at runtime:
//
//	GET  /api/throttle  -> {"throttle":75}
//	POST /api/throttle  body: 75  or  {"throttle":75}
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/midu/telco-upf-gen/internal/telemetry"
	"github.com/midu/telco-upf-gen/internal/traffic"
	"github.com/midu/telco-upf-gen/internal/upf"
)

// fabricNode is one addressed UPF endpoint (discovered or explicit).
type fabricNode struct {
	Host     string
	Node     int
	Sessions int
	Ports    map[string]int
	// ControlURL is the node's control-lite HTTP base ("" = unreachable
	// from this generator, e.g. external mode without upf[].control).
	ControlURL string
}

func main() {
	cfgPath := flag.String("config", envOr("TG_CONFIG", ""), "YAML config file (empty = env/flag defaults)")
	name := flag.String("name", "", "pod name (overrides config)")
	peerDNS := flag.String("peer-dns", "", "UPF pod DNS template, {n} = node ordinal (overrides config)")
	nodes := flag.Int("nodes", 0, "total UPF nodes (overrides config)")
	upfMetricsPort := flag.Int("upf-metrics", 0, "UPF control-lite HTTP port (overrides config)")
	metricsAddr := flag.String("metrics", envOr("TG_METRICS", ":8080"), "this pod's HTTP listen address")
	pps := flag.Float64("pps", 0, "target pps per flow at 100% (overrides config)")
	throttle := flag.Float64("throttle", 0, "initial output level 0-100 (overrides config)")
	maxSess := flag.Int("max-sessions", 0, "max sessions targeted per UPF node (overrides config)")
	interval := flag.Duration("interval", 0, "token-bucket tick (overrides config)")
	duration := flag.Duration("duration", 0, "stop after this duration (0 = run forever)")
	flag.Parse()

	cfg, err := loadResolvedConfig(*cfgPath, *name, *peerDNS, *nodes, *upfMetricsPort, *pps, *throttle, *maxSess, *interval, *duration)
	if err != nil {
		log.Fatalf("trafficgen: %v", err)
	}

	upfs, err := resolveFabric(cfg)
	if err != nil {
		log.Fatalf("trafficgen: %v", err)
	}

	gen := traffic.NewGenerator(cfg.Name, cfg.Interval)
	count := buildFlows(gen, upfs, cfg)
	bench := traffic.NewBench(gen, cfg.Bench)
	log.Printf("trafficgen %s: %d flows against %d UPF node(s), ~%.0f pps aggregate at 100%%, throttle %.0f%%",
		cfg.Name, count, len(upfs), float64(count)*cfg.PPS, gen.Throttle())

	if err := gen.Dial(); err != nil {
		log.Fatalf("trafficgen: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if cfg.Duration > 0 {
		go func() {
			select {
			case <-time.After(cfg.Duration):
				log.Printf("trafficgen: duration elapsed, stopping")
				stop()
			case <-ctx.Done():
			}
		}()
	}
	gen.Start(ctx)

	go ppsLogger(ctx, gen)

	srv := &http.Server{Addr: *metricsAddr, Handler: routes(gen, bench, upfs), ReadHeaderTimeout: 5 * time.Second}
	log.Printf("trafficgen %s: control HTTP on %s", cfg.Name, *metricsAddr)
	if err := srv.ListenAndServe(); err != nil && ctx.Err() == nil {
		log.Fatalf("trafficgen: %v", err)
	}
}

// loadResolvedConfig merges YAML config (if any) with flag overrides.
func loadResolvedConfig(cfgPath, name, peerDNS string, nodes, metricsPort int, pps, throttle float64, maxSess int, interval time.Duration, duration time.Duration) (*resolvedConfig, error) {
	rc := &resolvedConfig{
		Name:        envOr("POD_NAME", "trafficgen-0"),
		Interval:    20 * time.Millisecond,
		PPS:         100,
		Throttle:    100,
		MaxSess:     32,
		Duration:    duration,
		DNS:         envOr("UPF_PEER_DNS", "upf-{n}.upf-hs.telco.svc.cluster.local"),
		Nodes:       2,
		MetricsPort: 8080,
	}
	if v := envIntOr("UPF_NODES", 0); v > 0 {
		rc.Nodes = v
	}
	if v := envIntOr("UPF_METRICS_PORT", 0); v > 0 {
		rc.MetricsPort = v
	}
	if v := envFloatOr("TG_PPS", 0); v > 0 {
		rc.PPS = v
	}
	if v := envIntOr("TG_MAX_SESSIONS", 0); v > 0 {
		rc.MaxSess = v
	}

	if cfgPath != "" {
		c, err := traffic.LoadConfig(cfgPath)
		if err != nil {
			return nil, err
		}
		rc.Name = c.Name
		rc.Interval = c.Interval()
		rc.PPS = c.PPS
		rc.Throttle = c.Throttle
		rc.MaxSess = c.MaxSessions
		rc.External = c.External()
		rc.Ups = c.Ups
		rc.Bench = c.Bench
		if c.Discover != nil {
			rc.DNS = c.Discover.DNS
			rc.Nodes = c.Discover.Nodes
			rc.MetricsPort = c.Discover.MetricsPort
		}
		if d, err := c.ParseDuration(); err != nil {
			return nil, err
		} else if d > 0 {
			rc.Duration = d
		}
		log.Printf("trafficgen: config from %s", cfgPath)
	}

	// flags (when set) win over everything
	if name != "" {
		rc.Name = name
	}
	if peerDNS != "" {
		rc.DNS = peerDNS
	}
	if nodes > 0 {
		rc.Nodes = nodes
	}
	if metricsPort > 0 {
		rc.MetricsPort = metricsPort
	}
	if pps > 0 {
		rc.PPS = pps
	}
	if throttle > 0 {
		rc.Throttle = throttle
	}
	if maxSess > 0 {
		rc.MaxSess = maxSess
	}
	if interval > 0 {
		rc.Interval = interval
	}
	return rc, nil
}

type resolvedConfig struct {
	Name        string
	Interval    time.Duration
	PPS         float64
	Throttle    float64
	MaxSess     int
	Duration    time.Duration
	DNS         string
	Nodes       int
	MetricsPort int
	External    bool
	Ups         []traffic.UpfSpec
	Bench       traffic.BenchProfile
}

// resolveFabric returns the addressed UPF endpoints (discovered or explicit).
func resolveFabric(rc *resolvedConfig) ([]fabricNode, error) {
	if rc.External {
		out := make([]fabricNode, 0, len(rc.Ups))
		for i := range rc.Ups {
			u := &rc.Ups[i]
			out = append(out, fabricNode{
				Host:     u.Host,
				Node:     u.Node,
				Sessions: u.Sessions,
				Ports: map[string]int{
					"n3": u.Ports.N3,
					"n5": u.Ports.N5,
					"n9": u.Ports.N9,
				},
				ControlURL: controlURL(u.Host, u.Control),
			})
		}
		sort.Slice(out, func(a, b int) bool { return out[a].Node < out[b].Node })
		return out, nil
	}
	return discover(rc)
}

func controlURL(host string, port int) string {
	if port <= 0 {
		return ""
	}
	return fmt.Sprintf("http://%s:%d", host, port)
}

// buildFlows builds the normal traffic flows. Session `sessions-1` of each
// node is reserved for the RFC 2544 bench session, so normal flows cap at
// sessions-1 per node.
func buildFlows(gen *traffic.Generator, upfs []fabricNode, rc *resolvedConfig) int {
	nodes := len(upfs)
	if nodes == 0 {
		return 0
	}
	count := 0
	for _, info := range upfs {
		m := rc.MaxSess
		if info.Sessions > 1 && info.Sessions-1 < m {
			m = info.Sessions - 1 // reserve the top sequence for the bench
		}
		for s := 0; s < m; s++ {
			seq := uint32(s)
			anchor := (info.Node + s) % nodes
			n3port := info.Ports["n3"]
			n5port := info.Ports["n5"]
			n9port := info.Ports["n9"]
			if n3port == 0 && n5port == 0 {
				log.Printf("trafficgen: node %d (%s): no ports offered, skipping flows", info.Node, info.Host)
				break
			}
			if anchor == info.Node {
				// Locally anchored: N3 (uplink) + N5 (downlink) flows.
				if n3port != 0 {
					count += addFlow(gen, info.Host, n3port, "n3", fmt.Sprintf("%d-n3-s%03d", info.Node, s),
						upf.NewTeid(upf.TeidNSN3, uint32(info.Node), seq), rc.PPS)
				}
				if n5port != 0 {
					count += addFlow(gen, info.Host, n5port, "n5", fmt.Sprintf("%d-n5-s%03d", info.Node, s),
						upf.NewTeid(upf.TeidNSN5, uint32(info.Node), seq), rc.PPS)
				}
				continue
			}
			// East-west: session anchored at `anchor`, injected at this node.
			// The UPF retags into the N9 namespace and crosses the fabric.
			switch s % 4 {
			case 1: // EW via N3 ingress
				if n3port != 0 {
					count += addFlow(gen, info.Host, n3port, "n3", fmt.Sprintf("%d-n3ew-s%03d@n%d", info.Node, s, anchor),
						upf.NewTeid(upf.TeidNSN3, uint32(anchor), seq), rc.PPS)
				}
			case 3: // EW via N5 ingress
				if n5port != 0 {
					count += addFlow(gen, info.Host, n5port, "n5", fmt.Sprintf("%d-n5ew-s%03d@n%d", info.Node, s, anchor),
						upf.NewTeid(upf.TeidNSN5, uint32(anchor), seq), rc.PPS)
				}
			default: // EW direct on N9
				if n9port != 0 {
					count += addFlow(gen, info.Host, n9port, "n9", fmt.Sprintf("%d-n9ew-s%03d@n%d", info.Node, s, anchor),
						upf.NewTeid(upf.TeidNSN9, uint32(info.Node), seq), rc.PPS)
				}
			}
		}
	}
	return count
}

func addFlow(gen *traffic.Generator, host string, port int, ifn, name string, teid uint32, pps float64) int {
	gen.AddFlow(&traffic.Flow{
		Name:  name,
		Iface: ifn,
		Dest:  net.JoinHostPort(host, strconv.Itoa(port)),
		Teid:  teid,
		PPS:   pps,
		Size:  256,
	})
	return 1
}

func ppsLogger(ctx context.Context, gen *traffic.Generator) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	var prev telemetry.NodeSnap
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			snap := gen.Stats().Snapshot()
			if !first && snap.Ts > prev.Ts && len(snap.Ifaces) > 0 {
				i := snap.Ifaces[0]
				dt := float64(snap.Ts-prev.Ts) / 1000
				ppsNow := float64(i.TxPackets-prev.Ifaces[0].TxPackets) / dt
				log.Printf("trafficgen: %.0f pps tx (%.0f%% throttle, %d pkts total)",
					ppsNow, gen.Throttle(), i.TxPackets)
			}
			prev, first = snap, false
		}
	}
}

func routes(gen *traffic.Generator, bench *traffic.Bench, upfs []fabricNode) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(gen.Stats().Snapshot())
	})
	mux.HandleFunc("/api/flows", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(gen.FlowsSnapshot())
	})
	mux.HandleFunc("/api/throttle", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"throttle": gen.Throttle()})
		case http.MethodPost:
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			pct, err := parseThrottleBody(body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			v := gen.SetThrottle(pct)
			log.Printf("trafficgen: throttle set to %.1f%%", v)
			_ = json.NewEncoder(w).Encode(map[string]any{"throttle": v})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/rfc2544", rfc2544(bench, upfs))
	mux.HandleFunc("/api/rfc2544/cancel", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ok := bench.Cancel()
		if ok {
			log.Printf("trafficgen: rfc2544 run cancelled")
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"cancelled": ok})
	})
	return mux
}

// benchRequest selects profile overrides for one RFC 2544 run. Zero / empty
// fields fall back to the configured defaults; node selects the anchor by
// fabric identity (absent = first endpoint).
type benchRequest struct {
	FrameSizes     []int     `json:"frame_sizes"`
	CandidateRates []float64 `json:"candidate_rates"`
	LatencyRate    float64   `json:"latency_rate"`
	MinRate        float64   `json:"min_rate"`
	MaxRate        float64   `json:"max_rate"`
	DurationMs     int       `json:"duration_ms"`
	B2BSizes       []int     `json:"b2b_sizes"`
	Tests          []string  `json:"tests"`
	Node           *int      `json:"node"`
}

// rfc2544 exposes the benchmark control plane:
//
//	GET  /api/rfc2544[?lite=1]  -> {"status":…, "profile":…, "runs":[…]}
//	POST /api/rfc2544           -> body: profile overrides + {"node":n}
//	POST /api/rfc2544/cancel    -> stop the active run
func rfc2544(bench *traffic.Bench, upfs []fabricNode) http.HandlerFunc {
	client := &http.Client{Timeout: 5 * time.Second}
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("lite") == "1" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"status":  bench.Status(),
					"profile": bench.Profile(),
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":  bench.Status(),
				"profile": bench.Profile(),
				"runs":    bench.History(),
			})
		case http.MethodPost:
			body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var req benchRequest
			if len(bytes.TrimSpace(body)) > 0 {
				if err := json.Unmarshal(body, &req); err != nil {
					http.Error(w, "bad JSON body: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
			p := bench.Profile()
			if len(req.FrameSizes) > 0 {
				p.FrameSizes = req.FrameSizes
			}
			if len(req.CandidateRates) > 0 {
				p.CandidateRates = req.CandidateRates
			}
			if req.LatencyRate > 0 {
				p.LatencyRate = req.LatencyRate
			}
			if req.MinRate > 0 {
				p.MinRate = req.MinRate
			}
			if req.MaxRate > 0 {
				p.MaxRate = req.MaxRate
			}
			if req.DurationMs > 0 {
				p.DurationMs = req.DurationMs
			}
			if len(req.B2BSizes) > 0 {
				p.B2BSizes = req.B2BSizes
			}
			if len(req.Tests) > 0 {
				ts := make([]traffic.BenchTest, 0, len(req.Tests))
				for _, t := range req.Tests {
					ts = append(ts, traffic.BenchTest(strings.TrimSpace(t)))
				}
				p.Tests = ts
			}

			anchor, err := pickAnchor(upfs, req.Node)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if anchor.Ports["n3"] == 0 {
				http.Error(w, fmt.Sprintf("anchor upf-%d (%s) offers no N3 port", anchor.Node, anchor.Host), http.StatusBadRequest)
				return
			}
			if anchor.ControlURL == "" {
				http.Error(w, "anchor UPF control endpoint not reachable from this generator (in-cluster: provided by discovery; external mode: set upf[].control)", http.StatusConflict)
				return
			}
			ctrl := anchor.ControlURL
			bench.SetDeps(traffic.BenchDeps{
				SetDNTarget: func(target string) (string, error) {
					return requestDNTarget(client, ctrl, target)
				},
			})
			n3Addr := net.JoinHostPort(anchor.Host, strconv.Itoa(anchor.Ports["n3"]))
			if err := bench.Start(p, n3Addr, anchor.Node, anchor.Sessions); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}
			log.Printf("trafficgen: rfc2544 run started (anchor upf-%d %s, sizes %v, tests %v)",
				anchor.Node, n3Addr, p.FrameSizes, p.Tests)
			_ = json.NewEncoder(w).Encode(map[string]any{"started": true, "status": bench.Status()})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// pickAnchor resolves the bench anchor endpoint from a node id.
func pickAnchor(upfs []fabricNode, node *int) (fabricNode, error) {
	if len(upfs) == 0 {
		return fabricNode{}, fmt.Errorf("no UPF endpoints known")
	}
	if node == nil {
		return upfs[0], nil
	}
	for _, fn := range upfs {
		if fn.Node == *node {
			return fn, nil
		}
	}
	return fabricNode{}, fmt.Errorf("node %d not present in the fabric", *node)
}

// requestDNTarget GETs the UPF's current DN target and POSTs the new one,
// returning the previous value for the bench to restore.
func requestDNTarget(client *http.Client, ctrlURL, target string) (string, error) {
	body, err := getJSON(client, ctrlURL+"/api/dn-target")
	if err != nil {
		return "", fmt.Errorf("read current DN target: %w", err)
	}
	var cur struct {
		Target string `json:"target"`
	}
	if err := json.Unmarshal(body, &cur); err != nil {
		return "", fmt.Errorf("parse current DN target: %w", err)
	}
	payload, _ := json.Marshal(map[string]string{"target": target})
	resp, err := client.Post(ctrlURL+"/api/dn-target", "application/json", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusConflict || resp.StatusCode >= 500 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", fmt.Errorf("upf rejected DN target %q: status %d %s", target, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return cur.Target, nil
}

// parseThrottleBody accepts a bare number ("75") or JSON ({"throttle":75}).
func parseThrottleBody(body []byte) (float64, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return 0, errors.New("empty body (expected 0-100 or {\"throttle\":N})")
	}
	if strings.HasPrefix(trimmed, "{") {
		var v struct {
			Throttle float64 `json:"throttle"`
		}
		if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
			return 0, err
		}
		return v.Throttle, nil
	}
	pct, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("expected a number 0-100: %w", err)
	}
	return pct, nil
}

// discover polls every UPF's /api/teids until the fabric answers (bounded).
func discover(rc *resolvedConfig) ([]fabricNode, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	infos := make([]fabricNode, 0, rc.Nodes)
	missing := make([]int, rc.Nodes)
	for i := range missing {
		missing[i] = i
	}
	for attempt := 0; attempt < 45 && len(missing) > 0; attempt++ {
		var still []int
		for _, i := range missing {
			host := strings.ReplaceAll(rc.DNS, "{n}", strconv.Itoa(i))
			body, err := getJSON(client, fmt.Sprintf("http://%s:%d/api/teids", host, rc.MetricsPort))
			if err != nil {
				still = append(still, i)
				continue
			}
			var info struct {
				Node     int            `json:"node"`
				Name     string         `json:"name"`
				Sessions int            `json:"sessions"`
				Ports    map[string]int `json:"ports"`
			}
			if err := json.Unmarshal(body, &info); err != nil {
				still = append(still, i)
				continue
			}
			log.Printf("trafficgen: discovered %s (node %d, %d sessions)", info.Name, info.Node, info.Sessions)
			infos = append(infos, fabricNode{
				Host:       host,
				Node:       info.Node,
				Sessions:   info.Sessions,
				Ports:      info.Ports,
				ControlURL: fmt.Sprintf("http://%s:%d", host, rc.MetricsPort),
			})
		}
		sort.Slice(infos, func(a, b int) bool { return infos[a].Node < infos[b].Node })
		missing = still
		if len(still) > 0 {
			time.Sleep(2 * time.Second)
		}
	}
	if len(infos) == 0 {
		return nil, errors.New("no UPF endpoints reachable")
	}
	return infos, nil
}

func getJSON(client *http.Client, url string) ([]byte, error) {
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envIntOr(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloatOr(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}
