// Command trafficgen runs one traffic-generator pod. It discovers the UPF
// fabric via each node's /api/teids endpoint, then streams token-bucket
// GTP-U flows covering all three ingress paths:
//
//   - N3 anchored   : G-PDU on UPF N3 whose session is anchored locally
//   - N5 anchored   : G-PDU on UPF N5 (DN -> UE downlink) anchored locally
//   - east-west (EW): G-PDU on UPF N3/N5 whose session is anchored at a
//     PEER node; the UPF retags it into the N9 namespace and crosses the
//     fabric, which is what the UI plots as N9.
package main

import (
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

type upfInfo struct {
	Node     int            `json:"node"`
	Name     string         `json:"name"`
	Sessions int            `json:"sessions"`
	Ports    map[string]int `json:"ports"`
}

func main() {
	name := flag.String("name", envOr("POD_NAME", "trafficgen-0"), "pod name")
	peerDNS := flag.String("peer-dns", envOr("UPF_PEER_DNS", "upf-{n}.upf-hs.telco.svc.cluster.local"),
		"UPF pod DNS template, {n} = node ordinal")
	nodes := flag.Int("nodes", envIntOr("UPF_NODES", 2), "total UPF nodes")
	upfMetricsPort := flag.Int("upf-metrics", envIntOr("UPF_METRICS_PORT", 8080), "UPF control-lite HTTP port")
	metricsAddr := flag.String("metrics", ":8080", "this pod's HTTP listen address")
	pps := flag.Float64("pps", envFloatOr("TG_PPS", 100), "target pps per flow")
	pktSize := flag.Int("pkt-size", envIntOr("TG_PKT_SIZE", 256), "reserved (user datagrams auto-size per flow)")
	maxSess := flag.Int("max-sessions", envIntOr("TG_MAX_SESSIONS", 32), "max sessions targeted per UPF node")
	interval := flag.Duration("interval", 20*time.Millisecond, "token-bucket tick")
	duration := flag.Duration("duration", 0, "stop after this duration (0 = run forever)")
	flag.Parse()

	_ = pktSize // datagram size derives from the F-TEID; kept for CLI parity

	upfs, err := discover(*peerDNS, *nodes, *upfMetricsPort)
	if err != nil {
		log.Fatalf("trafficgen: %v", err)
	}

	gen := traffic.NewGenerator(*name, *interval)
	count := 0
	for _, info := range upfs {
		host := strings.ReplaceAll(*peerDNS, "{n}", strconv.Itoa(info.Node))
		m := *maxSess
		if info.Sessions > 0 && info.Sessions < m {
			m = info.Sessions
		}
		for s := 0; s < m; s++ {
			seq := uint32(s)
			anchor := (info.Node + s) % *nodes
			n3port := info.Ports["n3"]
			n5port := info.Ports["n5"]
			n9port := info.Ports["n9"]
			if n3port == 0 && n5port == 0 {
				log.Printf("trafficgen: %s: no ports advertised, skipping flows", info.Name)
				break
			}
			if anchor == info.Node {
				// Locally anchored: N3 (uplink) + N5 (downlink) flows.
				if n3port != 0 {
					count += addFlow(gen, host, strconv.Itoa(n3port), "n3", fmt.Sprintf("%s-n3-s%03d", info.Name, s),
						upf.NewTeid(upf.TeidNSN3, uint32(info.Node), seq), *pps)
				}
				if n5port != 0 {
					count += addFlow(gen, host, strconv.Itoa(n5port), "n5", fmt.Sprintf("%s-n5-s%03d", info.Name, s),
						upf.NewTeid(upf.TeidNSN5, uint32(info.Node), seq), *pps)
				}
				continue
			}
			// East-west: session anchored at `anchor`, injected at this UPF.
			// The UPF sees the remote node bits, retags into the N9
			// namespace and crosses the fabric.
			switch s % 4 {
			case 1: // EW via N3 ingress
				if n3port != 0 {
					count += addFlow(gen, host, strconv.Itoa(n3port), "n3", fmt.Sprintf("%s-n3ew-s%03d@n%d", info.Name, s, anchor),
						upf.NewTeid(upf.TeidNSN3, uint32(anchor), seq), *pps)
				}
			case 3: // EW via N5 ingress
				if n5port != 0 {
					count += addFlow(gen, host, strconv.Itoa(n5port), "n5", fmt.Sprintf("%s-n5ew-s%03d@n%d", info.Name, s, anchor),
						upf.NewTeid(upf.TeidNSN5, uint32(anchor), seq), *pps)
				}
			default: // EW direct on N9
				if n9port != 0 {
					count += addFlow(gen, host, strconv.Itoa(n9port), "n9", fmt.Sprintf("%s-n9ew-s%03d@n%d", info.Name, s, anchor),
						upf.NewTeid(upf.TeidNSN9, uint32(info.Node), seq), *pps)
				}
			}
		}
	}
	log.Printf("trafficgen %s: %d flows against %d UPF node(s), ~%.0f pps aggregate",
		*name, count, len(upfs), float64(count)*(*pps))

	if err := gen.Dial(); err != nil {
		log.Fatalf("trafficgen: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		go func() {
			select {
			case <-time.After(*duration):
				log.Printf("trafficgen: duration elapsed, stopping")
				stop()
			case <-ctx.Done():
			}
		}()
	}
	gen.Start(ctx)

	go func() {
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
				if !first && snap.Ts > prev.Ts {
					i := snap.Ifaces[0]
					dt := float64(snap.Ts-prev.Ts) / 1000
					ppsNow := float64(i.TxPackets-prev.Ifaces[0].TxPackets) / dt
					log.Printf("trafficgen: %.0f pps tx (%d pkts total)", ppsNow, i.TxPackets)
				}
				prev, first = snap, false
			}
		}
	}()

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
	srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("trafficgen %s: metrics on %s", *name, *metricsAddr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("trafficgen: %v", err)
	}
}

func addFlow(gen *traffic.Generator, host, port, ifn, name string, teid uint32, pps float64) int {
	f := &traffic.Flow{
		Name:  name,
		Iface: ifn,
		Dest:  net.JoinHostPort(host, port),
		Teid:  teid,
		PPS:   pps,
		Size:  256,
	}
	gen.AddFlow(f)
	return 1
}

// discover polls every UPF's /api/teids until the fabric answers (bounded).
func discover(tmpl string, n, metricsPort int) ([]upfInfo, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	infos := make([]upfInfo, 0, n)
	missing := make([]int, n)
	for i := range missing {
		missing[i] = i
	}
	for attempt := 0; attempt < 45 && len(missing) > 0; attempt++ {
		var still []int
		for _, i := range missing {
			host := strings.ReplaceAll(tmpl, "{n}", strconv.Itoa(i))
			url := fmt.Sprintf("http://%s:%d/api/teids", host, metricsPort)
			body, err := getJSON(client, url)
			if err != nil {
				still = append(still, i)
				continue
			}
			var info upfInfo
			if err := json.Unmarshal(body, &info); err != nil {
				still = append(still, i)
				continue
			}
			log.Printf("trafficgen: discovered %s (node %d, %d sessions)", info.Name, info.Node, info.Sessions)
			infos = append(infos, info)
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
