// Command upf runs one node of the distributed 5G UPF simulator.
//
// Each node listens for GTP-U on its N3, N5 (DN-facing) and N9 (peer UPF)
// endpoints, resolves F-TEIDs against the shared session space, and forwards
// user packets across the fabric. Node identity comes from the StatefulSet
// ordinal in the pod name; peers are addressed via a DNS template.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/midu/telco-upf-gen/internal/upf"
)

func main() {
	name := flag.String("node-name", envOr("POD_NAME", "upf-0"),
		"UPF node name; node ordinal derived from the numeric suffix")
	peerDNS := flag.String("peer-dns", envOr("UPF_PEER_DNS", "upf-{n}.upf-hs.telco.svc.cluster.local"),
		"peer UPF DNS template, {n} = node ordinal")
	nodes := flag.Int("nodes", envIntOr("UPF_NODES", 2), "total UPF nodes in the fabric")
	n3port := flag.Int("n3-port", envIntOr("N3_PORT", 2152), "N3 GTP-U UDP port (gNB direction)")
	n5port := flag.Int("n5-port", envIntOr("N5_PORT", 2153), "N5 GTP-U UDP port (DN direction)")
	n9port := flag.Int("n9-port", envIntOr("N9_PORT", 2154), "N9 GTP-U UDP port (peer UPF direction)")
	sessions := flag.Int("sessions", envIntOr("UPF_SESSIONS", 256), "shared F-TEID session space per node")
	workers := flag.Int("workers", envIntOr("UPF_WORKERS", 4), "per-node forwarding worker goroutines")
	dnTarget := flag.String("dn-target", envOr("DN_TARGET", ""),
		`DN egress "host:port" for plain-IP delivery (empty = count only)`)
	metricsAddr := flag.String("metrics", ":8080", "control-lite HTTP listen address")
	inspection := flag.Bool("inspection", envBoolOr("UPF_INSPECTION", true),
		"enable the SPI/DPI/policy/CDR pipeline")
	policyPath := flag.String("policy", envOr("UPF_POLICY", ""),
		"policy rules file (YAML); empty = allow-all")
	liSessions := flag.String("li-sessions", envOr("UPF_LI_SESSIONS", "0-7"),
		`lawful-interception session selector, e.g. "0-7,42"`)
	liRing := flag.Int("li-ring", envIntOr("UPF_LI_RING", 1024), "bounded event ring size")
	cdrIdle := flag.Duration("cdr-idle", envDurOr("UPF_CDR_IDLE", 15*time.Second), "session idle time that closes a CDR")
	cdrMin := flag.Duration("cdr-min", envDurOr("UPF_CDR_MIN", time.Second), "minimum session lifetime before a CDR closes")
	flag.Parse()

	id, err := ordinal(*name)
	if err != nil {
		log.Fatalf("upf: %v", err)
	}

	u := upf.New(upf.Config{
		NodeID:          id,
		NodeName:        *name,
		Nodes:           uint8(*nodes),
		PeerDNSTemplate: *peerDNS,
		N3Port:          *n3port,
		N5Port:          *n5port,
		N9Port:          *n9port,
		Sessions:        *sessions,
		DNTarget:        *dnTarget,
		Workers:         *workers,
		Inspection:      *inspection,
		PolicyPath:      *policyPath,
		LISelection:     *liSessions,
		LIRing:          *liRing,
		CDRIdle:         *cdrIdle,
		CDRMin:          *cdrMin,
	})
	log.Printf("upf %s: node %d of %d — N3 udp/:%d N5 udp/:%d N9 udp/:%d, %d sessions, %d workers",
		*name, id, *nodes, *n3port, *n5port, *n9port, *sessions, *workers)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		srv := &http.Server{Addr: *metricsAddr, Handler: u.Handler(), ReadHeaderTimeout: 5 * time.Second}
		log.Printf("upf %s: metrics on %s", *name, *metricsAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("upf: metrics server: %v", err)
			stop()
		}
	}()

	if err := u.Run(ctx); err != nil {
		log.Fatalf("upf: %v", err)
	}
}

// ordinal extracts the StatefulSet ordinal (pod name suffix) as the node ID.
func ordinal(name string) (uint8, error) {
	i := strings.LastIndex(name, "-")
	if i < 0 || i == len(name)-1 {
		return 0, fmt.Errorf("cannot derive node ordinal from %q (expect <name>-<n>)", name)
	}
	n, err := strconv.Atoi(name[i+1:])
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad ordinal in %q: %v", name, err)
	}
	if n > 255 {
		return 0, fmt.Errorf("ordinal %d out of range (0-255)", n)
	}
	return uint8(n), nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envBoolOr(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envDurOr(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
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
