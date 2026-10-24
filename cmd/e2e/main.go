// Command e2e verifies a live deployment end to end:
//
//  1. pod health   — every UPF/TG/webui/DB pod is Ready
//  2. control plane— traffic-generator throttle API answers and clamps
//  3. traffic flow — UPF ingress counters increase between two samples
//  4. external CNI — real G-PDUs into the NodePort ingress (upf-ext) show
//     up in the counters (set E2E_EXTERNAL_ADDR=<ip>:32152;
//     skipped without it)
//  5. DB persist   — webui bridge writes metrics (+SPI/DPI rows) into Postgres
//  6. CDR report   — closed session CDRs land in the store
//  7. RFC 2544 run — a small benchmark completes and its results persist
//
// It talks to the cluster entirely through the `oc` CLI ($KUBECONFIG) and
// port-forwards, so it runs from any workstation with kube access. Exit
// code 0 = all checks passed.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"

	"github.com/midu/telco-upf-gen/internal/gtpu"
	"github.com/midu/telco-upf-gen/internal/upf"
)

type e2e struct {
	ctx      context.Context
	ns       string
	dbUser   string
	dbPass   string
	dbName   string
	children []*exec.Cmd
}

func (e *e2e) stop() {
	for _, c := range e.children {
		if c.Process != nil {
			_ = c.Process.Kill()
			_ = c.Wait()
		}
	}
}

func main() {
	ns := envOr("E2E_NAMESPACE", envOr("NAMESPACE", "telco"))
	timeout := envDurOr("E2E_TIMEOUT", 10*time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	e := &e2e{
		ctx:    ctx,
		ns:     ns,
		dbUser: envOr("E2E_DB_USER", "upfgen"),
		dbPass: envOr("E2E_DB_PASSWORD", "postgres"),
		dbName: envOr("E2E_DB_NAME", "telco_upf"),
	}
	defer e.stop()

	log.Printf("== e2e: start (namespace %s)", ns)
	for _, c := range []struct {
		name string
		fn   func() error
	}{{
		name: "cluster reachable",
		fn:   e.checkCluster,
	}, {
		name: "pod health",
		fn:   e.checkPods,
	}, {
		name: "control plane (throttle API)",
		fn:   e.checkThrottle,
	}, {
		name: "traffic flow (UPF ingress counters)",
		fn:   e.checkTrafficFlow,
	}, {
		name: "cross-CNI ingress (NodePort GTP-U, svc/upf-ext)",
		fn:   e.checkExternalCNI,
	}, {
		name: "database persistence",
		fn:   e.checkDB,
	}, {
		name: "CDR reporting",
		fn:   e.checkCDR,
	}, {
		name: "RFC 2544 baseline run (+ persistence)",
		fn:   e.checkRFC2544,
	}} {
		log.Printf("== e2e: %s", c.name)
		if err := c.fn(); err != nil {
			log.Printf("   FAIL %s: %v", c.name, err)
			log.Printf("E2E: FAIL")
			os.Exit(1)
		}
		log.Printf("   PASS")
	}
	log.Printf("E2E: PASS (namespace %s)", ns)
}

func (e *e2e) checkCluster() error {
	out, err := e.oc("get", "namespace", e.ns)
	if err != nil {
		return fmt.Errorf("oc get namespace: %w (is $KUBECONFIG set?)", err)
	}
	if !strings.Contains(out, e.ns) {
		return errors.New("namespace not found (run `make deploy` first)")
	}
	return nil
}

type k8sPod struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		Phase             string `json:"phase"`
		ContainerStatuses []struct {
			Name  string `json:"name"`
			Ready bool   `json:"ready"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (e *e2e) checkPods() error {
	waitPods := envDurOr("E2E_WAIT_PODS", 3*time.Minute)
	deadline := time.Now().Add(waitPods)
	var last string
	for time.Now().Before(deadline) {
		out, err := e.oc("-n", e.ns, "get", "pods", "-o", "json")
		if err != nil {
			last = err.Error()
			time.Sleep(5 * time.Second)
			continue
		}
		var list struct {
			Items []k8sPod `json:"items"`
		}
		if err := json.Unmarshal([]byte(out), &list); err != nil {
			last = "parse: " + err.Error()
			time.Sleep(5 * time.Second)
			continue
		}
		var bad []string
		var checked int
		for _, p := range list.Items {
			if !isWorkloadPod(p.Metadata.Name) {
				continue
			}
			checked++
			if p.Status.Phase != "Running" {
				bad = append(bad, fmt.Sprintf("%s=%s", p.Metadata.Name, p.Status.Phase))
				continue
			}
			for _, cs := range p.Status.ContainerStatuses {
				if !cs.Ready {
					bad = append(bad, fmt.Sprintf("%s/%s not ready", p.Metadata.Name, cs.Name))
				}
			}
		}
		if checked == 0 {
			bad = append(bad, "no workload pods found")
		}
		if len(bad) == 0 {
			return nil
		}
		last = strings.Join(bad, ", ")
		log.Printf("   waiting: %s", last)
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("pods not ready within %s (%s)", waitPods, last)
}

// isWorkloadPod matches the deployed component names (StatefulSet ordinals
// and the webui Deployment hash suffix are covered by prefix match).
func isWorkloadPod(name string) bool {
	for _, p := range []string{"upf-", "trafficgen-", "webui-", "db-"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

var httpC = &http.Client{Timeout: 5 * time.Second}

func (e *e2e) checkThrottle() error {
	tg, stop := e.portForward("tg", "svc/tg-hs", 18081, 8080)
	defer stop()
	base := "http://" + tg
	if err := waitHTTP(e.ctx, base+"/healthz"); err != nil {
		return fmt.Errorf("generator healthz: %w", err)
	}
	for _, v := range []struct {
		body string
		want float64
	}{{`91.5`, 91.5}, {`-10`, 0}, {`400`, 100}, {`72`, 72}} {
		var out map[string]any
		if err := postJSON(base+"/api/throttle", v.body, &out); err != nil {
			return fmt.Errorf("POST throttle %s: %w", v.body, err)
		}
		if got, _ := out["throttle"].(float64); got != v.want {
			return fmt.Errorf("throttle %s => %v, want %v", v.body, got, v.want)
		}
	}
	// restore default output level
	if err := postJSON(base+"/api/throttle", `100`, nil); err != nil {
		return err
	}
	return nil
}

type statsSnap struct {
	Upfs []struct {
		Up   bool `json:"up"`
		Data *struct {
			Ifaces []struct {
				Name      string `json:"name"`
				RxPackets uint64 `json:"rxPkts"`
			} `json:"ifaces"`
		} `json:"data"`
	} `json:"upfs"`
}

func (e *e2e) checkTrafficFlow() error {
	webui, stop := e.portForward("webui", "svc/webui", 18080, 80)
	defer stop()
	sample := rxSampler("http://" + webui)

	a, upA, err := sample()
	if err != nil {
		return fmt.Errorf("stats sample 1: %w", err)
	}
	if !upA {
		return errors.New("not all UPF endpoints report up in /api/stats")
	}
	window := 8 * time.Second
	time.Sleep(window)
	b, _, err := sample()
	if err != nil {
		return fmt.Errorf("stats sample 2: %w", err)
	}
	delta := int64(b) - int64(a)
	if delta <= 0 {
		return fmt.Errorf("no traffic observed in %s (rx delta = %d)", window, delta)
	}
	log.Printf("   UPF rx delta: %d pkts over %s", delta, window)
	return nil
}

// rxSampler returns a closure reading the cumulative per-UPF ingress
// counters once through the webui aggregation API (all upf ifaces summed).
func rxSampler(base string) func() (uint64, bool, error) {
	return func() (uint64, bool, error) {
		var s statsSnap
		if err := getJSON(base+"/api/stats", &s); err != nil {
			return 0, false, err
		}
		var total uint64
		allUp := len(s.Upfs) > 0
		for _, u := range s.Upfs {
			if u.Up && u.Data != nil {
				for _, f := range u.Data.Ifaces {
					total += f.RxPackets
				}
			} else {
				allUp = false
			}
		}
		return total, allUp, nil
	}
}

// checkExternalCNI drives G-PDUs from *this workstation* into the cluster's
// NodePort GTP-U ingress (deploy/external-ingress.yaml, svc/upf-ext) — i.e.
// traffic that crosses campus routing / NAT from a distinct network rather
// than the flat default CNI the in-cluster generator uses. The UPF N3
// counters must observe the burst. Optional: skipped unless
// E2E_EXTERNAL_ADDR=<node-ip>:<n3NodePort> is set (default nodePort 32152).
func (e *e2e) checkExternalCNI() error {
	addr := envOr("E2E_EXTERNAL_ADDR", "")
	if addr == "" {
		log.Printf("   SKIP (set E2E_EXTERNAL_ADDR=<node-ip>:32152; get it from oc get svc upf-ext)")
		return nil
	}
	webui, stop := e.portForward("webui", "svc/webui", 18081, 80)
	defer stop()
	sample := rxSampler("http://" + webui)

	a, upA, err := sample()
	if err != nil {
		return fmt.Errorf("stats sample 1: %w", err)
	}
	if !upA {
		return errors.New("not all UPF endpoints report up in /api/stats")
	}

	dst, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return fmt.Errorf("bad E2E_EXTERNAL_ADDR: %w", err)
	}
	conn, err := net.DialUDP("udp", nil, dst)
	if err != nil {
		return fmt.Errorf("dial %s: %w", dst, err)
	}
	defer conn.Close()

	// A valid locally-anchored N3 uplink on upf-0: any seq < UPF_SESSIONS
	// exists in the reserved session space, so the forwarded datagram
	// traverses the full F-TEID dispatch path.
	teid := upf.NewTeid(upf.TeidNSN3, 0, 1)
	payload := mockUplinkDatagram(teid, 128)
	buf := gtpu.NewPacket(teid, payload).Marshal()
	const nPkt = 64
	for i := 0; i < nPkt; i++ {
		if _, err := conn.Write(buf); err != nil {
			return fmt.Errorf("send: %w", err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	window := 8 * time.Second
	time.Sleep(window)
	b, _, err := sample()
	if err != nil {
		return fmt.Errorf("stats sample 2: %w", err)
	}
	delta := int64(b) - int64(a)
	if delta < nPkt {
		return fmt.Errorf("NodePort ingress %s delivered < %d packets over %s (upf-ext applied? node routing/firewall for udp/%d reachable?)",
			dst, nPkt, window, dst.Port)
	}
	log.Printf("   cross-CNI burst: %d pkts via %s propagated into UPF counters", nPkt, dst)
	return nil
}

// mockUplinkDatagram builds a wire-valid IPv4/UDP egress datagram for the
// inner payload of a G-PDU (same shape the in-cluster generator emits).
func mockUplinkDatagram(teid uint32, size int) []byte {
	seq := uint16(teid & 0xffff)
	src := net.IPv4(10, 40, 2, byte(1+(teid>>16)&0x7f)).To4()
	dst := net.IPv4(220, 200, 50, byte(seq%40+2)).To4()
	sp := uint16(40000 + (seq % 10000))
	dp := uint16(9000 + (seq % 1000))
	payload := make([]byte, size-28)
	for i := range payload {
		payload[i] = byte(i) + byte(seq)
	}
	return mockIPv4UDP(src, dst, sp, dp, payload)
}

// mockIPv4UDP assembles an IPv4 datagram carrying a UDP segment with valid
// header checksums (the UPF's PSI pipeline reads the inner headers).
func mockIPv4UDP(src, dst net.IP, srcPort, dstPort uint16, payload []byte) []byte {
	seg := make([]byte, 8+len(payload))
	seg[0], seg[1] = byte(srcPort>>8), byte(srcPort)
	seg[2], seg[3] = byte(dstPort>>8), byte(dstPort)
	seg[4], seg[5] = byte(uint16(len(seg))>>8), byte(uint16(len(seg)))
	copy(seg[8:], payload)

	ip := make([]byte, 20)
	ip[0] = 0x45 // v4, no options
	ip[3] = byte(20 + len(seg))
	ip[4] = byte((20 + len(seg)) >> 8) // total length
	ip[8] = 64                         // TTL
	ip[9] = 17                         // proto UDP
	copy(ip[12:16], src)
	copy(ip[16:20], dst)
	cs := mockIPUDPChecksum(ip, 17, seg)
	ip[10], ip[11] = byte(cs>>8), byte(cs)
	return append(ip, seg...)
}

// mockIPUDPChecksum is the 1's-complement checksum over the IPv4
// pseudo-header (src, dst, zero, proto, udp-length) plus the UDP segment.
func mockIPUDPChecksum(ip []byte, proto byte, seg []byte) uint16 {
	var sum uint32
	for i := 12; i < 16; i += 2 {
		sum += uint32(ip[i])<<8 | uint32(ip[i+1])
	}
	for i := 16; i < 20; i += 2 {
		sum += uint32(ip[i])<<8 | uint32(ip[i+1])
	}
	sum += uint32(proto) + uint32(len(seg))
	for i := 0; i+1 < len(seg); i += 2 {
		sum += uint32(seg[i])<<8 | uint32(seg[i+1])
	}
	if len(seg)%2 == 1 {
		sum += uint32(seg[len(seg)-1]) << 8
	}
	for sum > 0xffff {
		sum = (sum >> 16) + (sum & 0xffff)
	}
	return ^uint16(sum)
}

// --------------------------------------------------------------------- db

func (e *e2e) db() (*sql.DB, func(), error) {
	addr, stop := e.portForward("db", "svc/db", 15432, 5432)
	dh, dp, err := net.SplitHostPort(addr)
	if err != nil {
		stop()
		return nil, nil, err
	}
	// lib/pq resolves no embedded "host=ip:port" — split the port, or the
	// lookup fails as a hostname.
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		dh, dp, e.dbUser, e.dbPass, e.dbName))
	if err != nil {
		stop()
		return nil, nil, err
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if err := db.PingContext(e.ctx); err == nil {
			return db, stop, nil
		}
		time.Sleep(time.Second)
	}
	db.Close()
	stop()
	return nil, nil, errors.New("database did not accept connections")
}

func (e *e2e) checkDB() error {
	webui, stop := e.portForward("webui", "svc/webui", 18082, 80)
	defer stop()
	base := "http://" + webui

	var st map[string]any
	if err := getJSON(base+"/api/db", &st); err != nil {
		return fmt.Errorf("webui /api/db: %w", err)
	}
	if st["enabled"] != true {
		return errors.New("webui store not enabled (DB_DSN missing in deployment?)")
	}
	deadline := time.Now().Add(90 * time.Second)
	for st["connected"] != true {
		if time.Now().After(deadline) {
			return fmt.Errorf("webui store not connected (note: %v)", st["note"])
		}
		time.Sleep(3 * time.Second)
		if err := getJSON(base+"/api/db", &st); err != nil {
			return err
		}
	}

	db, stopDB, err := e.db()
	if err != nil {
		return err
	}
	defer stopDB()
	defer db.Close()

	if err := e.waitRows(db, `SELECT count(*) FROM metrics_ts`, 90*time.Second, "metrics_ts"); err != nil {
		return err
	}
	if err := e.waitRows(db, `SELECT count(*) FROM dpi_summary`, 90*time.Second, "dpi_summary"); err != nil {
		return fmt.Errorf("%w (is UPF inspection enabled?)", err)
	}

	// knob state round-trips through the database
	if err := postJSON(base+"/api/knob", `{"pct":88}`, nil); err != nil {
		return fmt.Errorf("set knob: %w", err)
	}
	var kb map[string]any
	if err := getJSON(base+"/api/knob", &kb); err != nil {
		return err
	}
	if kb["source"] != "db" {
		return fmt.Errorf("knob source = %v, want db (state not persisting)", kb["source"])
	}
	// The ui_state row is written by the webui's persistence cycle, not
	// synchronously with the POST: poll for the 88 so a leftover row from a
	// previous run's restore ({"pct":100}) cannot win the race.
	knDeadline := time.Now().Add(30 * time.Second)
	for {
		var raw string
		if err := db.QueryRowContext(e.ctx, `SELECT value::text FROM ui_state WHERE key=$1`, "throttle").Scan(&raw); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("read ui_state: %w", err)
		}
		if strings.Contains(raw, "88") {
			log.Printf("   ui_state throttle: %s (round-tripped)", raw)
			break
		}
		if time.Now().After(knDeadline) {
			return fmt.Errorf("ui_state throttle = %q, want 88 (flush window exceeded)", raw)
		}
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return postJSON(base+"/api/knob", `{"pct":100}`, nil)
}

func (e *e2e) waitRows(db *sql.DB, q string, timeout time.Duration, table string) error {
	deadline := time.Now().Add(timeout)
	for {
		var n int
		if err := db.QueryRowContext(e.ctx, q).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			log.Printf("   %s rows: %d", table, n)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no %s rows after %s", table, timeout)
		}
		time.Sleep(3 * time.Second)
	}
}

func (e *e2e) checkCDR() error {
	db, stop, err := e.db()
	if err != nil {
		return err
	}
	defer stop()
	defer db.Close()

	// CDRs close on the session idle window (default 15s): a session earns a
	// CDR only after staying packetless that long. Steady-state fabric at
	// 100% never idles any touched session — exactly what the earlier
	// checks left running — so the check itself creates the idle window:
	// every generator drops to 0 through the webui knob, wait for the
	// ledger to sweep, restore afterwards.
	webui, stopKnob := e.portForward("webui-knob", "svc/webui", 18084, 80)
	defer stopKnob()
	base := "http://" + webui

	rowsAt := func() int {
		var n int
		if err := db.QueryRowContext(e.ctx, `SELECT count(*) FROM cdr`).Scan(&n); err != nil {
			return -1
		}
		return n
	}
	start := rowsAt()

	if err := postJSON(base+"/api/knob", `{"pct":0}`, nil); err != nil {
		return fmt.Errorf("pause traffic (knob 0): %w", err)
	}

	// idle window + ledger CloseIdle sweep + bridge poll + upsert
	deadline := time.Now().Add(90 * time.Second)
	for {
		n := rowsAt()
		if n > start {
			// Restore before reporting (checked; the defer covers early exits).
			if err := postJSON(base+"/api/knob", `{"pct":100}`, nil); err != nil {
				return fmt.Errorf("restore knob 100: %w", err)
			}
			log.Printf("   cdr rows: start=%d now=%d (idle-window closure verified)", start, n)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no CDR rows within 90s of zero-offer (start=%d; idle window + flush latency exceeded)", start)
		}
		select {
		case <-e.ctx.Done():
			return e.ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// ------------------------------------------------------------------------- rfc2544

type benchRunOut struct {
	RunID    string `json:"runId"`
	Finished string `json:"finished"`
	Error    string `json:"error"`
	Results  []struct {
		Test     string  `json:"test"`
		Sent     uint64  `json:"sent"`
		Received uint64  `json:"received"`
		LossPct  float64 `json:"lossPct"`
	} `json:"results"`
}

// checkRFC2544 launches a small baseline benchmark against the live fabric,
// verifies the run completes with sane measurement points, and that the
// results reach the store through the webui bridge.
func (e *e2e) checkRFC2544() error {
	tg, stop := e.portForward("tg-rfc", "svc/tg-hs", 18083, 8080)
	defer stop()
	base := "http://" + tg

	var lite struct {
		Status struct {
			Active bool `json:"active"`
		} `json:"status"`
	}
	if err := getJSON(base+"/api/rfc2544?lite=1", &lite); err != nil {
		return fmt.Errorf("bench status: %w", err)
	}
	if lite.Status.Active {
		if err := postJSON(base+"/api/rfc2544/cancel", `{}`, nil); err != nil {
			return fmt.Errorf("cancel pre-existing run: %w", err)
		}
		time.Sleep(2 * time.Second)
	}

	// Baseline profile: smallest frame, tight windows (~10-15s wall time).
	req := `{"frame_sizes":[64],"duration_ms":300,"min_rate":100,"max_rate":4000,` +
		`"latency_rate":150,"b2b_sizes":[128,1024],` +
		`"tests":["loss","throughput","latency","back2back"]}`
	if err := postJSON(base+"/api/rfc2544", req, nil); err != nil {
		return fmt.Errorf("start bench: %w", err)
	}

	deadline := time.Now().Add(2 * time.Minute)
	var last struct {
		Runs []benchRunOut `json:"runs"`
	}
	for time.Now().Before(deadline) {
		if err := getJSON(base+"/api/rfc2544", &last); err != nil {
			return err
		}
		if len(last.Runs) > 0 && last.Runs[0].Finished != "" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if len(last.Runs) == 0 || last.Runs[0].Finished == "" {
		_ = postJSON(base+"/api/rfc2544/cancel", `{}`, nil)
		return errors.New("bench run did not finish within 2m (cancelled)")
	}
	run := last.Runs[0]
	if run.Error != "" {
		return fmt.Errorf("bench run %s failed: %s", run.RunID, run.Error)
	}
	if len(run.Results) == 0 {
		return fmt.Errorf("bench run %s produced no results", run.RunID)
	}
	procedures := map[string]bool{}
	for _, r := range run.Results {
		if r.Sent == 0 {
			return fmt.Errorf("procedure %s measured nothing sent", r.Test)
		}
		if r.LossPct < 0 || r.LossPct > 100 {
			return fmt.Errorf("procedure %s lossPct = %v", r.Test, r.LossPct)
		}
		procedures[r.Test] = true
	}
	for _, t := range []string{"loss", "throughput", "latency", "back2back"} {
		if !procedures[t] {
			return fmt.Errorf("procedure %s missing from run %s", t, run.RunID)
		}
	}
	log.Printf("   run %s: %d measurement points", run.RunID, len(run.Results))

	// Results must reach the store (bridge polls + flushes within seconds).
	db, stopDB, err := e.db()
	if err != nil {
		return err
	}
	defer stopDB()
	defer db.Close()
	return e.waitRows(db, `SELECT count(*) FROM rfc2544_results`, 90*time.Second, "rfc2544_results")
}

// --------------------------------------------------------------- transport

// portForward starts `oc -n ns port-forward`, waits until the local port
// answers, and returns ("127.0.0.1:port", stopFunc). An already-open port
// (leftover forward from a previous run) is reused.
func (e *e2e) portForward(tag, what string, localPort, remotePort int) (string, func()) {
	addr := "127.0.0.1:" + strconv.Itoa(localPort)
	if alive(addr) {
		log.Printf("   reusing :%d for %s", localPort, what)
		return addr, func() {}
	}
	cmd := exec.CommandContext(e.ctx, "oc", "-n", e.ns, "port-forward", what, fmt.Sprintf("%d:%d", localPort, remotePort))
	discard := make([]byte, 4096)
	cmd.Stdout = writer(discard)
	cmd.Stderr = writer(discard)
	if err := cmd.Start(); err != nil {
		panic(fmt.Sprintf("port-forward start: %v", err))
	}
	e.children = append(e.children, cmd)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if alive(addr) {
			log.Printf("   %s -> :%d", what, localPort)
			return addr, func() {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	panic(fmt.Sprintf("port-forward for %s did not come up on %s", what, addr))
}

// writer returns an io.Writer that swallows output (shared buffer).
type swriter struct{ buf []byte }

func writer(buf []byte) *swriter { return &swriter{buf: buf} }

func (w *swriter) Write(p []byte) (int, error) {
	copy(w.buf, p)
	return len(p), nil
}

func alive(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

// oc runs the oc CLI (honours $KUBECONFIG from the environment).
func (e *e2e) oc(args ...string) (string, error) {
	full := append([]string{}, args...)
	out, err := exec.CommandContext(e.ctx, "oc", full...).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func getJSON(url string, out any) error {
	resp, err := httpC.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func postJSON(url, body string, out any) error {
	resp, err := httpC.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: status %d", url, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// waitHTTP polls until the endpoint answers 200.
func waitHTTP(ctx context.Context, url string) error {
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := httpC.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("endpoint %s not ready", url)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
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
