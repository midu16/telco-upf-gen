// Command e2e verifies a live deployment end to end:
//
//  1. pod health   — every UPF/TG/webui/DB pod is Ready
//  2. control plane— traffic-generator throttle API answers and clamps
//  3. traffic flow — UPF ingress counters increase between two samples
//  4. DB persist   — webui bridge writes metrics (+SPI/DPI rows) into Postgres
//  5. CDR report   — closed session CDRs land in the store
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
		dbUser: envOr("E2E_DB_USER", "postgres"),
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
		name: "database persistence",
		fn:   e.checkDB,
	}, {
		name: "CDR reporting",
		fn:   e.checkCDR,
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
	webui, stop := e.portForward("webui", "svc/webui", 18080, 8080)
	defer stop()
	base := "http://" + webui

	sample := func() (uint64, bool, error) {
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

// --------------------------------------------------------------------- db

func (e *e2e) db() (*sql.DB, func(), error) {
	addr, stop := e.portForward("db", "svc/db", 15432, 5432)
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		addr, e.dbUser, e.dbPass, e.dbName))
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
	webui, stop := e.portForward("webui", "svc/webui", 18082, 8080)
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
	var raw string
	if err := db.QueryRowContext(e.ctx, `SELECT value::text FROM ui_state WHERE key=$1`, "throttle").Scan(&raw); err != nil {
		return fmt.Errorf("read ui_state: %w", err)
	}
	if !strings.Contains(raw, "88") {
		return fmt.Errorf("ui_state throttle = %s, want 88", raw)
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
	// CDRs close after the session idle window (default 15s) + one bridge
	// poll; give it room.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		var n int
		if err := db.QueryRowContext(e.ctx, `SELECT count(*) FROM cdr`).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			log.Printf("   cdr rows: %d", n)
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("no CDR rows after 3m (idle window + flush latency exceeded)")
		}
		time.Sleep(5 * time.Second)
	}
}

// ------------------------------------------------------------ transport

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
