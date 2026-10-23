// Package web serves the UPF fabric dashboard. It fans out /metrics and
// /api/flows requests to every UPF and traffic-generator pod, joins the
// snapshots, and renders a single-page Go template that polls /api/stats.
package web

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/midu/telco-upf-gen/internal/dbstore"
)

//go:embed templates/index.html
var tplFS embed.FS

type endpoint struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// Server aggregates the fabric control-lite endpoints.
type Server struct {
	httpClient *http.Client
	upfs       []endpoint
	tgs        []endpoint
	tpl        *template.Template
	store      *dbstore.Store // optional persistent store
}

// New builds the dashboard server. Specs are "name=http://host:port,..."
// (name may be omitted; it is then derived from the URL host).
func New(upfSpec, tgSpec string) *Server {
	return &Server{
		httpClient: &http.Client{Timeout: 3 * time.Second},
		upfs:       parseEndpoints(upfSpec),
		tgs:        parseEndpoints(tgSpec),
		tpl:        template.Must(template.ParseFS(tplFS, "templates/index.html")),
	}
}

// SetStore attaches the persistent store (nil-safe: UI history endpoints
// report db disabled until it is set).
func (s *Server) SetStore(st *dbstore.Store) { s.store = st }

func (s *Server) UpfCount() int { return len(s.upfs) }
func (s *Server) TgCount() int  { return len(s.tgs) }

// Run starts the dashboard HTTP server.
func (s *Server) Run(addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return srv.ListenAndServe()
}

func parseEndpoints(spec string) []endpoint {
	var out []endpoint
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, u, found := strings.Cut(part, "=")
		if !found {
			u = part
			name = hostName(part)
		}
		out = append(out, endpoint{Name: name, URL: strings.TrimSuffix(u, "/")})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func hostName(u string) string {
	p, err := url.Parse(u)
	if err != nil || p.Host == "" {
		return u
	}
	return strings.SplitN(p.Host, ":", 2)[0]
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/", s.index)
	mux.HandleFunc("/api/topology", s.topology)
	mux.HandleFunc("/api/stats", s.stats)
	// persistent-state APIs (UI refresh re-derives its state from the DB)
	mux.HandleFunc("/api/db", s.dbStatus)
	mux.HandleFunc("/api/knob", s.knob)
	mux.HandleFunc("/api/cdrs", s.cdRs)
	mux.HandleFunc("/api/dpi", s.dpiHistory)
	mux.HandleFunc("/api/metrics", s.metricsHistory)
	mux.HandleFunc("/api/events", s.events)
	// RFC 2544 benchmark dashboard (results read from the DB, control
	// forwarded to a generator)
	mux.HandleFunc("/api/rfc2544", s.rfc2544)
	mux.HandleFunc("/api/rfc2544/status", s.rfc2544Status)
	mux.HandleFunc("/api/rfc2544/cancel", s.rfc2544Cancel)
	return mux
}

func (s *Server) index(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.tpl.Execute(w, map[string]any{
		"UpfCount": len(s.upfs),
		"TgCount":  len(s.tgs),
	})
}

func (s *Server) topology(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"upfs": s.upfs,
		"tgs":  s.tgs,
	})
}

type report struct {
	Name  string         `json:"name"`
	URL   string         `json:"url"`
	Up    bool           `json:"up"`
	Data  map[string]any `json:"data,omitempty"`
	Flows []any          `json:"flows,omitempty"`
}

// stats joins every pod's snapshot in one request. One round trip per pod,
// bounded by a hard timeout; unreachable pods are reported with up=false so
// the UI can draw their state.
func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	start := time.Now()
	total := len(s.upfs) + len(s.tgs)
	outs := make([]report, total)
	var wg sync.WaitGroup

	do := func(i int, c endpoint, withFlows bool) {
		defer wg.Done()
		rep := report{Name: c.Name, URL: c.URL}
		data, err := s.fetch(c.URL + "/metrics")
		if err == nil {
			rep.Up = true
			rep.Data = data
		} else if err != nil {
			log.Printf("webui: %s: %v", c.Name, err)
		}
		if withFlows {
			if flows, err := s.fetchList(c.URL + "/api/flows"); err == nil {
				rep.Flows = flows
			}
		}
		outs[i] = rep
	}

	for i, c := range s.upfs {
		wg.Add(1)
		go do(i, c, false)
	}
	for i, c := range s.tgs {
		wg.Add(1)
		go do(len(s.upfs)+i, c, true)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ts":        start.UnixMilli(),
		"upfs":      outs[:len(s.upfs)],
		"tgs":       outs[len(s.upfs):],
		"latencyMs": time.Since(start).Milliseconds(),
	})
}

func (s *Server) fetch(url string) (map[string]any, error) {
	var out map[string]any
	return out, s.getJSON(url, &out)
}

func (s *Server) fetchList(url string) ([]any, error) {
	var out []any
	return out, s.getJSON(url, &out)
}

func (s *Server) getJSON(url string, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("webui: %w", err)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ---------------------------------------------------------------------------
// persistent-state APIs: the dashboard re-reads everything from the database
// on refresh, so closing the browser (or restarting this pod) loses nothing.
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) dbStatus(w http.ResponseWriter, _ *http.Request) {
	if s.store == nil {
		writeJSON(w, map[string]any{"enabled": false})
		return
	}
	writeJSON(w, s.store.Status())
}

func (s *Server) knob(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, s.GetKnob())
	case http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<10))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		trimmed := strings.TrimSpace(string(body))
		var pct float64
		if strings.HasPrefix(trimmed, "{") {
			var v struct {
				Pct float64 `json:"pct"`
			}
			if err := json.Unmarshal([]byte(trimmed), &v); err != nil {
				http.Error(w, "expected {\"pct\":0-100}", http.StatusBadRequest)
				return
			}
			pct = v.Pct
		} else if trimmed == "" {
			http.Error(w, "expected {\"pct\":0-100}", http.StatusBadRequest)
			return
		} else if pct, err = strconv.ParseFloat(trimmed, 64); err != nil {
			http.Error(w, "expected a number 0-100", http.StatusBadRequest)
			return
		}
		pct, err = s.SetKnob(r.Context(), pct)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, KnobState{Pct: pct, Source: "set"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) cdRsEnabled() (*dbstore.Store, error) {
	if s.store == nil {
		return nil, errDBDisabled
	}
	return s.store, nil
}

var errDBDisabled = fmt.Errorf("database not configured (set DB_DSN on the webui)")

// cdRs lists recently closed CDRs from the database.
func (s *Server) cdRs(w http.ResponseWriter, r *http.Request) {
	store, err := s.cdRsEnabled()
	if err != nil {
		writeJSON(w, map[string]any{"cdrs": nil, "error": err.Error()})
		return
	}
	sinceMin := queryInt(r, "since_min", 10)
	limit := queryInt(r, "limit", 500)
	cdrs, err := store.CDRsSince(time.Now().Add(-time.Duration(sinceMin)*time.Minute), limit)
	if err != nil {
		writeJSON(w, map[string]any{"cdrs": nil, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"cdrs": cdrs, "count": len(cdrs)})
}

func (s *Server) dpiHistory(w http.ResponseWriter, r *http.Request) {
	store, err := s.cdRsEnabled()
	if err != nil {
		writeJSON(w, map[string]any{"rows": nil, "error": err.Error()})
		return
	}
	sinceMin := queryInt(r, "since_min", 10)
	limit := queryInt(r, "limit", 5000)
	rows, err := store.DPISumSince(time.Now().Add(-time.Duration(sinceMin)*time.Minute), limit)
	if err != nil {
		writeJSON(w, map[string]any{"rows": nil, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"rows": rows, "count": len(rows)})
}

func (s *Server) metricsHistory(w http.ResponseWriter, r *http.Request) {
	store, err := s.cdRsEnabled()
	if err != nil {
		writeJSON(w, map[string]any{"rows": nil, "error": err.Error()})
		return
	}
	sinceMin := queryInt(r, "since_min", 10)
	limit := queryInt(r, "limit", 5000)
	rows, err := store.MetricsSince(time.Now().Add(-time.Duration(sinceMin)*time.Minute), limit)
	if err != nil {
		writeJSON(w, map[string]any{"rows": nil, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"rows": rows, "count": len(rows)})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	store, err := s.cdRsEnabled()
	if err != nil {
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	kind := r.URL.Query().Get("kind")
	limit := queryInt(r, "limit", 200)
	var rows any
	switch kind {
	case "policy", "":
		rows, err = store.PolicyEvents(limit)
	case "li":
		rows, err = store.LIEvents(limit)
	default:
		http.Error(w, "kind must be policy|li", http.StatusBadRequest)
		return
	}
	if err != nil {
		writeJSON(w, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, map[string]any{"kind": kind, "rows": rows})
}

// ---------------------------------------------------------------------------
// RFC 2544 dashboard APIs
// ---------------------------------------------------------------------------

// pickTG selects one traffic generator endpoint (tg=<name> query param;
// default = the first configured one).
func (s *Server) pickTG(name string) (endpoint, error) {
	if len(s.tgs) == 0 {
		return endpoint{}, fmt.Errorf("no traffic generators configured (set TRAFFICGENS)")
	}
	if name == "" {
		return s.tgs[0], nil
	}
	for _, c := range s.tgs {
		if c.Name == name {
			return c, nil
		}
	}
	return endpoint{}, fmt.Errorf("traffic generator %q not configured", name)
}

// forwardTG proxies one request to a generator and copies the response
// back (status code included), so the UI sees the generator's own errors.
func (s *Server) forwardTG(w http.ResponseWriter, r *http.Request, method, path string) {
	c, err := s.pickTG(r.URL.Query().Get("tg"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 6*time.Second)
	defer cancel()
	var rdr io.Reader
	if method == http.MethodPost || method == http.MethodPut {
		b, err := io.ReadAll(io.LimitReader(r.Body, 16<<10))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.URL+path, rdr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, io.LimitReader(resp.Body, 1<<20)); err != nil {
		log.Printf("webui: forwardTG %s %s: %v", method, path, err)
	}
}

// rfc2544: GET reads persisted measurement points (since_min minutes back,
// default 24h); POST forwards a start request to the selected generator.
func (s *Server) rfc2544(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		store, err := s.cdRsEnabled()
		if err != nil {
			writeJSON(w, map[string]any{"rows": nil, "error": err.Error()})
			return
		}
		sinceMin := queryInt(r, "since_min", 1440)
		limit := queryInt(r, "limit", 5000)
		rows, err := store.RFC2544Since(time.Now().Add(-time.Duration(sinceMin)*time.Minute), limit)
		if err != nil {
			writeJSON(w, map[string]any{"rows": nil, "error": err.Error()})
			return
		}
		writeJSON(w, map[string]any{"rows": rows, "count": len(rows)})
	case http.MethodPost:
		s.forwardTG(w, r, http.MethodPost, "/api/rfc2544")
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// rfc2544Status proxies the generator's live bench status (lite: no runs).
func (s *Server) rfc2544Status(w http.ResponseWriter, r *http.Request) {
	s.forwardTG(w, r, http.MethodGet, "/api/rfc2544?lite=1")
}

// rfc2544Cancel asks the selected generator to cancel its active run.
func (s *Server) rfc2544Cancel(w http.ResponseWriter, r *http.Request) {
	s.forwardTG(w, r, http.MethodPost, "/api/rfc2544/cancel")
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}
