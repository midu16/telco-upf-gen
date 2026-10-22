// Package web serves the UPF fabric dashboard. It fans out /metrics and
// /api/flows requests to every UPF and traffic-generator pod, joins the
// snapshots, and renders a single-page Go template that polls /api/stats.
package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
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
