// The bridge is the only component that writes to the persistent store.
// It polls every fabric endpoint a few times per second, converts the
// control-lite reports into store jobs and enqueues them (batched,
// transactional, buffered by the store itself). Dashboard state, knob
// position and historical rows all survive restarts because they live in
// the database, not in this process.
package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/midu/telco-upf-gen/internal/dbstore"
)

// knobKey is the ui_state row holding the traffic-knob position.
const knobKey = "throttle"

// poll types ---------------------------------------------------------------

type nodeSnap struct {
	Node   string           `json:"node"`
	Role   string           `json:"role"`
	Ts     int64            `json:"ts"`
	Ifaces []ifSnap         `json:"ifaces"`
	Gauges map[string]int64 `json:"gauges"`
}

type ifSnap struct {
	Name         string `json:"name"`
	RxPackets    uint64 `json:"rxPkts"`
	RxBytes      uint64 `json:"rxBytes"`
	TxPackets    uint64 `json:"txPkts"`
	TxBytes      uint64 `json:"txBytes"`
	Encapsulated uint64 `json:"encaps"`
	Decapsulated uint64 `json:"decaps"`
	Dropped      uint64 `json:"dropped"`
	Errors       uint64 `json:"errors"`
}

type dpiEngineSnap struct {
	Inspected  uint64    `json:"inspected"`
	LISelected int       `json:"liSessions"`
	Rows       []dpiRow  `json:"rows"`
	Rules      []dpiRule `json:"rules"`
}

type dpiRow struct {
	IFace   string `json:"iface"`
	Dir     string `json:"direction"`
	AppID   int16  `json:"appId"`
	AppName string `json:"appName"`
	Pkts    uint64 `json:"pkts"`
	Bytes   uint64 `json:"bytes"`
	Dropped uint64 `json:"dropped"`
}

type dpiRule struct {
	ID     string `json:"id"`
	Desc   string `json:"description"`
	Apps   string `json:"apps"`
	Proto  string `json:"proto"`
	Action string `json:"action"`
}

type spiSnap struct {
	Valid   bool   `json:"valid"`
	Proto   uint8  `json:"proto"`
	Src     string `json:"src"`
	Dst     string `json:"dst"`
	SrcPort uint16 `json:"srcPort"`
	DstPort uint16 `json:"dstPort"`
}

type policyEvSnap struct {
	IFace      string  `json:"iface"`
	Dir        string  `json:"direction"`
	OuterSrc   string  `json:"outerSrc"`
	OuterDst   string  `json:"outerDst"`
	SPI        spiSnap `json:"spi"`
	AppID      int16   `json:"appId"`
	AppName    string  `json:"appName"`
	RuleID     string  `json:"ruleId"`
	Action     int     `json:"action"` // 0 allow, 1 observe, 2 drop
	PayloadLen int32   `json:"payloadLen"`
}

type liEvSnap struct {
	SessionID  uint32    `json:"sessionId"`
	TEID       uint32    `json:"teid"`
	Dir        string    `json:"direction"`
	IFace      string    `json:"iface"`
	SPI        spiSnap   `json:"spi"`
	AppID      int16     `json:"appId"`
	AppName    string    `json:"appName"`
	Detail     string    `json:"detail"`
	PayloadLen int32     `json:"payloadLen"`
	Ts         time.Time `json:"ts"`
}

type cdrSnap struct {
	SessionID  uint64               `json:"sessionId"`
	NodeID     int16                `json:"nodeId"`
	AnchorPod  string               `json:"anchorPod"`
	UeAddr     string               `json:"ueAddr"`
	DnAddr     string               `json:"dnAddr"`
	UpPkts     uint64               `json:"upPkts"`
	DownPkts   uint64               `json:"downPkts"`
	UpBytes    uint64               `json:"upBytes"`
	DownBytes  uint64               `json:"downBytes"`
	StartedAt  time.Time            `json:"startedAt"`
	EndedAt    time.Time            `json:"endedAt"`
	DurationMS int64                `json:"durationMs"`
	Apps       map[string][2]uint64 `json:"apps"`
	Rating     string               `json:"rating"`
	Status     string               `json:"status"`
}

// rfcRunSnap is one finished benchmark run as reported by a generator
// (subset of /api/rfc2544); results reuse the store's wire type.
type rfcRunSnap struct {
	RunID    string                  `json:"runId"`
	Started  string                  `json:"started"`
	Finished string                  `json:"finished,omitempty"`
	Target   string                  `json:"target"`
	Error    string                  `json:"error,omitempty"`
	Results  []dbstore.RFC2544Result `json:"results"`
}

// Bridge aggregates the fabric and persists it.
type Bridge struct {
	store *dbstore.Store
	srv   *Server
	intv  time.Duration

	mu sync.Mutex
	// prevIf[key=pod|if] -> snapshot counters, for pps calculation
	prevIf map[string]prevIface
	// prevDPI[key=pod|if|dir|app] -> cumulative counters, deltas persisted
	prevDPI map[string]prevDPI
	// nodeIDs[pod] from /api/teids
	nodeIDs  map[string]int
	upfTeids map[string]bool
	// seenRuns[tgPod][runId] = true: benchmark runs already persisted
	seenRuns map[string]map[string]bool
}

type prevIface struct {
	ts     int64
	rxPkts uint64
	txPkts uint64
}

type prevDPI struct {
	pkts    uint64
	bytes   uint64
	dropped uint64
}

// NewBridge binds the store to the server. The store may be nil (disabled).
func NewBridge(store *dbstore.Store, srv *Server, interval time.Duration) *Bridge {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Bridge{
		store:    store,
		srv:      srv,
		intv:     interval,
		prevIf:   map[string]prevIface{},
		prevDPI:  map[string]prevDPI{},
		nodeIDs:  map[string]int{},
		seenRuns: map[string]map[string]bool{},
	}
}

// Run polls the fabric and restores the knob until ctx is done.
func (b *Bridge) Run(ctx context.Context) {
	if b.store == nil {
		return
	}
	b.restoreKnob(ctx)
	t := time.NewTicker(b.intv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.tick(ctx)
		}
	}
}

// restoreKnob re-applies the persisted knob position to every traffic
// generator (state retention across webui and generator restarts).
func (b *Bridge) restoreKnob(ctx context.Context) {
	stored, err := b.store.GetUIState(knobKey)
	if err != nil || len(stored) == 0 {
		return
	}
	var v struct {
		Pct float64 `json:"pct"`
	}
	if json.Unmarshal(stored, &v) != nil || v.Pct <= 0 || v.Pct > 100 {
		return
	}
	n := b.srv.applyThrottle(ctx, v.Pct)
	log.Printf("webui: restored knob to %.1f%% (%d/tg endpoints applied)", v.Pct, n)
}

func (b *Bridge) tick(ctx context.Context) {
	type job struct {
		do func()
	}
	var jobs []job
	for _, c := range b.srv.upfs {
		c := c
		jobs = append(jobs, job{do: func() { b.pollUPF(ctx, c) }})
	}
	for _, c := range b.srv.tgs {
		c := c
		jobs = append(jobs, job{do: func() { b.pollTG(ctx, c) }})
	}
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			j.do()
		}(j)
	}
	wg.Wait()
}

func (b *Bridge) pollUPF(ctx context.Context, c endpoint) {
	snap, ok := b.fetchNodeSnap(ctx, c.URL)
	if !ok {
		return
	}
	if _, known := b.nodeIDs[c.Name]; !known {
		if nid, err := b.fetchNodeID(ctx, c.URL); err == nil {
			b.nodeIDs[c.Name] = nid
		}
	}

	// metrics samples per interface
	b.mu.Lock()
	samples := make([]dbstore.CounterSample, 0, len(snap.Ifaces))
	for _, f := range snap.Ifaces {
		key := c.Name + "|" + f.Name
		prev := b.prevIf[key]
		var rxPps, txPps float64
		if prev.ts > 0 && snap.Ts > prev.ts {
			dt := float64(snap.Ts-prev.ts) / 1000
			rxPps = rate(f.RxPackets, prev.rxPkts, dt)
			txPps = rate(f.TxPackets, prev.txPkts, dt)
		}
		b.prevIf[key] = prevIface{ts: snap.Ts, rxPkts: f.RxPackets, txPkts: f.TxPackets}
		samples = append(samples, dbstore.CounterSample{
			Pod:      c.Name,
			IFace:    f.Name,
			RxPkts:   f.RxPackets,
			TxPkts:   f.TxPackets,
			RxBytes:  f.RxBytes,
			TxBytes:  f.TxBytes,
			Encaps:   f.Encapsulated,
			Decaps:   f.Decapsulated,
			Dropped:  f.Dropped,
			NErrors:  f.Errors,
			RxPps:    rxPps,
			TxPps:    txPps,
			Throttle: 100,
		})
	}
	b.mu.Unlock()
	for _, s := range samples {
		b.store.SampleMetric(s)
	}
	b.store.Heartbeat(c.Name, "upf", b.nodeIDs[c.Name], map[string]any{
		"activeSessions": snap.Gauges["activeSessions"],
		"cdrsPending":    snap.Gauges["cdrsPending"],
		"policyDrops":    snap.Gauges["policyDrops"],
	})

	// SPI/DPI aggregates (persisted as deltas)
	if body, err := b.getJSONRaw(ctx, c.URL+"/api/dpi"); err == nil {
		var env struct {
			Enabled bool           `json:"enabled"`
			Engine  *dpiEngineSnap `json:"engine"`
		}
		if json.Unmarshal(body, &env) == nil && env.Enabled && env.Engine != nil {
			b.persistDPIDeltas(c.Name, env.Engine.Rows)
		}
	}

	// policy + LI event drains
	if body, err := b.getJSONRaw(ctx, c.URL+"/api/events/policy"); err == nil {
		var evs []policyEvSnap
		if json.Unmarshal(body, &evs) == nil {
			for _, e := range evs {
				a := "observe"
				if e.Action == 2 {
					a = "drop"
				}
				proto := nilInt16(int16(e.SPI.Proto))
				sp, dp := nilInt32(int32(e.SPI.SrcPort)), nilInt32(int32(e.SPI.DstPort))
				appID := nilInt16(e.AppID)
				b.store.PolicySample(dbstore.PolicyEvent{
					Pod: c.Name, IFace: e.IFace, Direction: e.Dir,
					OuterSrc: e.OuterSrc, OuterDst: e.OuterDst,
					InnerSrc: e.SPI.Src, InnerDst: e.SPI.Dst,
					L4Proto: proto, SrcPort: sp, DstPort: dp,
					AppID: appID, AppName: e.AppName,
					RuleID: e.RuleID, Action: a, PayloadLen: e.PayloadLen,
				})
			}
		}
	}
	if body, err := b.getJSONRaw(ctx, c.URL+"/api/events/li"); err == nil {
		var evs []liEvSnap
		if json.Unmarshal(body, &evs) == nil {
			for _, e := range evs {
				proto := nilInt16(int16(e.SPI.Proto))
				sp, dp := nilInt32(int32(e.SPI.SrcPort)), nilInt32(int32(e.SPI.DstPort))
				appID := nilInt16(e.AppID)
				b.store.LISample(dbstore.LIEvent{
					Pod: c.Name, SessionID: uint64(e.SessionID),
					TEID:      fmt.Sprintf("0x%08x", e.TEID),
					Direction: e.Dir,
					InnerSrc:  e.SPI.Src, InnerDst: e.SPI.Dst,
					L4Proto: proto, SrcPort: sp, DstPort: dp,
					AppID: appID, AppName: e.AppName,
					Detail: e.Detail, PayloadLen: e.PayloadLen,
				})
			}
		}
	}

	// closed CDR drain
	if body, err := b.getJSONRaw(ctx, c.URL+"/api/cdrs"); err == nil {
		var cdns []cdrSnap
		if json.Unmarshal(body, &cdns) == nil {
			for _, ccr := range cdns {
				apps, _ := json.Marshal(ccr.Apps)
				b.store.UpsertCDR(dbstore.CDR{
					SessionID: ccr.SessionID, AnchorPod: ccr.AnchorPod,
					NodeID: ccr.NodeID, UeAddr: ccr.UeAddr, DnAddr: ccr.DnAddr,
					UpPkts: ccr.UpPkts, DownPkts: ccr.DownPkts,
					UpBytes: ccr.UpBytes, DownBytes: ccr.DownBytes,
					StartedAt: ccr.StartedAt, EndedAt: ccr.EndedAt,
					DurationMS: ccr.DurationMS, Apps: apps,
					Rating: ccr.Rating, Status: ccr.Status,
				})
			}
		}
	}
}

func (b *Bridge) pollTG(ctx context.Context, c endpoint) {
	snap, ok := b.fetchNodeSnap(ctx, c.URL)
	if !ok {
		return
	}
	throttle := 100.0
	if g, found := snap.Gauges["throttle"]; found {
		throttle = float64(g)
	}
	flows := 0
	if g, found := snap.Gauges["flows"]; found {
		flows = int(g)
	}
	for _, f := range snap.Ifaces {
		b.mu.Lock()
		key := c.Name + "|" + f.Name
		prev := b.prevIf[key]
		var txPps float64
		if prev.ts > 0 && snap.Ts > prev.ts {
			txPps = rate(f.TxPackets, prev.txPkts, float64(snap.Ts-prev.ts)/1000)
		}
		b.prevIf[key] = prevIface{ts: snap.Ts, txPkts: f.TxPackets}
		b.mu.Unlock()

		b.store.SampleMetric(dbstore.CounterSample{
			Pod: c.Name, IFace: f.Name,
			TxPkts: f.TxPackets, TxBytes: f.TxBytes,
			Encaps: f.Encapsulated, Dropped: f.Dropped, NErrors: f.Errors,
			TxPps: txPps, Throttle: throttle,
		})
	}
	b.store.Heartbeat(c.Name, "trafficgen", 0, map[string]any{
		"flows":    flows,
		"throttle": throttle,
	})
	b.pollRFC2544(ctx, c)
}

// pollRFC2544 persists finished benchmark runs of this generator. A run is
// re-upserted until all of its measurement points are verified present in
// the store (the upsert is idempotent), so a dropped write batch under queue
// pressure cannot lose a run — the next poll repairs it. Only then is the
// run marked seen, which bounds the re-check work.
func (b *Bridge) pollRFC2544(ctx context.Context, c endpoint) {
	var env struct {
		Runs []rfcRunSnap `json:"runs"`
	}
	if err := b.getJSON(ctx, c.URL+"/api/rfc2544", &env); err != nil {
		return // silent: the bench is optional and polled every tick
	}
	b.mu.Lock()
	seen, ok := b.seenRuns[c.Name]
	if !ok {
		seen = map[string]bool{}
		b.seenRuns[c.Name] = seen
	}
	var uns []dbstore.RFC2544Result
	var done []string
	for _, run := range env.Runs {
		if run.RunID == "" || seen[run.RunID] || run.Finished == "" {
			continue
		}
		if len(run.Results) == 0 { // nothing to persist (e.g. aborted run)
			seen[run.RunID] = true
			continue
		}
		have, err := b.store.RFC2544RunRows(run.RunID)
		if err == nil && have >= len(run.Results) {
			done = append(done, run.RunID) // fully stored; stop re-upserting
			continue
		}
		for _, r := range run.Results {
			if r.RunID == "" {
				r.RunID = run.RunID
			}
			if r.Ts == "" {
				r.Ts = time.Now().UTC().Format(time.RFC3339Nano)
			}
			uns = append(uns, r)
		}
	}
	for _, id := range done {
		seen[id] = true
	}
	b.mu.Unlock()
	if len(uns) > 0 {
		log.Printf("webui: persisting %d rfc2544 results from %s", len(uns), c.Name)
		b.store.UpsertRFC2544(c.Name, uns)
	}
}

// persistDPIDeltas converts cumulative engine rows into interval deltas.
// A counter reset (pod restart) drops the baseline for that key.
func (b *Bridge) persistDPIDeltas(pod string, rows []dpiRow) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range rows {
		key := pod + "|" + r.IFace + "|" + r.Dir + "|" + strconv.Itoa(int(r.AppID))
		prev := b.prevDPI[key]
		if r.Pkts < prev.pkts || r.Bytes < prev.bytes || r.Dropped < prev.dropped {
			b.prevDPI[key] = prevDPI{pkts: r.Pkts, bytes: r.Bytes, dropped: r.Dropped}
			continue
		}
		b.prevDPI[key] = prevDPI{pkts: r.Pkts, bytes: r.Bytes, dropped: r.Dropped}
		dPkts, dBytes, dDrop := r.Pkts-prev.pkts, r.Bytes-prev.bytes, r.Dropped-prev.dropped
		if dPkts == 0 && dDrop == 0 {
			continue
		}
		b.store.DPISample(dbstore.DPISummary{
			Pod: pod, IFace: r.IFace, Direction: r.Dir,
			AppID: r.AppID, AppName: r.AppName,
			Pkts: dPkts, NBytes: dBytes, Dropped: dDrop,
		})
	}
}

// helpers ---------------------------------------------------------------

func rate(new, old uint64, dt float64) float64 {
	if dt <= 0 || new < old {
		return 0
	}
	return float64(new-old) / dt
}

func nilInt16(v int16) *int16 {
	if v == 0 {
		return nil
	}
	return &v
}

func nilInt32(v int32) *int32 {
	if v == 0 {
		return nil
	}
	return &v
}

func (b *Bridge) fetchNodeSnap(ctx context.Context, base string) (nodeSnap, bool) {
	var snap nodeSnap
	if err := b.getJSON(ctx, base+"/metrics", &snap); err != nil {
		log.Printf("webui: %s metrics: %v", base, err)
		return snap, false
	}
	return snap, true
}

func (b *Bridge) fetchNodeID(ctx context.Context, base string) (int, error) {
	var info struct {
		Node int `json:"node"`
	}
	return info.Node, b.getJSON(ctx, base+"/api/teids", &info)
}

func (b *Bridge) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := b.srv.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (b *Bridge) getJSONRaw(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.srv.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 4<<20))
}

// knob and state APIs (served by the server, persisted here) ----------------

// KnobState is the stored traffic-knob position.
type KnobState struct {
	Pct    float64 `json:"pct"`
	Source string  `json:"source"` // db | default
}

// GetKnob returns the persisted knob position (DB on refresh = retention).
func (s *Server) GetKnob() KnobState {
	if s.store != nil {
		if raw, err := s.store.GetUIState(knobKey); err == nil && len(raw) > 0 {
			var v struct {
				Pct float64 `json:"pct"`
			}
			if json.Unmarshal(raw, &v) == nil && v.Pct > 0 && v.Pct <= 100 {
				return KnobState{Pct: v.Pct, Source: "db"}
			}
		}
	}
	return KnobState{Pct: 100, Source: "default"}
}

// SetKnob persists and propagates the knob position to every traffic gen.
func (s *Server) SetKnob(ctx context.Context, pct float64) (float64, error) {
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	if s.store != nil {
		b, _ := json.Marshal(map[string]any{"pct": pct})
		s.store.SetUIState(knobKey, json.RawMessage(b))
	}
	n := s.applyThrottle(ctx, pct)
	log.Printf("webui: knob set to %.1f%% (%d/tg endpoints applied)", pct, n)
	return pct, nil
}

// applyThrottle POSTs /api/throttle to every traffic generator.
func (s *Server) applyThrottle(ctx context.Context, pct float64) int {
	body := fmt.Sprintf(`{"throttle":%g}`, pct)
	n := 0
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, c := range s.tgs {
		c := c
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/api/throttle", strings.NewReader(body))
			if err != nil {
				return
			}
			resp, err := s.httpClient.Do(req)
			if err != nil {
				log.Printf("webui: %s throttle: %v", c.Name, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				mu.Lock()
				n++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return n
}
