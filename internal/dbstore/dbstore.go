// Package dbstore is the persistent observability/usage store for the UPF
// fabric. It owns the PostgreSQL connection used to persist traffic metrics,
// SPI/DPI results, CDR/LI usage records and UI configuration state.
//
// Design rules:
//   - The UPF data plane never touches this package. UPF pods expose report
//     endpoints over HTTP; the webui aggregates and persists them here, so
//     database I/O can never stall the packet path.
//   - Everything is configuration via environment (see FromEnv). With no
//     database configured the store is disabled and the fabric keeps running.
//   - Writes go through a bounded in-memory queue flushed in batches inside a
//     single transaction. Transient database outages buffer up to QueueSize
//     jobs; the oldest are dropped once the queue is full (monotonic counters
//     self-heal on the next sample).
//   - The schema (sql/*.sql, applied in name order) is embedded and executed
//     idempotently on first connect; no separate migration tooling or init
//     container.
package dbstore

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/lib/pq" // postgres driver for database/sql
)

//go:embed sql
var sqlFS embed.FS

// driverDB / driverTx are the narrow slices of database/sql the store needs.
// *sql.DB and *sql.Tx satisfy them; fakes in tests do too.
type driverDB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	PingContext(ctx context.Context) error
	Close() error
	SetMaxOpenConns(n int)
}

type driverTx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Commit() error
	Rollback() error
}

// queryer is implemented by *sql.DB (and test fakes that want to serve reads).
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Options configures a Store. Zero values take defaults in Open.
type Options struct {
	DSN         string        // postgres://user:pass@host:port/db?sslmode=...
	MaxConns    int32         // default 4
	FlushIntv   time.Duration // default 5s
	QueueSize   int           // default 4096
	PingTimeout time.Duration // default 2s
	MaxBatch    int           // default 256 jobs per flushed transaction
}

func (o *Options) setDefaults() {
	if o.MaxConns == 0 {
		o.MaxConns = 4
	}
	if o.FlushIntv <= 0 {
		o.FlushIntv = 5 * time.Second
	}
	if o.QueueSize <= 0 {
		o.QueueSize = 4096
	}
	if o.PingTimeout <= 0 {
		o.PingTimeout = 2 * time.Second
	}
	if o.MaxBatch <= 0 {
		o.MaxBatch = 256
	}
}

// FromEnv reads the persistence configuration from the environment:
//
//	DB_DSN       postgres DSN (empty => store disabled)
//	DB_MAX_CONNS default 4
//	DB_FLUSH_MS  flush interval in ms, default 5000
//	DB_QUEUE     max buffered write jobs, default 4096
func FromEnv() (Options, bool) {
	var o Options
	o.DSN = os.Getenv("DB_DSN")
	if v := os.Getenv("DB_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			o.MaxConns = int32(n)
		}
	}
	if v := os.Getenv("DB_FLUSH_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			o.FlushIntv = time.Duration(n) * time.Millisecond
		}
	}
	if v := os.Getenv("DB_QUEUE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			o.QueueSize = n
		}
	}
	o.setDefaults() // expose the effective config to callers (webui startup log)
	return o, o.DSN != ""
}

type job struct {
	at    time.Time
	apply func(ctx context.Context, tx driverTx) error
}

// Store is the persistent store. Safe for concurrent use.
type Store struct {
	opts  Options
	db    driverDB
	begin func(ctx context.Context) (driverTx, error) // seam; tests swap a fake
	queue chan job
	stop  chan struct{}
	done  chan struct{}

	mu       sync.Mutex
	lastNote string // human-readable connection state for Status/logs
	noteSeen uint64

	migrated  atomic.Bool
	queued    atomic.Int64
	flushed   atomic.Int64
	dropped   atomic.Int64
	batches   atomic.Int64
	queuedNow atomic.Int64
	lastFlush atomic.Int64 // unix millis
}

// Open builds a Store and starts its flush loop. No connection is made until
// the first flush tick; a database that comes up later is adopted
// automatically.
func Open(opts Options) (*Store, error) {
	if opts.DSN == "" {
		return nil, errors.New("dbstore: empty DSN")
	}
	opts.setDefaults()
	db, err := sql.Open("postgres", opts.DSN)
	if err != nil {
		return nil, fmt.Errorf("dbstore: open: %w", err)
	}
	db.SetMaxOpenConns(int(opts.MaxConns))
	s := &Store{
		opts:  opts,
		db:    db,
		begin: func(ctx context.Context) (driverTx, error) { return db.BeginTx(ctx, nil) },
		queue: make(chan job, opts.QueueSize),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go s.loop()
	return s, nil
}

// Close stops the flush loop after one final flush attempt, then the pool.
func (s *Store) Close() {
	close(s.stop)
	<-s.done
	_ = s.db.Close()
}

// Status reports store health for logs and the /api/db endpoint.
type Status struct {
	Enabled   bool   `json:"enabled"`
	Connected bool   `json:"connected"`
	Migrated  bool   `json:"migrated"`
	Queued    int64  `json:"queued"`
	InQueue   int    `json:"inQueue"`
	Flushes   int64  `json:"flushes"`
	Dropped   int64  `json:"dropped"`
	Batches   int64  `json:"batches"`
	LastFlush string `json:"lastFlush,omitempty"`
	Note      string `json:"note"`
}

func (s *Store) Status() Status {
	st := Status{
		Enabled:   true,
		Connected: s.migrated.Load(),
		Migrated:  s.migrated.Load(),
		Queued:    s.queued.Load(),
		InQueue:   len(s.queue),
		Flushes:   s.flushed.Load(),
		Dropped:   s.dropped.Load(),
		Batches:   s.batches.Load(),
	}
	if ms := s.lastFlush.Load(); ms > 0 {
		st.LastFlush = time.UnixMilli(ms).UTC().Format(time.RFC3339)
	}
	st.Note = s.lastNoteLocked()
	return st
}

func (s *Store) lastNoteLocked() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastNote
}

func (s *Store) setNote(format string, args ...any) {
	n := fmt.Sprintf(format, args...)
	s.mu.Lock()
	changed := n != s.lastNote
	s.lastNote = n
	s.mu.Unlock()
	if changed {
		log.Printf("dbstore: %s", n)
	}
}

// loop drives periodic flushing until Close.
func (s *Store) loop() {
	defer close(s.done)
	t := time.NewTicker(s.opts.FlushIntv)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.flush(context.Background())
		case <-s.stop:
			ctx, cancel := context.WithTimeout(context.Background(), s.opts.PingTimeout*3)
			s.flush(ctx) // final drain; whatever remains stays queued (bounded)
			cancel()
			return
		}
	}
}

// drainBurstBatches bounds how many batches one flush tick may commit, so
// a burst of reports (per-packet LI events, poll backlogs) cannot starve the
// 5s tick: one tick can move up to 16 * MaxBatch jobs.
const drainBurstBatches = 16

// flush pings, migrates once, then burst-drains the queue in
// transaction-sized batches.
func (s *Store) flush(ctx context.Context) {
	pingCtx, cancel := context.WithTimeout(ctx, s.opts.PingTimeout)
	defer cancel()

	if err := s.db.PingContext(pingCtx); err != nil {
		s.setNote("disconnected (%v); retrying next cycle", err)
		return
	}

	if !s.migrated.Load() {
		if err := s.migrate(pingCtx); err != nil {
			s.setNote("migrate failed: %v", err)
			return
		}
		s.migrated.Store(true)
		s.setNote("connected (%s)", time.Now().Format(time.RFC3339))
	}

	batchCtx, bcancel := context.WithTimeout(ctx, s.opts.PingTimeout*4)
	defer bcancel()
	for n := 0; n < drainBurstBatches; n++ {
		var jobs []job
	drain:
		for len(jobs) < s.opts.MaxBatch {
			select {
			case j := <-s.queue:
				jobs = append(jobs, j)
			default:
				break drain
			}
		}
		if len(jobs) == 0 {
			return
		}

		tx, err := s.begin(batchCtx)
		if err != nil {
			s.dropped.Add(int64(len(jobs)))
			s.setNote("begin failed: %v (batch of %d dropped)", err, len(jobs))
			return
		}
		for i, j := range jobs {
			if err := j.apply(batchCtx, tx); err != nil {
				_ = tx.Rollback()
				s.dropped.Add(int64(len(jobs)))
				s.setNote("tx failed at job %d/%d: %v", i+1, len(jobs), err)
				return
			}
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			s.dropped.Add(int64(len(jobs)))
			s.setNote("commit failed: %v (batch of %d dropped)", err, len(jobs))
			return
		}
		s.flushed.Add(int64(len(jobs)))
		s.batches.Add(1)
		s.queuedNow.Add(-int64(len(jobs)))
		s.lastFlush.Store(time.Now().UnixMilli())
	}
}

// migrate applies the embedded DDL (sql/*.sql in name order, each registered
// in schema_migrations) once per connection lifetime.
func (s *Store) migrate(ctx context.Context) error {
	entries, err := sqlFS.ReadDir("sql")
	if err != nil {
		return fmt.Errorf("dbstore: read schema: %w", err)
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	applied := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		raw, err := sqlFS.ReadFile("sql/" + name)
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("dbstore: read schema %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, string(raw)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("dbstore: apply schema %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT DO NOTHING`, name); err != nil {
			_ = tx.Rollback()
			return err
		}
		applied = append(applied, name)
	}
	if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("dbstore: commit schema: %w", err)
	}
	s.setNote("schema applied: %s", strings.Join(applied, ", "))
	return nil
}

// enqueue records a write job; when the queue is full the job is dropped and
// accounted in Dropped.
func (s *Store) enqueue(apply func(ctx context.Context, tx driverTx) error) {
	s.queued.Add(1)
	s.queuedNow.Add(1)
	select {
	case s.queue <- job{at: time.Now(), apply: apply}:
	default:
		s.dropped.Add(1)
		s.queuedNow.Add(-1)
	}
}

// ---------------------------------------------------------------------------
// write API (each method = one buffered job)
// ---------------------------------------------------------------------------

// CounterSample is one pod/interface sample as reported over /metrics.
type CounterSample struct {
	Pod      string  `json:"pod"`
	IFace    string  `json:"iface"`
	RxPkts   uint64  `json:"rxPkts"`
	TxPkts   uint64  `json:"txPkts"`
	RxBytes  uint64  `json:"rxBytes"`
	TxBytes  uint64  `json:"txBytes"`
	Encaps   uint64  `json:"encaps"`
	Decaps   uint64  `json:"decaps"`
	Dropped  uint64  `json:"dropped"`
	NErrors  uint64  `json:"nErrors"`
	RxPps    float64 `json:"rxPps"`
	TxPps    float64 `json:"txPps"`
	Throttle float64 `json:"throttle"` // 0-100; 0 (unset) stored as 100
}

const sampleSQL = `INSERT INTO metrics_ts
	(ts, pod, iface, rx_pkts, tx_pkts, rx_bytes, tx_bytes,
	 encaps, decaps, dropped, n_errors, rx_pps, tx_pps, throttle)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	ON CONFLICT (pod, iface, ts) DO UPDATE SET
		rx_pkts=EXCLUDED.rx_pkts, tx_pkts=EXCLUDED.tx_pkts,
		rx_bytes=EXCLUDED.rx_bytes, tx_bytes=EXCLUDED.tx_bytes,
		encaps=EXCLUDED.encaps, decaps=EXCLUDED.decaps,
		dropped=EXCLUDED.dropped, n_errors=EXCLUDED.n_errors,
		rx_pps=EXCLUDED.rx_pps, tx_pps=EXCLUDED.tx_pps, throttle=EXCLUDED.throttle`

// SampleMetric buffers one metrics sample.
func (s *Store) SampleMetric(c CounterSample) {
	if c.Throttle == 0 {
		c.Throttle = 100
	}
	ts := time.Now().UTC()
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, sampleSQL,
			ts, c.Pod, c.IFace,
			c.RxPkts, c.TxPkts, c.RxBytes, c.TxBytes,
			c.Encaps, c.Decaps, c.Dropped, c.NErrors,
			c.RxPps, c.TxPps, c.Throttle)
		return err
	})
}

// Heartbeat records pod liveness and a small summary blob.
func (s *Store) Heartbeat(pod, role string, nodeID int, summary any) {
	sum, err := json.Marshal(summary)
	if err != nil || sum == nil {
		sum = []byte("{}")
	}
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO node_heartbeat (pod, role, node_id, summary, last_seen)
			VALUES ($1,$2,$3,$4,now())
			ON CONFLICT (pod) DO UPDATE SET
				role=EXCLUDED.role, node_id=EXCLUDED.node_id,
				summary=EXCLUDED.summary, last_seen=EXCLUDED.last_seen`,
			pod, role, nodeID, sum)
		return err
	})
}

// DPISummary is one aggregated interval of SPI/DPI classification counts.
type DPISummary struct {
	Pod       string `json:"pod"`
	IFace     string `json:"iface"`
	Direction string `json:"direction"` // UL | DL
	AppID     int16  `json:"appId"`
	AppName   string `json:"appName"`
	Pkts      uint64 `json:"pkts"`
	NBytes    uint64 `json:"nbytes"`
	Dropped   uint64 `json:"dropped"`
}

// DPISample buffers one DPI aggregation row.
func (s *Store) DPISample(d DPISummary) {
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO dpi_summary
			(ts, pod, iface, direction, app_id, app_name, pkts, nbytes, dropped)
			VALUES (now(),$1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (pod, iface, direction, app_id, ts) DO UPDATE SET
				pkts=EXCLUDED.pkts, nbytes=EXCLUDED.nbytes,
				dropped=EXCLUDED.dropped, app_name=EXCLUDED.app_name`,
			d.Pod, d.IFace, d.Direction, d.AppID, d.AppName,
			d.Pkts, d.NBytes, d.Dropped)
		return err
	})
}

// PolicyEvent is one policy enforcement occurrence (drop/observe).
type PolicyEvent struct {
	Pod        string `json:"pod"`
	IFace      string `json:"iface"`
	Direction  string `json:"direction"`
	OuterSrc   string `json:"outerSrc,omitempty"`
	OuterDst   string `json:"outerDst,omitempty"`
	InnerSrc   string `json:"innerSrc,omitempty"` // inet literal; "" => NULL
	InnerDst   string `json:"innerDst,omitempty"`
	L4Proto    *int16 `json:"l4Proto,omitempty"`
	SrcPort    *int32 `json:"srcPort,omitempty"`
	DstPort    *int32 `json:"dstPort,omitempty"`
	AppID      *int16 `json:"appId,omitempty"`
	AppName    string `json:"appName,omitempty"`
	RuleID     string `json:"ruleId"`
	Action     string `json:"action"` // drop | observe
	PayloadLen int32  `json:"payloadLen"`
}

// PolicySample buffers one policy event.
func (s *Store) PolicySample(p PolicyEvent) {
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO policy_events
			(pod, iface, direction, outer_src, outer_dst, inner_src, inner_dst,
			 l4_proto, src_port, dst_port, app_id, app_name, rule_id, action, payload_len)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			p.Pod, p.IFace, p.Direction, nilOrNull(p.OuterSrc), nilOrNull(p.OuterDst),
			nilInet(p.InnerSrc), nilInet(p.InnerDst), p.L4Proto, p.SrcPort, p.DstPort,
			p.AppID, nilOrNull(p.AppName), p.RuleID, p.Action, p.PayloadLen)
		return err
	})
}

// LIEvent is one lawful-interception-style record for a selected session.
type LIEvent struct {
	Pod        string `json:"pod"`
	SessionID  uint64 `json:"sessionId"`
	TEID       string `json:"teid"`
	Direction  string `json:"direction"`
	InnerSrc   string `json:"innerSrc,omitempty"`
	InnerDst   string `json:"innerDst,omitempty"`
	L4Proto    *int16 `json:"l4Proto,omitempty"`
	SrcPort    *int32 `json:"srcPort,omitempty"`
	DstPort    *int32 `json:"dstPort,omitempty"`
	AppID      *int16 `json:"appId,omitempty"`
	AppName    string `json:"appName,omitempty"`
	Detail     string `json:"detail,omitempty"` // SNI / host / other
	PayloadLen int32  `json:"payloadLen"`
}

// LISample buffers one LI record.
func (s *Store) LISample(e LIEvent) {
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO li_events
			(pod, session_id, teid, direction, inner_src, inner_dst,
			 l4_proto, src_port, dst_port, app_id, app_name, detail, payload_len)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			e.Pod, e.SessionID, e.TEID, e.Direction,
			nilInet(e.InnerSrc), nilInet(e.InnerDst), e.L4Proto, e.SrcPort,
			e.DstPort, e.AppID, nilOrNull(e.AppName), e.Detail, e.PayloadLen)
		return err
	})
}

// CDR is a closed session billing record.
type CDR struct {
	SessionID  uint64          `json:"sessionId"`
	AnchorPod  string          `json:"anchorPod"`
	NodeID     int16           `json:"nodeId"`
	UeAddr     string          `json:"ueAddr,omitempty"`
	DnAddr     string          `json:"dnAddr,omitempty"`
	UpPkts     uint64          `json:"upPkts"`
	DownPkts   uint64          `json:"downPkts"`
	UpBytes    uint64          `json:"upBytes"`
	DownBytes  uint64          `json:"downBytes"`
	StartedAt  time.Time       `json:"startedAt"`
	EndedAt    time.Time       `json:"endedAt"`
	DurationMS int64           `json:"durationMs"`
	Apps       json.RawMessage `json:"apps"` // {"<app_id>":[pkts,bytes],...}
	Rating     string          `json:"rating"`
	Status     string          `json:"status"`
}

const cdrSQL = `INSERT INTO cdr
	(session_id, anchor_pod, node_id, ue_addr, dn_addr,
	 up_pkts, down_pkts, up_bytes, down_bytes,
	 started_at, ended_at, duration_ms, apps, rating, status)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
	ON CONFLICT (session_id, anchor_pod, started_at) DO UPDATE SET
		dn_addr=EXCLUDED.dn_addr,
		up_pkts=EXCLUDED.up_pkts, down_pkts=EXCLUDED.down_pkts,
		up_bytes=EXCLUDED.up_bytes, down_bytes=EXCLUDED.down_bytes,
		ended_at=EXCLUDED.ended_at, duration_ms=EXCLUDED.duration_ms,
		apps=EXCLUDED.apps, rating=EXCLUDED.rating, status=EXCLUDED.status`

// UpsertCDR buffers one (re-)report of a closed CDR.
func (s *Store) UpsertCDR(c CDR) {
	apps := c.Apps
	if len(apps) == 0 {
		apps = json.RawMessage("{}")
	}
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, cdrSQL,
			c.SessionID, c.AnchorPod, c.NodeID, nilInet(c.UeAddr), nilInet(c.DnAddr),
			c.UpPkts, c.DownPkts, c.UpBytes, c.DownBytes,
			c.StartedAt.UTC(), c.EndedAt.UTC(), c.DurationMS, apps,
			c.Rating, c.Status)
		return err
	})
}

// RFC2544Result is one RFC 2544 measurement point as reported by a traffic
// generator (field names match the generator's /api/rfc2544 JSON).
type RFC2544Result struct {
	RunID       string  `json:"runId"`
	Ts          string  `json:"ts"` // RFC3339; unparseable stamps fall back to write time
	Test        string  `json:"test"`
	FrameSize   int     `json:"frameSize"`
	OfferedPPS  float64 `json:"offeredPps"`
	OfferedBPS  float64 `json:"offeredBps"`
	Sent        uint64  `json:"sent"`
	Received    uint64  `json:"received"`
	LossPct     float64 `json:"lossPct"`
	LatMinUS    float64 `json:"latMinUs"`
	LatAvgUS    float64 `json:"latAvgUs"`
	LatMaxUS    float64 `json:"latMaxUs"`
	LatP50US    float64 `json:"latP50Us"`
	LatP95US    float64 `json:"latP95Us"`
	LatP99US    float64 `json:"latP99Us"`
	B2BLossless int     `json:"b2bLossless"`
}

const rfc2544SQL = `INSERT INTO rfc2544_results
	(run_id, test, frame_size, offered_pps, tg_pod, ts,
	 offered_bps, sent, received, loss_pct,
	 lat_min_us, lat_avg_us, lat_max_us, lat_p50_us, lat_p95_us, lat_p99_us,
	 b2b_lossless)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
	ON CONFLICT (run_id, test, frame_size, offered_pps) DO NOTHING`

// UpsertRFC2544 buffers the measurement points of one generator run against
// tgPod. Per-point idempotency comes from the table's primary key, so a
// re-upsert of the same run changes nothing.
func (s *Store) UpsertRFC2544(tgPod string, results []RFC2544Result) {
	for _, r := range results {
		res := r // capture per job
		ts, err := time.Parse(time.RFC3339Nano, res.Ts)
		if err != nil {
			ts = time.Now().UTC()
		}
		s.enqueue(func(ctx context.Context, tx driverTx) error {
			_, err := tx.ExecContext(ctx, rfc2544SQL,
				res.RunID, res.Test, res.FrameSize, res.OfferedPPS, tgPod, ts.UTC(),
				res.OfferedBPS, res.Sent, res.Received, res.LossPct,
				res.LatMinUS, res.LatAvgUS, res.LatMaxUS, res.LatP50US,
				res.LatP95US, res.LatP99US, res.B2BLossless)
			return err
		})
	}
}

// SetUIState persists a UI configuration value (e.g. the traffic knob).
func (s *Store) SetUIState(key string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		s.dropped.Add(1)
		s.setNote("marshal ui_state %q: %v", key, err)
		return
	}
	s.enqueue(func(ctx context.Context, tx driverTx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO ui_state (key, value, updated_at)
			VALUES ($1,$2,now())
			ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at`,
			key, b)
		return err
	})
}

func nilOrNull(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilInet(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ---------------------------------------------------------------------------
// read API (blocking; used by the dashboard, not the packet path)
// ---------------------------------------------------------------------------

// GetUIState returns the stored JSON for one UI state key.
func (s *Store) GetUIState(key string) (json.RawMessage, error) {
	rows, done, err := s.query(`SELECT value FROM ui_state WHERE key = $1`, key)
	if err != nil {
		return nil, err
	}
	defer done()
	if !rows.Next() {
		return nil, nil
	}
	var b []byte
	if err := rows.Scan(&b); err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// UIState returns all stored UI state pairs.
func (s *Store) UIState() (map[string]json.RawMessage, error) {
	rows, done, err := s.query(`SELECT key, value FROM ui_state ORDER BY key`)
	if err != nil {
		return nil, err
	}
	defer done()
	out := make(map[string]json.RawMessage)
	for rows.Next() {
		var k string
		var b []byte
		if err := rows.Scan(&k, &b); err != nil {
			return nil, err
		}
		out[k] = json.RawMessage(b)
	}
	return out, rows.Err()
}

// CDRsSince lists closed billing records, newest first.
func (s *Store) CDRsSince(since time.Time, limit int) ([]CDR, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	rows, done, err := s.query(`SELECT session_id, anchor_pod, node_id,
		COALESCE(ue_addr::text,''), COALESCE(dn_addr::text,''),
		up_pkts, down_pkts, up_bytes, down_bytes,
		started_at, ended_at, duration_ms, apps, rating, status
		FROM cdr WHERE started_at >= $1
		ORDER BY started_at DESC LIMIT $2`, since.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer done()
	var out []CDR
	for rows.Next() {
		var c CDR
		var apps []byte
		if err := rows.Scan(&c.SessionID, &c.AnchorPod, &c.NodeID,
			&c.UeAddr, &c.DnAddr, &c.UpPkts, &c.DownPkts, &c.UpBytes,
			&c.DownBytes, &c.StartedAt, &c.EndedAt, &c.DurationMS, &apps,
			&c.Rating, &c.Status); err != nil {
			return nil, err
		}
		c.Apps = json.RawMessage(apps)
		out = append(out, c)
	}
	return out, rows.Err()
}

// RFC2544Row is one stored RFC 2544 measurement point.
type RFC2544Row struct {
	RunID       string    `json:"runId"`
	Ts          time.Time `json:"ts"`
	Test        string    `json:"test"`
	FrameSize   int       `json:"frameSize"`
	OfferedPPS  float64   `json:"offeredPps"`
	TgPod       string    `json:"tgPod"`
	OfferedBPS  float64   `json:"offeredBps"`
	Sent        uint64    `json:"sent"`
	Received    uint64    `json:"received"`
	LossPct     float64   `json:"lossPct"`
	LatMinUS    float64   `json:"latMinUs"`
	LatAvgUS    float64   `json:"latAvgUs"`
	LatMaxUS    float64   `json:"latMaxUs"`
	LatP50US    float64   `json:"latP50Us"`
	LatP95US    float64   `json:"latP95Us"`
	LatP99US    float64   `json:"latP99Us"`
	B2BLossless int       `json:"b2bLossless"`
}

// RFC2544Since lists stored benchmark measurement points since ts, oldest
// first, capped.
func (s *Store) RFC2544Since(since time.Time, limit int) ([]RFC2544Row, error) {
	if limit <= 0 || limit > 20000 {
		limit = 5000
	}
	rows, done, err := s.query(`SELECT run_id, ts, test, frame_size, offered_pps, tg_pod,
		offered_bps, sent, received, loss_pct,
		lat_min_us, lat_avg_us, lat_max_us, lat_p50_us, lat_p95_us, lat_p99_us,
		b2b_lossless
		FROM rfc2544_results WHERE ts >= $1
		ORDER BY ts ASC, run_id, test, frame_size, offered_pps LIMIT $2`,
		since.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer done()
	var out []RFC2544Row
	for rows.Next() {
		var r RFC2544Row
		if err := rows.Scan(&r.RunID, &r.Ts, &r.Test, &r.FrameSize, &r.OfferedPPS,
			&r.TgPod, &r.OfferedBPS, &r.Sent, &r.Received, &r.LossPct,
			&r.LatMinUS, &r.LatAvgUS, &r.LatMaxUS, &r.LatP50US,
			&r.LatP95US, &r.LatP99US, &r.B2BLossless); err != nil {
			return nil, err
		}
		r.Ts = r.Ts.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// RFC2544RunRows returns how many measurement points are already stored for
// one generator run id.
func (s *Store) RFC2544RunRows(runID string) (int, error) {
	rows, done, err := s.query(`SELECT count(*) FROM rfc2544_results WHERE run_id = $1`, runID)
	if err != nil {
		return 0, err
	}
	defer done()
	if !rows.Next() {
		return 0, errors.New("dbstore: count query returned no row")
	}
	var n int
	if err := rows.Scan(&n); err != nil {
		return 0, err
	}
	return n, rows.Err()
}

// MetricsRow is one persisted (pod, iface) sample.
type MetricsRow struct {
	Ts       time.Time `json:"ts"`
	Pod      string    `json:"pod"`
	IFace    string    `json:"iface"`
	RxPkts   uint64    `json:"rxPkts"`
	TxPkts   uint64    `json:"txPkts"`
	RxBytes  uint64    `json:"rxBytes"`
	TxBytes  uint64    `json:"txBytes"`
	Encaps   uint64    `json:"encaps"`
	Decaps   uint64    `json:"decaps"`
	Dropped  uint64    `json:"dropped"`
	NErrors  uint64    `json:"nErrors"`
	RxPps    float64   `json:"rxPps"`
	TxPps    float64   `json:"txPps"`
	Throttle float64   `json:"throttle"`
}

// MetricsSince lists stored samples since ts, oldest first, capped.
func (s *Store) MetricsSince(since time.Time, limit int) ([]MetricsRow, error) {
	if limit <= 0 || limit > 20000 {
		limit = 5000
	}
	rows, done, err := s.query(`SELECT ts, pod, iface, rx_pkts, tx_pkts, rx_bytes, tx_bytes,
		encaps, decaps, dropped, n_errors, rx_pps, tx_pps, throttle
		FROM metrics_ts WHERE ts >= $1 ORDER BY ts ASC, pod, iface LIMIT $2`,
		since.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer done()
	var out []MetricsRow
	for rows.Next() {
		var r MetricsRow
		if err := rows.Scan(&r.Ts, &r.Pod, &r.IFace, &r.RxPkts, &r.TxPkts,
			&r.RxBytes, &r.TxBytes, &r.Encaps, &r.Decaps, &r.Dropped,
			&r.NErrors, &r.RxPps, &r.TxPps, &r.Throttle); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DPISumSince lists stored SPI/DPI aggregate rows since ts, capped.
func (s *Store) DPISumSince(since time.Time, limit int) ([]DPISummary, error) {
	if limit <= 0 || limit > 20000 {
		limit = 10000
	}
	rows, done, err := s.query(`SELECT ts, pod, iface, direction, app_id, app_name, pkts, nbytes, dropped
		FROM dpi_summary WHERE ts >= $1 ORDER BY ts ASC, pod, iface, app_id LIMIT $2`,
		since.UTC(), limit)
	if err != nil {
		return nil, err
	}
	defer done()
	var out []DPISummary
	for rows.Next() {
		var ts time.Time
		var d DPISummary
		if err := rows.Scan(&ts, &d.Pod, &d.IFace, &d.Direction, &d.AppID, &d.AppName, &d.Pkts, &d.NBytes, &d.Dropped); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// PolicyEvRow is one stored policy enforcement event.
type PolicyEvRow struct {
	Ts         time.Time `json:"ts"`
	Pod        string    `json:"pod"`
	IFace      string    `json:"iface"`
	Direction  string    `json:"direction"`
	OuterSrc   string    `json:"outerSrc"`
	OuterDst   string    `json:"outerDst"`
	InnerSrc   string    `json:"innerSrc"`
	InnerDst   string    `json:"innerDst"`
	L4Proto    string    `json:"l4Proto"`
	SrcPort    string    `json:"srcPort"`
	DstPort    string    `json:"dstPort"`
	AppName    string    `json:"appName"`
	RuleID     string    `json:"ruleId"`
	Action     string    `json:"action"`
	PayloadLen int32     `json:"payloadLen"`
}

func (s *Store) policyEvRows(q string, args ...any) (any, error) {
	queryer, ok := s.db.(queryer)
	if !ok {
		return nil, errors.New("dbstore: underlying driver does not support queries")
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.PingTimeout*2)
	rows, err := queryer.QueryContext(ctx, q, args...)
	if err != nil {
		cancel()
		return nil, err
	}
	// rows must be closed before their context is canceled (lib/pq).
	defer func() { rows.Close(); cancel() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := map[string]any{}
		for i, c := range cols {
			switch v := vals[i].(type) {
			case []byte:
				m[c] = string(v)
			case time.Time:
				m[c] = v.UTC().Format(time.RFC3339)
			case nil:
				m[c] = ""
			default:
				m[c] = v
			}
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PolicyEvents lists recent policy enforcement events, newest first.
func (s *Store) PolicyEvents(limit int) (any, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	return s.policyEvRows(`SELECT ts, pod, iface, direction,
		COALESCE(outer_src,''), COALESCE(outer_dst,''),
		COALESCE(inner_src::text,''), COALESCE(inner_dst::text,''),
		COALESCE(l4_proto::text,''), COALESCE(src_port::text,''), COALESCE(dst_port::text,''),
		COALESCE(app_name,''), rule_id, action, payload_len
		FROM policy_events ORDER BY ts DESC, id DESC LIMIT $1`, limit)
}

// LIEvents lists recent lawful-interception records, newest first.
func (s *Store) LIEvents(limit int) (any, error) {
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	return s.policyEvRows(`SELECT ts, pod, '', direction, '', '',
		COALESCE(inner_src::text,''), COALESCE(inner_dst::text,''),
		COALESCE(l4_proto::text,''), COALESCE(src_port::text,''), COALESCE(dst_port::text,''),
		COALESCE(app_name,''), '', '', payload_len, session_id, teid, detail
		FROM li_events ORDER BY ts DESC, id DESC LIMIT $1`, limit)
}

// query runs q on the read pool and returns rows plus a done() cleanup.
// The context must stay alive until all rows are consumed (lib/pq aborts a
// streaming result when its context is canceled), so callers defer done()
// — which closes the rows first and only then cancels.
func (s *Store) query(q string, args ...any) (*sql.Rows, func(), error) {
	qr, ok := s.db.(queryer)
	if !ok {
		return nil, nil, errors.New("dbstore: underlying driver does not support queries")
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.PingTimeout*2)
	rows, err := qr.QueryContext(ctx, q, args...)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return rows, func() { rows.Close(); cancel() }, nil
}
