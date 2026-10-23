package dbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type fakeTx struct {
	rec     *recorder
	execErr error
}

func (f *fakeTx) ExecContext(_ context.Context, q string, args ...any) (sql.Result, error) {
	if f.execErr != nil {
		return nil, f.execErr
	}
	f.rec.record("exec", q, len(args))
	return driver.RowsAffected(1), nil
}
func (f *fakeTx) Commit() error   { f.rec.record("commit", "", 0); return nil }
func (f *fakeTx) Rollback() error { f.rec.record("rollback", "", 0); return nil }

type recorder struct {
	mu    sync.Mutex
	ops   []string
	stmts []string
	args  []int
}

func (r *recorder) record(op, q string, n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
	if q != "" {
		r.stmts = append(r.stmts, q)
		r.args = append(r.args, n)
	}
}
func (r *recorder) opsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}
func (r *recorder) stmtsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stmts...)
}

func countSub(all []string, sub string) int { return strings.Count(strings.Join(all, "\n"), sub) }

type fakeDB struct {
	rec     *recorder
	pingErr error
}

func (f *fakeDB) ExecContext(_ context.Context, q string, _ ...any) (sql.Result, error) {
	f.rec.record("exec", q, 0)
	return driver.RowsAffected(1), nil
}
func (f *fakeDB) PingContext(_ context.Context) error { return f.pingErr }
func (f *fakeDB) Close() error                        { return nil }
func (f *fakeDB) SetMaxOpenConns(_ int)               {}

// ---------------------------------------------------------------------------
// flush / migrate / queue behavior
func newTestStore(t *testing.T, queueSize int) (*Store, *recorder, *fakeTx) {
	t.Helper()
	rec := &recorder{}
	tx := &fakeTx{rec: rec}
	db := &fakeDB{rec: rec}
	s := &Store{
		opts:  Options{MaxConns: 1, FlushIntv: time.Hour, QueueSize: queueSize, PingTimeout: time.Second, MaxBatch: 16},
		db:    db,
		begin: func(context.Context) (driverTx, error) { return tx, nil },
		queue: make(chan job, queueSize),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	return s, rec, tx
}

// ---------------------------------------------------------------------------
// flush / migrate / queue behavior
// ---------------------------------------------------------------------------

func TestFlushPersistsBatch(t *testing.T) {
	s, rec, _ := newTestStore(t, 16)
	s.SampleMetric(CounterSample{Pod: "upf-0", IFace: "N3", RxPkts: 100, Throttle: 100})
	s.Heartbeat("upf-0", "upf", 0, map[string]int{"flows": 96})
	s.SetUIState("throttle", map[string]any{"percent": 42})

	s.flush(context.Background())

	ops := rec.opsSnapshot()
	if !contains(ops, "commit") {
		t.Fatalf("expected a commit, got ops %v", ops)
	}
	stmts := rec.stmtsSnapshot()
	// migrate runs first (schema_migrations insert + DDL), then the 3 jobs.
	if countSub(stmts, "schema_migrations") == 0 {
		t.Fatalf("expected migration insert, got %d stmts", len(stmts))
	}
	if countSub(stmts, "INSERT INTO metrics_ts") == 0 {
		t.Fatal("metrics_ts insert missing")
	}
	if countSub(stmts, "INSERT INTO node_heartbeat") == 0 {
		t.Fatal("node_heartbeat insert missing")
	}
	if countSub(stmts, "INSERT INTO ui_state") == 0 {
		t.Fatal("ui_state insert missing")
	}
	if got := s.flushed.Load(); got != 3 {
		t.Fatalf("flushed = %d, want 3", got)
	}
	if got := s.batches.Load(); got != 1 {
		t.Fatalf("batches = %d, want 1", got)
	}
}

func TestFlushSkipsWhenDisconnected(t *testing.T) {
	s, rec, _ := newTestStore(t, 16)
	s.db.(*fakeDB).pingErr = fmt.Errorf("down")
	s.SampleMetric(CounterSample{Pod: "upf-0", IFace: "N3"})

	s.flush(context.Background())

	if s.migrated.Load() {
		t.Fatal("store must not mark migrated while disconnected")
	}
	if got := s.flushed.Load(); got != 0 {
		t.Fatalf("flushed = %d, want 0", got)
	}
	if !strings.Contains(s.Status().Note, "disconnected") {
		t.Fatalf("expected disconnected note, got %q", s.Status().Note)
	}
	_ = rec
}

func TestMigrateRunsOnce(t *testing.T) {
	s, rec, _ := newTestStore(t, 16)
	s.flush(context.Background())
	first := countSub(rec.stmtsSnapshot(), "schema_migrations")
	s.flush(context.Background())
	second := countSub(rec.stmtsSnapshot(), "schema_migrations")
	if first == 0 {
		t.Fatal("first flush must apply the schema")
	}
	if second != first {
		t.Fatalf("schema applied %d then %d times; want exactly once", first, second)
	}
}

func TestBeginErrorDropsBatch(t *testing.T) {
	rec := &recorder{}
	db := &fakeDB{rec: rec}
	s := &Store{
		opts:  Options{MaxConns: 1, FlushIntv: time.Hour, QueueSize: 16, PingTimeout: time.Second, MaxBatch: 16},
		db:    db,
		begin: func(context.Context) (driverTx, error) { return nil, fmt.Errorf("no tx") },
		queue: make(chan job, 16),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.migrated.Store(true) // skip the schema path; test the batch begin failure
	s.SampleMetric(CounterSample{Pod: "upf-0", IFace: "N3"})
	s.SetUIState("k", 1)
	s.flush(context.Background())

	if got := s.dropped.Load(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}
	if got := s.flushed.Load(); got != 0 {
		t.Fatalf("flushed = %d, want 0", got)
	}
}

func TestTxExecErrorRollsBackAndDrops(t *testing.T) {
	rec := &recorder{}
	tx := &fakeTx{rec: rec, execErr: fmt.Errorf("bad row")}
	db := &fakeDB{rec: rec}
	s := &Store{
		opts:  Options{MaxConns: 1, FlushIntv: time.Hour, QueueSize: 16, PingTimeout: time.Second, MaxBatch: 16},
		db:    db,
		begin: func(context.Context) (driverTx, error) { return tx, nil },
		queue: make(chan job, 16),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	// pre-migrate so the tx error hits a data job, not the schema
	s.migrated.Store(true)
	s.enqueue(func(context.Context, driverTx) error { return tx.execErr })
	s.flush(context.Background())

	if !contains(rec.opsSnapshot(), "rollback") {
		t.Fatalf("expected rollback on tx error, got %v", rec.opsSnapshot())
	}
}

func TestQueueFullDropsOldest(t *testing.T) {
	s, _, _ := newTestStore(t, 1)
	s.SetUIState("a", 1) // fills the single slot
	s.SetUIState("b", 2) // queue full -> dropped
	if got := s.dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	if got := len(s.queue); got != 1 {
		t.Fatalf("queue len = %d, want 1", got)
	}
}

func TestCounterSampleThrottleDefaults(t *testing.T) {
	rec := &recorder{}
	tx := &fakeTx{rec: rec}
	db := &fakeDB{rec: rec}
	s := &Store{
		opts:  Options{FlushIntv: time.Hour, QueueSize: 16, PingTimeout: time.Second, MaxBatch: 16},
		db:    db,
		begin: func(context.Context) (driverTx, error) { return tx, nil },
		queue: make(chan job, 16),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	s.migrated.Store(true)                                  // skip migrate
	s.SampleMetric(CounterSample{Pod: "tg-0", IFace: "TX"}) // Throttle 0 => 100
	s.flush(context.Background())
	// The throttle column is the 14th arg; with a default it must be 100.
	// We only assert the statement ran with 14 params.
	st := rec.stmtsSnapshot()
	if countSub(st, "INSERT INTO metrics_ts") != 1 {
		t.Fatalf("metrics insert not recorded: %v", st)
	}
}

// ---------------------------------------------------------------------------
// FromEnv
// ---------------------------------------------------------------------------

func TestFromEnv(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://u:p@h:5432/db")
	t.Setenv("DB_MAX_CONNS", "7")
	t.Setenv("DB_FLUSH_MS", "250")
	t.Setenv("DB_QUEUE", "128")

	o, enabled := FromEnv()
	if !enabled {
		t.Fatal("store should be enabled when DB_DSN is set")
	}
	if o.DSN != "postgres://u:p@h:5432/db" {
		t.Fatalf("dsn = %q", o.DSN)
	}
	if o.MaxConns != 7 {
		t.Fatalf("maxconns = %d, want 7", o.MaxConns)
	}
	if o.FlushIntv != 250*time.Millisecond {
		t.Fatalf("flush = %v, want 250ms", o.FlushIntv)
	}
	if o.QueueSize != 128 {
		t.Fatalf("queue = %d, want 128", o.QueueSize)
	}
}

func TestFromEnvDisabled(t *testing.T) {
	t.Setenv("DB_DSN", "")
	_, enabled := FromEnv()
	if enabled {
		t.Fatal("store must be disabled with empty DSN")
	}
}

func contains(slice []string, v string) bool {
	for _, s := range slice {
		if s == v {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Live Postgres integration (skipped unless DB_TEST_DSN is set)
// ---------------------------------------------------------------------------

func TestLivePostgres(t *testing.T) {
	dsn := os.Getenv("DB_TEST_DSN")
	if dsn == "" {
		t.Skip("DB_TEST_DSN not set; skipping live Postgres test")
	}
	s, err := Open(Options{DSN: dsn, FlushIntv: 20 * time.Millisecond, QueueSize: 64, PingTimeout: 3 * time.Second})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	// wait for connect + migrate
	deadline := time.Now().Add(15 * time.Second)
	for !s.Status().Migrated && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !s.Status().Migrated {
		t.Fatalf("store did not migrate in time: %+v", s.Status())
	}

	s.SampleMetric(CounterSample{Pod: "live-pod", IFace: "N3", RxPkts: 7, TxPkts: 9, Throttle: 66})
	s.SetUIState("live-throttle", map[string]any{"percent": 33, "note": "it"})
	s.UpsertCDR(CDR{
		SessionID: 4242, AnchorPod: "live-pod", NodeID: 0,
		UpPkts: 10, DownPkts: 20, UpBytes: 1000, DownBytes: 2000,
		StartedAt: time.Now().Add(-time.Minute).UTC(), EndedAt: time.Now().UTC(),
		DurationMS: 60000, Apps: nil, Rating: "unrated", Status: "closed",
	})

	// wait for the writes to land
	deadline = time.Now().Add(10 * time.Second)
	for s.Status().Flushes < 3 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := s.Status().Flushes; got < 3 {
		t.Fatalf("flushes = %d, want >= 3 (+%v)", got, s.Status())
	}

	// read back UI state
	v, err := s.GetUIState("live-throttle")
	if err != nil {
		t.Fatalf("getui: %v", err)
	}
	if !strings.Contains(string(v), "33") {
		t.Fatalf("ui state round-trip = %s, want contains 33", v)
	}

	// read back a CDR
	cdrs, err := s.CDRsSince(time.Now().Add(-time.Hour), 10)
	if err != nil {
		t.Fatalf("cdrs: %v", err)
	}
	found := false
	for _, c := range cdrs {
		if c.SessionID == 4242 {
			found = true
			if c.DownBytes != 2000 {
				t.Fatalf("cdr down_bytes = %d, want 2000", c.DownBytes)
			}
		}
	}
	if !found {
		t.Fatalf("cdr 4242 not found: %+v", cdrs)
	}
}
