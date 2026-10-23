-- 002_rfc2544.sql — persisted RFC 2544 benchmark measurements.
--
-- One row per (run, test procedure, frame size, offered rate) measurement
-- point as reported by a traffic generator. The webui polls generator runs
-- and enqueues each unseen (run, test, size, rate) row only, so re-polling
-- is idempotent and a re-run cannot duplicate history.

CREATE TABLE IF NOT EXISTS rfc2544_results (
  run_id       text             NOT NULL,      -- generator run id (r<ns>)
  test         text             NOT NULL,      -- loss | latency | throughput | back2back
  frame_size   integer          NOT NULL,      -- full G-PDU octets (64..65000)
  offered_pps  double precision NOT NULL,      -- offered frames/s (0 for b2b bursts)

  tg_pod       text             NOT NULL,      -- reporting generator pod
  ts           timestamptz      NOT NULL,      -- measurement completion time

  offered_bps  double precision NOT NULL DEFAULT 0, -- offered bits/s over the wire
  sent         bigint           NOT NULL DEFAULT 0,
  received     bigint           NOT NULL DEFAULT 0,
  loss_pct     double precision NOT NULL DEFAULT 0,

  lat_min_us   double precision NOT NULL DEFAULT 0, -- one-way latency, microseconds
  lat_avg_us   double precision NOT NULL DEFAULT 0,
  lat_max_us   double precision NOT NULL DEFAULT 0,
  lat_p50_us   double precision NOT NULL DEFAULT 0,
  lat_p95_us   double precision NOT NULL DEFAULT 0,
  lat_p99_us   double precision NOT NULL DEFAULT 0,

  b2b_lossless bigint           NOT NULL DEFAULT 0, -- back2back: largest lossless burst

  PRIMARY KEY (run_id, test, frame_size, offered_pps)
);

CREATE INDEX IF NOT EXISTS rfc2544_results_ts_idx ON rfc2544_results (ts DESC);
