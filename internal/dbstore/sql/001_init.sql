-- 001_init.sql — telco-upf-gen observability / usage store (PostgreSQL 14+)
--
-- Applied idempotently by internal/dbstore (Migrate) and tracked in
-- schema_migrations. The webui pod is the only writer/reader of this store;
-- the UPF data plane never touches it (reports flow UPF -> webui -> DB).

CREATE TABLE IF NOT EXISTS schema_migrations (
  name       text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);

-- One row per (pod, iface) per sample: cumulative counters as reported by the
-- pod plus the instantaneous rates and (for traffic generators) the active
-- throttle position at sample time.
CREATE TABLE IF NOT EXISTS metrics_ts (
  ts        timestamptz NOT NULL,
  pod       text        NOT NULL,
  iface     text        NOT NULL,               -- N3 | N5 | N9 | TX
  rx_pkts   bigint      NOT NULL DEFAULT 0,
  tx_pkts   bigint      NOT NULL DEFAULT 0,
  rx_bytes  bigint      NOT NULL DEFAULT 0,
  tx_bytes  bigint      NOT NULL DEFAULT 0,
  encaps    bigint      NOT NULL DEFAULT 0,
  decaps    bigint      NOT NULL DEFAULT 0,
  dropped   bigint      NOT NULL DEFAULT 0,
  n_errors  bigint      NOT NULL DEFAULT 0,
  rx_pps    double precision NOT NULL DEFAULT 0,
  tx_pps    double precision NOT NULL DEFAULT 0,
  throttle  double precision NOT NULL DEFAULT 100,
  PRIMARY KEY (pod, iface, ts)
);
CREATE INDEX IF NOT EXISTS metrics_ts_ts_idx ON metrics_ts (ts);

-- Liveness + configuration snapshot per pod (last writer wins).
CREATE TABLE IF NOT EXISTS node_heartbeat (
  pod       text PRIMARY KEY,
  role      text NOT NULL,                      -- upf | trafficgen | webui
  node_id   integer,
  summary   jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_seen timestamptz NOT NULL DEFAULT now()
);

-- Per-interval SPI/DPI classification totals. Per-packet inspection results
-- are aggregated in the webui between flushes and persisted here as deltas,
-- so the row count stays bounded by (pods x ifaces x apps x intervals).
CREATE TABLE IF NOT EXISTS dpi_summary (
  ts        timestamptz NOT NULL,
  pod       text NOT NULL,
  iface     text NOT NULL,
  direction text NOT NULL,                      -- UL | DL
  app_id    smallint NOT NULL,
  app_name  text NOT NULL,
  pkts      bigint NOT NULL DEFAULT 0,
  nbytes    bigint NOT NULL DEFAULT 0,
  dropped   bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (pod, iface, direction, app_id, ts)
);
CREATE INDEX IF NOT EXISTS dpi_summary_ts_idx ON dpi_summary (ts);

-- Policy enforcement events: one row per packet dropped or flagged by a DPI
-- policy rule. Naturally low-rate (only policy hits land here).
CREATE TABLE IF NOT EXISTS policy_events (
  id          bigserial PRIMARY KEY,
  ts          timestamptz NOT NULL DEFAULT now(),
  pod         text NOT NULL,
  iface       text NOT NULL,
  direction   text NOT NULL,
  outer_src   text,
  outer_dst   text,
  inner_src   inet,
  inner_dst   inet,
  l4_proto    smallint,
  src_port    integer,
  dst_port    integer,
  app_id      smallint,
  app_name    text,
  rule_id     text NOT NULL,
  action      text NOT NULL,                    -- drop | observe
  payload_len integer NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS policy_events_ts_idx ON policy_events (ts);
CREATE INDEX IF NOT EXISTS policy_events_rule_idx ON policy_events (rule_id, ts);

-- Lawful-interception-style feed: per-packet header metadata for selected
-- sessions (selector configured in the UPF). Bounded by selector scope.
CREATE TABLE IF NOT EXISTS li_events (
  id          bigserial PRIMARY KEY,
  ts          timestamptz NOT NULL DEFAULT now(),
  pod         text NOT NULL,
  session_id  bigint NOT NULL,
  teid        text NOT NULL,
  direction   text NOT NULL,
  inner_src   inet,
  inner_dst   inet,
  l4_proto    smallint,
  src_port    integer,
  dst_port    integer,
  app_id      smallint,
  app_name    text,
  detail      text NOT NULL DEFAULT '',
  payload_len integer NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS li_events_session_idx ON li_events (session_id, ts);
CREATE INDEX IF NOT EXISTS li_events_ts_idx ON li_events (ts);

-- Session-level Call Detail Records (billing format), upserted idempotently
-- on the natural key (a CDR is closed once but may be re-reported).
CREATE TABLE IF NOT EXISTS cdr (
  id           bigserial PRIMARY KEY,
  session_id   bigint NOT NULL,
  anchor_pod   text NOT NULL,
  node_id      smallint NOT NULL DEFAULT 0,
  ue_addr      inet,
  dn_addr      inet,
  up_pkts      bigint NOT NULL DEFAULT 0,
  down_pkts    bigint NOT NULL DEFAULT 0,
  up_bytes     bigint NOT NULL DEFAULT 0,
  down_bytes   bigint NOT NULL DEFAULT 0,
  started_at   timestamptz NOT NULL,
  ended_at     timestamptz NOT NULL,
  duration_ms  bigint NOT NULL DEFAULT 0,
  apps         jsonb NOT NULL DEFAULT '{}'::jsonb,  -- {"<app_id>": [pkts, bytes], ...}
  rating       text NOT NULL DEFAULT 'unrated',
  status       text NOT NULL DEFAULT 'closed',
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (session_id, anchor_pod, started_at)
);
CREATE INDEX IF NOT EXISTS cdr_started_idx ON cdr (started_at);

-- UI configuration state (traffic-knob position, dashboard settings). This is
-- what makes a browser refresh restore the exact previous state: the webui
-- reads these rows on load and re-applies them to the traffic generators.
CREATE TABLE IF NOT EXISTS ui_state (
  key        text PRIMARY KEY,
  value      jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
