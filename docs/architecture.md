# telco-upf-gen — Architecture

A distributed 5G **User Plane Function (UPF)** simulator that runs as a set of
pods on OpenShift. It fabricates realistic **GTP-U** user-plane traffic across
the 3GPP reference points **N3** (gNB/UE direction), **N5** (data-network / DN
direction) and **N9** (inter-UPF), inspects every packet on the data plane
(SPI + DPI), produces telco-grade **usage** records (CDRs and a
lawful-interception feed), and visualizes the whole flow in a real-time Web UI
backed by PostgreSQL.

This document is the single source of truth for *how the system is shaped*.
The running code lives in `internal/` and `cmd/`; the manifests live in
`deploy/`. All diagrams below are authored in **Mermaid** so they render in
any GitHub/Markdown viewer without external image dependencies.

---

## 0. Design goals

| Goal | How it is met |
|---|---|
| **Distributed** | Each UPF node is one pod in a `StatefulSet`. Nodes form a fabric and forward to each other over N9. There is no master. |
| **Environment-agnostic** | No cluster name, IP, or path is hard-coded. Everything is YAML / ConfigMap / env var. The traffic generator can run in-cluster *or* on an external machine. |
| **Realistic wire format** | Native GTP-U (GTPv1-U, 3GPP TS 29.281 / RFC 8180) encode/decode, not a stand-in. |
| **Inspectable** | A per-packet SPI/DPI/policy/usage pipeline runs in the forwarding hot path. |
| **Billable & auditable** | Per-session CDRs and a lawful-interception event feed, persisted durably. |
| **Observable** | A webui aggregates every pod, keeps state in Postgres (survives refresh), and exposes a live "throttle" knob. |
| **Load-testable** | The traffic generator doubles as an RFC 2544 benchmarking suite (throughput / latency / loss / back-to-back). |

---

## 1. Component map

There are four deployable units, each a separate Go main and a separate
Containerfile.

| Component | Binary | Role | Scaled as |
|---|---|---|---|
| **UPF** | `cmd/upf` | The data plane. Listens for GTP-U on N3/N5/N9, resolves F-TEIDs, inspects (SPI/DPI/policy/CDR) and forwards across the fabric. | `StatefulSet` (1 pod per logical node) |
| **Traffic generator** | `cmd/trafficgen` | Emits token-bucket GTP-U flows into the fabric's ingress ports; carries realistic mock payloads for DPI. Dual-mode (in-cluster / external). Doubles as the **RFC 2544 benchmark runner** (closed-loop on the live fabric). | `Deployment` (N pods) — or a standalone binary |
| **Web UI** | `cmd/webui` | Fans out to every pod, renders the dashboard, and is the **only** process that writes to Postgres (metrics, SPI/DPI, CDRs, LI, UI state). | `Deployment` (1) |
| **Database** | PostgreSQL 16 | Durable store for metrics, inspection results, usage and UI state. | `StatefulSet` (1) + PVC |

```mermaid
flowchart LR
  subgraph CLUSTER["OpenShift cluster (namespace = config)"]
    direction TB
    subgraph FABRIC["User plane (StatefulSet upf, n nodes)"]
      U0["upf-0\nN3/N5/N9"]
      U1["upf-1\nN3/N5/N9"]
    end
    TG["trafficgen (xN)\nGTP-U source"]
    WUI["webui\naggregate + store"]
    DB[("PostgreSQL 16\n+ PVC")]
  end
  EXT["external load machine\n(bin/trafficgen)"]

  TG  -- "N3/N5/N9 GTP-U (UDP)" --> U0
  TG  -- "N3/N5/N9 GTP-U (UDP)" --> U1
  U0 <== "N9 GTP-U (inter-UPF)" ==> U1
  EXT -. "external ingress\n(NodePort / LB)" .-> U0
  WUI -- "poll /metrics,/api/*" --> U0
  WUI -- "poll /metrics,/api/*" --> U1
  WUI -- "poll /api/throttle" --> TG
  WUI == "batch upsert" ==> DB
```

The webui is deliberately the **sole DB writer**. The data plane (UPF) and the
traffic generator never open a database connection, so a database outage can
only delay *reporting* — it can never stall *data flow*.

---

## 2. The F-TEID addressing scheme

There is **no control plane** and no signalling (no PFCP, no GTP-C). Session
identity is encoded directly into the 4-octet **Fully-qualified TUNNEL Endpoint
Identifier (F-TEID)** so that any node can decode a packet's destination and
path *without* consulting a map. This is what makes the fabric stateless and
horizontally scalable.

```
F-TEID (32 bits)
┌──────────┬──────────┬───────────────────┐
│   ns     │  node    │      seq          │
│ 8 bit    │  8 bit   │       16 bit      │
└──────────┴──────────┴───────────────────┘
  ns<<24 | node<<16 | seq
```

The **namespace** byte selects the reference point (which ingress interface
the tunnel belongs to):

| Namespace | Value | Reference point | Direction |
|---|---|---|---|
| `TeidNSN3` | `0x10` | **N3** | UE / gNB → UPF (uplink) |
| `TeidNSN5` | `0x20` | **N5** | DN → UPF (downlink, from data network) |
| `TeidNSN9` | `0x40` | **N9** | peer-UPF → UPF (east-west transit) |

| Field | Bits | Meaning |
|---|---|---|
| `ns` | 24..31 | Which interface/namespace the tunnel was announced on. |
| `node` | 16..23 | The logical UPF node that *anchors* this session (0..255). |
| `seq` | 0..15 | Session sequence within a node (0..65535). |

Two worked examples (straight from `internal/upf/session_test.go`):

```
0x10 03 1234  ->  ns=N3(0x10), node=3, seq=0x1234   (Uplink to node 3)
0x40 01 002a  ->  ns=N9(0x40), node=1, seq=0x2a     (N9 transit to node 1)
```

**Consequence:** when a packet arrives whose `node` field names a *different*
UPF, the receiving node retags the tunnel into the **N9 namespace** (same
`node`/`seq`) and hands it to that peer. That is the entire east-west (E-W)
mechanism — no lookup, no state, O(1).

```mermaid
flowchart TB
  P["received G-PDU"] --> D["decode F-TEID\nns | node | seq"]
  D --> Q{"node == self?"}
  Q -- "yes (anchored here)" --> L["deliver to attached\ninterface (N5/N3 egress)"]
  Q -- "no (anchored elsewhere)" --> E["retag ns -> N9\nforward to peer 'node' over N9"]
  E --> PEER["peer UPF delivers on arrival"]
```

---

## 3. GTP-U wire format

The simulator implements the real **GTPv1-U** user-plane message (G-PDU,
3GPP TS 29.281 / RFC 8180). Codec lives in `internal/gtpu` with full
round-trip tests.

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|VER=1|T/U=1|  R |S|            |   255 (G-PDU) |  T-PDU length |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        Tunnel endpoint ID (F-TEID)           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  [optional] Seq Ext  T=4 |    SeqExtLen=4  |  E |  |  |  |seq|
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        IPv4 user datagram                     |
~                                                               ~
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Octet | Field | Notes |
|---|---|---|
| 0 | `VER=001, T/U=1` | fixed `0x30`; bit 2 (`S`) set iff a sequence extension follows |
| 1 | Message type | `0xFF` = G-PDU |
| 2–3 | T-PDU length | big-endian, length of everything after the 4th octet |
| 4–7 | **F-TEID** | the addressing scheme of §2 |
| 8–13 | Sequence extension *(optional)* | type `4`, present on alternating packets to exercise the full decoder |
| 14+ | Inner IPv4 datagram | a *real* user packet (see §7 mock payloads) |

---

## 4. The per-packet data plane

Every G-PDU accepted at an **ingress** interface (N3 = uplink, N5 = downlink)
passes through one inspection pipeline *before* forwarding. N9 is pure
transit: packets crossing between nodes are never re-inspected or re-counted,
which guarantees each user packet is accounted **exactly once** — by the node
that saw it on N3/N5.

```mermaid
flowchart TB
  R["G-PDU arrives (N3 => dir UL, N5 => dir DL)"] --> T["decode F-TEID + inner payload"]
  T --> S["**SPI** Shallow Packet Inspection\n5-tuple: src/dst IP, L4 proto,\nsrc/dst port, payload length"]
  S --> D["**DPI** application classify (bounded scan of app payload)\ndns / http / https+SNI / ssh / quic / icmp / other"]
  D --> P{"**Policy** engine\nfirst matching rule wins\n(allow | observe | drop)"}
  P -- "drop" --> X["count drop + append\npolicy event (rule, 5-tuple, app)"]
  P -- "observe" --> E["append policy event, keep forwarding"]
  P -- "allow / no match" --> F["forward"]
  E --> F
  F --> L["**Usage ledger** Touch(session, dir, app, bytes, endpoints)"]
  L --> W{"packet anchored locally OR for a peer?"}
  W -- "local" --> O["encapsulate / deliver on egress iface"]
  W -- "peer" --> N9["retag N9, send to peer node"]
```

* **SPI** (`internal/dpi/spi.go`) is a fixed-cost header parse. It never reads
  payload bytes, so it can run at line rate on every packet.
* **DPI** (`internal/dpi/apps.go`) is a *bounded* classification — only the
  first N octets of the application payload are scanned, and only for the
  few app signatures that matter. It is allocation-light on purpose because it
  runs in the forwarding workers.
* **Policy** (`internal/dpi/policy.go`) is an ordered rule list loaded from
  YAML (ConfigMap-mounted per cluster). First match wins; no match = allow.
  Rules match on the SPI 5-tuple and the app class.
* **Aggregation** is sharded: per-interface/per-app counters live in striped
  buckets so the hot path takes a lock only for its own shard.

App classes detected by DPI:

| AppID | Name | Signature |
|---|---|---|
| 0 | unknown | unparseable inner datagram |
| 1 | other | recognised transport, no known app signature |
| 2 | icmp | IP proto 1 |
| 3 | dns | UDP :53, valid query — QNAME extracted for detail |
| 4 | http | TCP :80, request line — `GET /path` extracted |
| 5 | https | TCP :443, TLS ClientHello — **SNI** extracted |
| 6 | ssh | TCP :22 banner |
| 7 | quic | UDP, QUIC long-header first byte |

---

## 5. Traffic sequences

### 5.1 Uplink (N3) — session anchored locally

```mermaid
sequenceDiagram
  participant TG as trafficgen
  participant U0 as upf-0 (anchor)
  participant DN as DN egress
  TG->>U0: G-PDU on N3 (F-TEID ns=N3 node=0 seq=7)
  U0->>U0: SPI + DPI + policy + ledger.Touch(UL)
  alt anchored locally
    U0->>DN: deliver inner IP on N5/DN egress
  else anchored on peer
    U0->>U0: retag ns -> N9 (node=peer seq=7)
    U0->>U1: G-PDU on N9
    U1->>DN: deliver on arrival
  end
```

### 5.2 Downlink (N5) — traffic injected from the DN side

```mermaid
sequenceDiagram
  participant TG as trafficgen
  participant U1 as upf-1 (anchor)
  participant UE as UE/gNB egress
  TG->>U1: G-PDU on N5 (F-TEID ns=N5 node=1 seq=9)
  U1->>U1: SPI + DPI + policy + ledger.Touch(DL)
  U1->>UE: deliver inner IP toward UE on N3 egress
```

### 5.3 East-west (E-W) — anchored on a peer node

```mermaid
sequenceDiagram
  participant TG as trafficgen
  participant A as upf-0 (ingress)
  participant B as upf-1 (anchor)
  TG->>A: G-PDU on N3 (ns=N3 node=1 seq=42)
  A->>A: inspect, ledger.Touch(UL); node(1) != self -> retag N9
  A->>B: G-PDU on N9 (ns=N9 node=1 seq=42)
  B->>B: node == self -> deliver on egress (no re-inspect)
```

The traffic generator emits all three shapes: anchored N3, anchored N5, and
E-W via N3-ingress / N5-ingress / direct-N9, so every data-plane branch is
exercised continuously.

---

## 6. Usage analytics: CDRs and Lawful Interception

Two independent usage feeds come off the data plane:

### 6.1 CDRs (Call Detail Records) — billing-shaped, per session

Sessions are keyed by F-TEID sequence. A session is *closed* when it stays
idle for `UPF_CDR_IDLE` (default 15 s, configurable per node) after living at
least `UPF_CDR_MIN`. A closed CDR carries the full accounting for the session:

| CDR field | Meaning |
|---|---|
| `sessionId`, `nodeId`, `anchorPod` | identity of the session and the node that accounted it |
| `ueAddr`, `dnAddr` | first-seen inner UE / DN endpoints |
| `upPkts/upBytes`, `downPkts/downBytes` | per-direction volume (UL on N3, DL on N5) |
| `startedAt`, `endedAt`, `durationMs` | session lifetime |
| `apps` | per-app breakdown `{"<app_id>": [pkts, bytes]}` |
| `rating`, `status` | simulated rating state (`sim-rated`, `closed`) |

The ledger is sharded and bounded: the pending-CDR queue is capped
(oldest dropped first) so a long collector outage cannot grow memory
without limit, and CDRs **upsert idempotently** on `(session_id, anchor_pod,
started_at)` — re-reporting a closed CDR is always safe.

### 6.2 Lawful-Interception (LI) feed

A configurable session *selector* (`UPF_LI_SESSIONS`, e.g. `0-7,42`) chooses
which sessions emit per-packet header records (5-tuple, app, detail such as
SNI/QNAME) into a bounded ring. This simulates an LI capture without storing
payloads. Policy actions (`observe`/`drop`) emit their own event stream with
the matching rule.

```mermaid
flowchart LR
  subgraph UPF["UPF node hot path (per packet)"]
    INS["inspect()"] --> LED["usage ledger\nTouch(seq, dir, app, bytes)"]
    INS -- "selected\nsession" --> LIR["LI ring\n(header record)"]
    INS -- "policy\nobserve/drop" --> POR["policy ring\n(rule + 5-tuple)"]
  end
  SW["sweeper every 2s\nCloseIdle()"] --> CDRQ["pending CDR queue\n(bounded, idempotent)"]
  LED -. session idle .-> SW
  subgraph DRAIN["one-shot HTTP drains (poll by collector)"]
    C1["GET /api/cdrs"]
    C2["GET /api/events/li"]
    C3["GET /api/events/policy"]
  end
  CDRQ --> C1
  LIR --> C2
  POR --> C3
  C1 & C2 & C3 --> WUI["webui bridge"]
  WUI == "upsert" ==> PG[("PostgreSQL\ncdr / li_events /\npolicy_events")]
```

The drain endpoints are **one-shot** (they return and clear the buffer): the
webui bridge polls them every `WEBUI_AGG_INTERVAL` (default 5 s) and persists
the results. A dead webui therefore only delays reporting; the data plane
keeps forwarding, and CDRs re-report safely on the next drain.

---

## 7. The traffic generator

`internal/traffic` implements a **token-bucket** generator: every flow
accumulates credits at `pps × throttle%` and emits packets on a fixed tick
(`interval_ms`, default 20 ms). The output level is an atomic knob changeable
at runtime from 0–100 %.

### 7.1 Flow model

For every UPF node the generator dials in:

* **anchored** flows — N3 (UL) and N5 (DL) using that node's own namespace;
* **east-west** flows — sessions anchored on a *peer* node, injected here via
  N3, via N5, or direct on N9 (mix chosen per session).

Each flow is one UDP egress socket to `host:port` of the target interface.
Flows share sockets per destination where possible.

### 7.2 Dynamic throttle (0–100 %)

```
GET  /api/throttle        -> {"throttle":75}
POST /api/throttle  body: 75      or {"throttle":75}   (clamped to 0..100)
```

The knob takes effect on the next token tick — no restart. The webui exposes
the same knob as a UI slider and persists its position in Postgres (`ui_state`)
so a page refresh restores the exact level, and re-applies it to every
generator on webui startup.

### 7.3 Realistic mock payloads (for honest DPI)

Inner datagrams are **wire-valid IPv4** with computed IP/TCP/UDP checksums, so
the UPF's SPI/DPI classifies genuine-looking traffic. Each flow emulates one
application (chosen from the F-TEID), and per-packet fields (DNS txid, HTTP
path, TCP/QUIC counters) vary with the packet sequence:

| Mock app | Inner stack | DPI result |
|---|---|---|
| HTTP | IPv4 → TCP :80, `GET /page-NNNNN` + Host | `http`, path in detail |
| HTTPS | IPv4 → TCP :443, truncated **TLS 1.2 ClientHello** with SNI | `https`, SNI in detail |
| DNS | IPv4 → UDP :53, A-record query | `dns`, QNAME in detail |
| QUIC | IPv4 → UDP :443, QUIC v1 long header | `quic` |
| other | IPv4 → UDP, incrementing pattern | `other` |

### 7.4 Dual deployment mode

The *same binary/container* runs in two modes, selected by YAML:

| Mode | Trigger | Endpoint source |
|---|---|---|
| **in-cluster** | `discover:` (or default) | each node's `/api/teids` over headless StatefulSet DNS (`{n}` = ordinal) |
| **external** | `upf:` list non-empty | explicit `host + node + sessions + ports` (IP:NodePort works); discovery skipped |

```mermaid
flowchart TB
  subgraph IN["in-cluster mode"]
    TGI["trafficgen pod"] -- "GET /api/teids" --> U0I["upf-0 .. upf-n\n(headless DNS upf-{n})"]
  end
  subgraph EX["external mode"]
    TGE["bin/trafficgen\nany workstation"] -- "YAML: host:port\n(NodePort / LB)" --> U0E["cluster fabric\nNodePort ingress"]
  end
```

Example external config (`local/trafficgen-external.yaml`):

```yaml
name: trafficgen-ext-0
interval_ms: 20
pps: 100
throttle: 50
upf:
  - host: 192.0.2.10        # cluster / NodePort address
    node: 0
    sessions: 64
    ports: { n3: 32152, n5: 32153, n9: 0 }
  - host: 192.0.2.10
    node: 1
    sessions: 64
    ports: { n3: 32152, n5: 32153, n9: 0 }
```

---

## 8. Web UI, aggregation and state persistence

The webui has two jobs: render the live dashboard, and act as the **persistence
bridge** for the whole fabric.

### 8.1 Live aggregation

Every few seconds the webui fans out over HTTP to each endpoint:

| Source | Endpoints polled | Purpose |
|---|---|---|
| UPF nodes | `/metrics`, `/api/teids`, `/api/dpi`, `/api/cdrs`, `/api/events/policy`, `/api/events/li`, `/api/dn-target` | counters, session info, DPI aggregates, one-shot drains, bench DN-target state |
| traffic generators | `/metrics`, `/api/flows`, `/api/throttle`, `/api/rfc2544` | TX counters, flow table, active knob, finished benchmark runs |

The dashboard itself is served by the webui (`/`). It polls `/api/stats` and
`/api/topology` and renders the animated N3/N5/N9 fabric, per-interface rates
(pps, encaps/decaps, drops), aggregate charts and the pod fleet.

### 8.2 The bridge: fabric → Postgres

`internal/web/bridge.go` is the **only** component that writes to the
database. Each poll cycle it converts control-lite reports into store jobs:

* **metrics** — per-`(pod, iface)` counters plus rates computed from the
  previous snapshot → `metrics_ts`;
* **DPI aggregates** — engine counters are cumulative, so the bridge persists
  **deltas** between flushes (a counter reset, i.e. a pod restart, drops the
  baseline) → `dpi_summary`;
* **policy / LI events** — drained straight through → `policy_events`,
  `li_events`;
* **CDRs** — drained and upserted idempotently → `cdr`;
* **heartbeats** — liveness + a small config snapshot per pod →
  `node_heartbeat`;
* **RFC 2544 results** — finished benchmark runs of every generator →
  `rfc2544_results` (§12).

Writes are batched and buffered inside the store, so a database blip delays
reporting but never blocks the poll loop or the data plane. The store queue
(4096 jobs, burst-drained up to 16 transaction-sized batches per tick) plus
the **verify-then-seen** persistence rule for benchmark rows (a run is
re-upserted until the stored row count matches its result count — the upsert
is idempotent, so a dropped batch under queue pressure self-heals on the
next poll) keep event bursts from losing measurement data.

### 8.3 State retention — zero loss on refresh

The dashboard is **stateless**: on every load it re-derives its state from the
database (metrics history, DPI history, CDRs, events) and re-applies stored
UI state. The traffic-knob position lives in `ui_state`; on webui startup the
bridge reads it back and re-pushes it to every generator, so the exact previous
level survives a webui restart, a generator restart, or a browser refresh.

```mermaid
sequenceDiagram
  participant BR as browser
  participant W as webui
  participant DB as PostgreSQL
  participant TG as trafficgen

  Note over W,DB: webui startup
  W->>DB: read ui_state['throttle']
  alt stored value exists
    W->>TG: POST /api/throttle (restore)
  end

  Note over BR,W: user moves the knob
  BR->>W: POST /api/knob {"pct":50}
  W->>DB: upsert ui_state['throttle']
  W->>TG: POST /api/throttle 50 (all gens)
  TG-->>W: {"throttle":50}
  W-->>BR: {"pct":50,"source":"set"}

  Note over BR,W: page refresh (any time later)
  BR->>W: GET / (dashboard)
  W->>DB: metrics/dpi/cdrs/events + ui_state
  DB-->>W: persisted history + knob=50
  W-->>BR: exact previous state restored
```

---

## 9. Database schema (PostgreSQL 14+)

The store is defined in `internal/dbstore/sql/001_init.sql` and applied
idempotently by `internal/dbstore` (tracked in `schema_migrations`). The webui
is the sole reader/writer.

```mermaid
erDiagram
  schema_migrations {
    text name PK
    timestamptz applied_at
  }
  metrics_ts {
    timestamptz ts PK
    text pod PK
    text iface PK "N3 | N5 | N9 | TX"
    bigint rx_pkts
    bigint tx_pkts
    bigint rx_bytes
    bigint tx_bytes
    bigint encaps
    bigint decaps
    bigint dropped
    bigint n_errors
    double rx_pps
    double tx_pps
    double throttle
  }
  node_heartbeat {
    text pod PK
    text role "upf | trafficgen | webui"
    integer node_id
    jsonb summary
    timestamptz last_seen
  }
  dpi_summary {
    timestamptz ts PK
    text pod PK
    text iface PK
    text direction PK "UL | DL"
    smallint app_id PK
    text app_name
    bigint pkts
    bigint nbytes
    bigint dropped
  }
  policy_events {
    bigserial id PK
    timestamptz ts
    text pod
    text iface
    text direction
    text outer_src
    text outer_dst
    inet inner_src
    inet inner_dst
    smallint l4_proto
    integer src_port
    integer dst_port
    smallint app_id
    text app_name
    text rule_id
    text action "drop | observe"
    integer payload_len
  }
  li_events {
    bigserial id PK
    timestamptz ts
    text pod
    bigint session_id
    text teid
    text direction
    inet inner_src
    inet inner_dst
    smallint l4_proto
    integer src_port
    integer dst_port
    smallint app_id
    text app_name
    text detail
    integer payload_len
  }
  cdr {
    bigserial id PK
    bigint session_id UK "part of unique key"
    text anchor_pod UK "part of unique key"
    smallint node_id
    inet ue_addr
    inet dn_addr
    bigint up_pkts
    bigint down_pkts
    bigint up_bytes
    bigint down_bytes
    timestamptz started_at UK "part of unique key"
    timestamptz ended_at
    bigint duration_ms
    jsonb apps "app_id -> [pkts, bytes]"
    text rating
    text status
    timestamptz created_at
  }
  ui_state {
    text key PK
    jsonb value
    timestamptz updated_at
  }
  rfc2544_results {
    text run_id PK "part of unique key"
    text test PK "loss | throughput | latency | back2back"
    integer frame_size PK "G-PDU octets"
    double offered_pps PK "offered frames/s"
    text tg_pod
    timestamptz ts
    double offered_bps
    bigint sent
    bigint received
    double loss_pct
    double lat_min_us
    double lat_avg_us
    double lat_max_us
    double lat_p50_us
    double lat_p95_us
    double lat_p99_us
    bigint b2b_lossless
  }
```

The store ships as two idempotent migrations — `001_init.sql` (everything
above except the benchmark table) and `002_rfc2544.sql` (§12) — replayed safe
by `schema_migrations`.

Design notes worth calling out:

* **`metrics_ts`** and **`dpi_summary`** are keyed by `(…, ts)` and carry a
  dedicated `ts` index — they are the append-only time series the dashboards
  chart. `dpi_summary` stores *deltas*, so row count is bounded by
  `pods × ifaces × apps × intervals`, not by packets.
* **`policy_events`** is naturally low-rate (only policy hits land here).
* **`li_events`** is driven by the LI selector scope; the engine's ring is
  bounded, but per-packet selection makes the persisted rate essentially
  "traffic × poll cadence" — the store's burst-drain (up to 16 × 256 jobs per
  5 s tick) is sized to keep that from saturating the write queue.
* **`rfc2544_results`** is one row per offered-rate measurement point per
  run; the PK makes the bridge's verify-then-seen re-upsert idempotent and
  keeps history queryable per size and per procedure (§12).
* **`cdr`** has a `UNIQUE (session_id, anchor_pod, started_at)` so the
  idempotent upsert in the bridge is safe under re-reporting.
* **`ui_state`** is a tiny key/value JSONB table — the row that makes a
  browser refresh restore the exact previous knob position.

---

## 10. Configuration & environment-agnostic surface

Nothing about a target cluster is hard-coded. Every component is driven by
flags + environment (which the OpenShift manifests template from ConfigMaps),
and the traffic generator additionally takes a YAML file. The matrix below is
the authoritative reference.

| Variable | Default | Component | Meaning |
|---|---|---|---|
| `POD_NAME` | `upf-0` / `trafficgen-0` | upf, tg | node identity (ordinal drives the fabric) |
| `UPF_NODES` | `2` | upf, tg | total UPF nodes in the fabric |
| `UPF_PEER_DNS` | `upf-{n}.upf-hs.telco.svc.cluster.local` | upf, tg | peer DNS template (`{n}` = ordinal) |
| `N3_PORT` / `N5_PORT` / `N9_PORT` | `2152` / `2153` / `2154` | upf | GTP-U UDP listen ports |
| `N3_BIND` / `N5_BIND` / `N9_BIND` | *(empty)* | upf | pin a data-plane listener to one interface IP (e.g. a Multus attachment); empty = wildcard `0.0.0.0`, so one socket serves the default CNI, secondary Multus networks and NodePort/LB DNATs simultaneously |
| `UPF_SESSIONS` | `256` | upf | session space per node |
| `UPF_WORKERS` | `4` | upf | forwarding worker pool size |
| `DN_TARGET` | *(empty)* | upf | DN egress `host:port` for plain-IP delivery (empty = count only) |
| `UPF_INSPECTION` | `true` | upf | enable the SPI/DPI/policy/CDR pipeline |
| `UPF_POLICY` | *(empty)* | upf | policy rules file (YAML); empty = allow-all |
| `UPF_LI_SESSIONS` | `0-7` | upf | LI session selector, e.g. `0-7,42` |
| `UPF_LI_RING` | `1024` | upf | bounded event ring size |
| `UPF_CDR_IDLE` | `15s` | upf | idle window that closes a CDR |
| `UPF_CDR_MIN` | `1s` | upf | minimum session lifetime before a CDR closes |
| `TG_CONFIG` | *(empty)* | tg | YAML config path (in-cluster vs external) |
| `TG_METRICS` | `:8080` | tg | control HTTP address |
| `TG_PPS` | `100` | tg | per-flow target pps at 100 % (flag/env override) |
| `TG_MAX_SESSIONS` | `32` | tg | max sessions targeted per UPF node |
| `UPF_METRICS_PORT` | `8080` | tg | UPF control-lite port (discovery) |
| `ADDR` | `:8080` | webui | dashboard HTTP address |
| `UPF_ENDPOINTS` | *(empty)* | webui | `name=http://host:port,...` |
| `TG_ENDPOINTS` | *(empty)* | webui | `name=http://host:port,...` |
| `WEBUI_AGG_INTERVAL` | `5s` | webui | bridge poll cadence |
| `DB_DSN` | *(empty)* | webui | PostgreSQL DSN (empty = store disabled) |
| `DB_MAX_CONNS` / `DB_FLUSH_MS` / `DB_QUEUE` | `4` / `5000` / `4096` | webui | pool / flush interval / job queue (flush burst-drains ≤16 batches × 256 jobs per tick) |
| `DB_AUTHFILE` | `~/.docker/config.json` | Makefile (local DB pull) | authfile used when `make test-local` pulls `DB_IMAGE` from registry.redhat.io |

Global orchestration variables consumed by the **Makefile** (not by the Go
binaries): `QUAY_USERNAME`, `QUAY_PASSWORD`, `KUBECONFIG`, plus overridables
`NAMESPACE`, `IMAGE_REGISTRY`, `IMAGE_OWNER`, `TAG`, `PLATFORMS`,
`COMPONENTS`, `DB_IMAGE` (default `registry.redhat.io/rhel9/postgresql-16:latest`),
`DB_AUTHFILE`, and the local-stack `DB_USER` (default `upfgen` — `postgres`
is a built-in role in that image), `DB_PASSWORD`, `DB_NAME`. The e2e runner
additionally reads `E2E_EXTERNAL_ADDR` (`<node-ip>:<n3NodePort>`) to drive
the cross-CNI ingress check; without it that check is skipped.

---

## 11. Deployment topology (OpenShift)

All objects are rendered from `deploy/` via `make deploy` against
`$KUBECONFIG`. Names, ports and the image pull reference all come from
ConfigMaps / Makefile variables, so the same tree deploys to any cluster.

```mermaid
flowchart TB
  subgraph NS["Namespace (configurable)"]
    direction TB

    subgraph SCC["SecurityContextConstraints"]
      SCC1["upf-udp: non-root\nNET_BIND_SERVICE only\n(anyuid/fsGroup RunAsAny)"]
    end

    subgraph DATA["data plane"]
      STS[("upf\nStatefulSet x2\nService upf-hs (headless)\nService upf (ClusterIP)\nUDP 2152/2153/2154\nHTTP 8080")]
    end

    subgraph GEN["traffic"]
      DEP1["trafficgen\nDeployment xN\nService tg-hs (headless)\nService tg (ClusterIP)\nUDP egress\nHTTP 8080 /api/throttle"]
    end

    subgraph OBS["observability"]
      DEP2["webui\nDeployment x1\nService webui (ClusterIP)\nRoute webui (dashboard)\nHTTP 8080"]
      STSDB[("db\nStatefulSet x1\nService db (ClusterIP)\nPVC (persistent)\nHTTP 5432")]
    end

    subgraph EXT["external ingress"]
      LB["NodePort / LoadBalancer\nfor external trafficgen\ne.g. N3/N5 GTP-U UDP"]
    end
  end

  SCC -. constrains .-> DATA & GEN & OBS
  DEP1 -- GTP-U --> STS
  DEP2 -- poll --> STS
  DEP2 -- poll --> DEP1
  DEP2 == "persist" ==> STSDB
  EXT -. external load .-> STS
```

Key properties:

* **`upf`** is a `StatefulSet` because node identity (ordinal) is meaningful —
  it is baked into the F-TEID `node` field and into the headless DNS name that
  peers and the generator resolve.
* **GTP-U is UDP**, so the Services expose UDP on 2152/2153/2154. External
  load comes in through a NodePort/LoadBalancer service (the "external
  ingress" path) or by running `bin/trafficgen` off-cluster in *external* mode.
* A dedicated **SCC** (`upf-udp`) lets the non-root pods bind the GTP-U UDP
  ports below 1024 via `NET_BIND_SERVICE` — no host namespaces, no raw
  sockets, all other capabilities dropped.
* **`db`** is a single-replica `StatefulSet` with a **PVC** so the metrics /
  usage / state survive pod rescheduling and restarts.
* **`upf-env` / `tg-env` / `webui-env`** (`deploy/configmap.yaml`) carry
  every tunable per workload family (fabric shape, ports + bind IPs,
  inspection, throttle, endpoints, webui settings) consumed via `envFrom`;
  an explicit `env` entry always wins, so per-deployment overrides need no
  manifest edits.
* **`upf-ext`** (`deploy/external-ingress.yaml`) is the NodePort/LoadBalancer
  GTP-U ingress (udp 32152-32154 + tcp 32151 control) used by cross-CNI
  validators and off-cluster generators.
* **Multus NADs** (`deploy/multus.yaml`, `make deploy-multus`) optionally
  attach the UPF and generator pods to genuine secondary networks — see
  "CNI topologies" below.

### CNI topologies — same network vs distinct networks

The data plane is CNI-agnostic by construction: forwarders use the F-TEID
dispatch (no addresses), listeners wildcard-bind by default, replies target
whatever source address a packet arrived from. The tree deploys a **NodePort
service** (`upf-ext`, `deploy/external-ingress.yaml`) for gTP-U ingress from
outside the default network, and optional **Multus attachments**
(`deploy/multus.yaml`, `make deploy-multus`) that put generator + UPF pods on
a true secondary networks — both topologies validated with the same e2e
suite.

#### Same CNI (flat, default)

All workloads ride the default pod network; the headless Service resolves
each node, traffic never leaves the CNI.

```mermaid
flowchart LR
  subgraph NET1["default CNI (flat)"]
    TG0["trafficgen-0"] -- "GTP-U udp/2152" --> U0["upf-0"]
    TG0 -- "GTP-U udp/2152" --> U1["upf-1"]
    WUI["webui"] -- "poll" --> TG0 & U0 & U1
  end
  TG0 -. "svc/webui" .- WUI
  U0 <-->|"N9 fabric"| U1
```

#### Distinct CNI networks

Three independent sources of load cross network boundaries into the UPF:
(1) a Multus-attached generator on its own CIDR, (2) an off-cluster
generator entering through the NodePort/LB service (`upf-ext`), which walks
campus routing and any NAT between networks, and (3) the webui staying on
the default CNI throughout.

```mermaid
flowchart TB
  subgraph NET2["default CNI"]
    WUI2["webui (aggregation · DB)"]
    U02[("upf-0\npod IP via upf-hs\n+ net1 on Multus CIDR")]
  end
  subgraph MU["Multus secondary networks (NADs)"]
    TG2["trafficgen+tg-data\n10.60.0.0/24"] -- "GTP-U udp/2152\n(cross-CNI routed)" --> U02
  end
  subgraph OFF["off-premises network"]
    TGX["bin/trafficgen\n(any host)"] -- "GTP-U udp/32152\nNodePort/LB — routed + NAT" --> ING["svc upf-ext : NodePort 32151-32154"] -- DNAT --> U02
  end
  WUI2 -. "poll control 8080" .-> TG2 & U02
```

#### Validation matrix

| Path | How it gets exercised |
|---|---|
| same CNI | always: `make e2e` check *traffic flow* (in-cluster generator + headless DNS) |
| distinct CNI (Multus) | `make deploy-multus`, then `make e2e` — pods attach `net1` on the NAD CIDRs (verify with `oc -n telco exec upf-0 -- ip -4 addr show net1`) and flow counters increase from the secondary network |
| distinct CNI (NodePort/LB + campus NAT) | `make e2e E2E_EXTERNAL_ADDR=<node-ip>:32152` — the e2e sends a G-PDU burst from the workstation and asserts every datagram propagates through the DNAT path into the UPF N3 counters |

For an off-cluster generator run, use the ready-made external profile
(`local/trafficgen-external.yaml`) — it points at the NodePort/LB endpoints
and the TCP control port 32151 the bench needs for `/api/dn-target`; full
schema in section 7.4.

---

## 12. RFC 2544 benchmarking (implemented)

The traffic generator's token-bucket engine, per-flow counters and dual-mode
addressing double as a native **RFC 2544** benchmarking suite. A run is a
**closed loop on the live fabric**: the generator *pauses* its normal flows,
claims the reserved top session sequence of the target node (`seq =
sessions-1`) as its transceiver, injects G-PDU frames into that node's N3
(direct UDP to the ingress port), and reads them back where the node delivers
to the data network. The data-network egress is a **runtime switch** —
`GET/POST /api/dn-target` on the UPF — which the bench points at its own RX
socket for the run and restores afterwards ("locally anchored" mode: no N9
usage, one node absorbs its own traffic).

```
bench frame (G-PDU, no seq-ext)
┌──────────────┬──────────────────────────────┬───────────┬─────────────┐
│ GTP hdr+TEID │ inner IPv4/UDP (real csums)  │ 8B index  │ 8B tx stamp │
│              │                              │ + payload │ (ns, monot. │
│              │                              │  filler   │  clock)     │
└──────────────┴──────────────────────────────┴───────────┴─────────────┘
```

The RX side correlates by the 8-byte index (no per-packet timestamps in the
outer header) and computes one-way latency from the monotonic transmit stamp.

### 12.1 The four procedures

| Procedure | Algorithm | Reported |
|---|---|---|
| **Frame loss rate** | log-ladder of offered rates from `min_rate`→`max_rate` (6 points, geometric steps; or explicit `candidate_rates`), each probe held for `duration_ms/2` (clamped 250–1500 ms) | `loss% = (sent-received)/sent` per rate × frame size |
| **Throughput** | geometric (sqrt) bisect between the largest lossless ladder rate and `max_rate`, ≤10 probes, 5 % loss tolerance stop, then one confirmation run at the found rate | maximum zero-ish-loss offered rate + wire Mb/s |
| **Latency** | light offered rate (`latency_rate`, default `min_rate`), `duration_ms` clamped 500–5000 ms, per-frame tx stamps | one-way min/avg/max and p50/p95/p99 |
| **Back-to-back** | ascending unpaced bursts, stopping at the first lossy burst | longest lossless burst (frames) |

Offered bit/s = pps × frame octets × 8, so offered Gbps on the dashboard is
the product of the rate window and the frame size (1.73 Mpps × 65000 octets
≈ 900 Gbps offered). The engine is scalable per profile: `send_workers`
gives each sender its own N3 socket and token-bucket share, `rx_workers`
spawns receive loops on the sink; the deployment default (4/4) rides in
`TG_BENCH_PROFILE` on `deploy/configmap.yaml`. Because the path is Go
kernel sockets (no DPDK/AF_XDP), the achievable lossless rate — what the
throughput bisection reports — is memory-bandwidth bound; scale the workers
with `deploy/*.yaml` CPU limits and `UPF_WORKERS`.

A run follows the YAML/JSON **profile** (frame sizes × tests × rates). Runs
are kept in generator memory (last 10) with their own `run_id`, status and
progress; the control surface is proxied through the webui:

```
GET  /api/rfc2544               webui → rows read from Postgres (charts' source)
GET  /api/rfc2544?lite=1        generator-side live status (no run history)
POST /api/rfc2544               start run (profile body; forwarded to a generator)
POST /api/rfc2544/cancel        stop the active run
```

### 12.2 Results persistence and rendering

```mermaid
flowchart LR
  subgraph GEN["traffic generator"]
    T["bench engine"] --> R["run history\n(rfc2544 status)"]
  end
  W["webui bridge"] -- "poll finished runs" --> R
  W -- "re-upsert until row count verified" --> PG[("PostgreSQL\nrfc2544_results")]
  UI["RFC 2544 panel\n4 canvas charts"] -- "GET /api/rfc2544" --> PG
```

Every finished run's measurement points are bridged into the
`rfc2544_results` table (primary key `(run_id, test, frame_size,
offered_pps)`, idempotent `ON CONFLICT DO NOTHING` upsert, `ts` index). The
bridge marks a run seen **only after counting the stored rows** — under
queue pressure a dropped write batch is repaired on the next poll instead of
being lost forever. The dashboard panel (loss curves on a log offered-rate
axis, zero-loss throughput bars, latency min/avg/max, back-to-back bursts)
renders **from those database rows**, so charts survive webui, generator and
browser restarts, and a page refresh re-reads history the same way as all
other analytics.

---

## 13. Repository layout

```
cmd/
  upf/            data-plane entrypoint (flags + env)
  trafficgen/     generator entrypoint (YAML config, throttle API)
  webui/          dashboard + persistence bridge
  e2e/            live-cluster end-to-end test runner
internal/
  gtpu/           GTPv1-U codec (+ tests)
  upf/            F-TEID / session table, forwarding engine (+ tests)
  dpi/            SPI, DPI, policy, engine, LI ring, aggregator (+ tests)
  usage/          per-session ledger -> CDRs (+ tests)
  traffic/        token-bucket flows, YAML config, mock payloads, throttle
  telemetry/      lock-free counters + JSON snapshots
  web/            aggregation API, dashboard, DB bridge
  dbstore/        PostgreSQL store, migrations, read APIs
deploy/           OpenShift manifests (Namespace, SCC, STS, Deployments,
                  Services, Routes, PVC, ConfigMaps)
local/            example YAML configs (local / external TG, policy)
docs/             architecture.md (+ Mermaid) and docs/podman-demo.md
                  (live podman local-stack walkthrough with rendered proofs)
*.Containerfile   UBI9 multi-stage images per component (dnf-only, uid 1001)
Makefile          all podman + oc automation (help/lint/build/push/...)
.github/          build-and-push CI (fmt/vet/test gate, parallel image matrix)
```

The data-plane packages (`gtpu`, `upf`, `dpi`, `usage`, `traffic`) are
deliberately free of any database import — the *only* place that knows about
PostgreSQL is `internal/dbstore` and the webui that uses it. That isolation is
what guarantees a database failure can never stall user-plane forwarding.
