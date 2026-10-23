# telco-upf-gen — Podman Container Demo

Step-by-step demo of the **full distributed 5G UPF simulator running in
podman containers**: 2 UPF nodes, 2 traffic generators and the Web UI on one
shared container network, with captured proof of every step.

Everything below was executed for real on 2026-10-22; each screenshot is a
render of the actual command output captured at that step. The dashboard
view (`step08-dashboard-live.png`) is drawn from the live pod `/metrics` /
webui `/api/stats` data sampled during the run (no headless browser exists
in this environment — open `http://127.0.0.1:18082` in a browser for the
real animated UI; all numbers shown are the same live values).

## Architecture as a container fan

```mermaid
flowchart TB
    subgraph NET["podman network upf-demo (CNI, embedded DNS)"]
        direction TB

        subgraph TGs["traffic generators"]
            TG0["tg-0<br/>96 token-bucket<br/>GTP-U flows"]
            TG1["tg-1<br/>96 token-bucket<br/>GTP-U flows"]
        end

        subgraph UPFs["UPF fabric (quay.io/midu/upf)"]
            UPF0["upf-0 (node 0)<br/>N3 :2152 · N5 :2153 · N9 :2154"]
            UPF1["upf-1 (node 1)<br/>N3 :2152 · N5 :2153 · N9 :2154"]
        end

        WEB["webui-demo (:8082)<br/>fan-out /metrics + /api/flows"]
    end

    DN[("DN egress<br/>absorbed / DN_TARGET<br/>plain IPv4")]

    TG0 -- "N3 · GTP-U (UL, anchored+EW)" --> UPF0
    TG0 -- "N5 · GTP-U (DL, anchored+EW)" --> UPF0
    TG1 -- "N3 · GTP-U (UL, anchored+EW)" --> UPF1
    TG1 -- "N5 · GTP-U (DL, anchored+EW)" --> UPF1
    TG0 -- "N3/N5 toward upf-1 (EW inject)" --> UPF1
    TG1 -- "N3/N5 toward upf-0 (EW inject)" --> UPF0

    UPF0 <== "N9 · GTP-U · east-west (teid retag)" ==> UPF1
    UPF0 -- "decap → plain IP" --> DN
    UPF1 -- "decap → plain IP" --> DN

    WEB -. "poll 2 s" .-> UPF0
    WEB -. "poll 2 s" .-> UPF1
    WEB -. "poll 2 s" .-> TG0
    WEB -. "poll 2 s" .-> TG1
```

All 5 containers share one podman CNI network (`upf-demo`), which provides
**container-name DNS** — that single feature plays the role of OpenShift's
headless Service in the cluster deployment: `upf-0`/`upf-1` resolve for
N9 peer discovery and for the Web UI endpoints.

## Prerequisites

| Requirement | Note |
|---|---|
| podman ≥ 4 | used: 5.8.7 |
| images | `quay.io/midu/{upf,trafficgen,webui}:latest` (local after `make build push`, or pulled) |
| capabilities | `NET_BIND_SERVICE` per UPF/TG container — the non-root images bind GTP-U ports 2152–2154 (<1024). This is exactly what the OpenShift `upf-udp` SCC grants. |
| host ports | 18080/18081 (UPF metrics), 18090/18091 (TG metrics), 18082 (dashboard) |

## Step 00 — images available

```bash
podman images --format '{{.Repository}}:{{.Tag}}   {{.Size}}' \
  | grep -E 'midu/(upf|trafficgen|webui):latest'
```

![step 00](img/step00-images.png)

*Proof:* all three images exist locally (built & pushed by `make build push`),
so `podman run` below starts them without re-pulling.

## Step 01 — isolated podman network

```bash
podman network create upf-demo
podman network ls --format '{{.Name}}'
```

![step 01](img/step01-network.png)

*Why:* podman's embedded DNS on a custom network gives every container a
`<name>` A record. The UPF's `UPF_PEER_DNS='upf-{n}'` template and the
Web UI's `http://upf-0:8080` endpoints both resolve through it. On OpenShift
this is the headless Service (`upf-hs`/`tg-hs`).

## Step 02 — UPF node 0

```bash
podman run -d --name upf-0 --network upf-demo --cap-add NET_BIND_SERVICE \
  -p 18080:8080 \
  -e POD_NAME=upf-0 -e UPF_NODES=2 -e UPF_PEER_DNS='upf-{n}' \
  quay.io/midu/upf:latest

curl -s http://127.0.0.1:18080/healthz
```

![step 02](img/step02-upf0.png)

*What you see:* log lines `N3 ready on udp/:2152`, `N5 … /2153`,
`N9 … /2154` — the three GTP-U interfaces bound inside the container by a
non-root process; `healthz: ok`. The node ordinal (0) is derived from
`POD_NAME`, deriving the exact same F-TEID space as `upf-1`.

## Step 03 — UPF node 1

```bash
podman run -d --name upf-1 --network upf-demo --cap-add NET_BIND_SERVICE \
  -p 18081:8080 \
  -e POD_NAME=upf-1 -e UPF_NODES=2 -e UPF_PEER_DNS='upf-{n}' \
  quay.io/midu/upf:latest

curl -s http://127.0.0.1:18081/healthz
```

![step 03](img/step03-upf1.png)

*What you see:* second node up. From here the fabric is live: each node
resolves its peer over network DNS and is ready to accept N9 traffic.

## Step 04 — traffic generators (2)

```bash
podman run -d --name tg-0 --network upf-demo -p 18090:8080 \
  -e POD_NAME=tg-0 -e UPF_NODES=2 -e UPF_PEER_DNS='upf-{n}' \
  -e TG_PPS=100 -e TG_MAX_SESSIONS=32 \
  quay.io/midu/trafficgen:latest
# identical for tg-1 (port 18091)
```

![step 04](img/step04-trafficgen.png)

*What you see:* the generator **self-discovered both UPFs** via
`/api/teids` (`discovered upf-0 … upf-1`) and planned **96 flows**: for each
of the 32 sessions — anchored N3 uplink, anchored N5 downlink, and
east-west flows whose F-TEID embeds the *peer* anchor node.

## Step 05 — Web UI

```bash
podman run -d --name webui-demo --network upf-demo -p 18082:8080 \
  -e UPF_ENDPOINTS='upf-0=http://upf-0:8080,upf-1=http://upf-1:8080' \
  -e TG_ENDPOINTS='tg-0=http://tg-0:8080,tg-1=http://tg-1:8080' \
  quay.io/midu/webui:latest

curl -s http://127.0.0.1:18082/healthz
curl -s http://127.0.0.1:18082/api/topology
```

![step 05](img/step05-webui.png)

*What you see:* `ok` + the topology the dashboard will plot (2 UPF + 2 TG
endpoints, container-DNS addressed). Open **http://127.0.0.1:18082** for
the live animated dashboard.

## Step 06 — UPF metrics: encapsulation & decapsulation

```bash
curl -s http://127.0.0.1:18080/metrics | python3 -m json.tool
curl -s http://127.0.0.1:18081/metrics | python3 -m json.tool
```

![step 06](img/step06-upf-metrics.png)

*What you see after ~1 minute:* N3 `txPkts/encaps` growing (GTP-U capped
downlink), N5 `decaps` growing (tunnels stripped onto the DN), N9 growing
(see step 07), `dropped=0` and `errors=0` on every interface of both nodes.

## Step 07 — the east-west (N9) proof

```bash
# N9 counters on both nodes
curl -s http://127.0.0.1:18080/metrics   # + :18081
# flow table of tg-0
curl -s http://127.0.0.1:18090/api/flows
```

![step 07](img/step07-ew-n9.png)

*What you see:* flows named `*ew*@n1` hit `upf-0` while carrying
`anchorNode=1` (teid `0x10010001` = N3-namespace, node **1**, session 1).
`upf-0` retags them into the N9 namespace (`0x40…`) and transmits them to
`upf-1`, which decaps onto its DN. Result: **52,224 packets on N9 in each
direction, perfectly symmetric, 0 drops** — the inter-UPF fabric is
provably carrying user traffic.

## Step 08 — live rates (webui aggregation, 6 × 3 s samples)

```bash
curl -s http://127.0.0.1:18082/api/stats   # all 4 pods reachable
# + per-UPF 3-second counter deltas (what the dashboard's chart shows)
```

![step 08 rates](img/step08-live-pps.png)

Steady state at 2 × 96 flows × 100 pps per generator:

| interface | per UPF | meaning |
|---|---|---|
| N3 rx | ~4.8 k pps | UL GTP-U from "gNBs" (anchored + EW-injected) |
| N5 rx | ~4.8 k pps | DN→UE downlink GTP-U |
| N9 tx/rx | ~3.2 k pps | east-west crossings (both directions) |
| total | **~25.6 k pps** | fabric aggregate ingest |

and the live fabric view rendered from that data:

![step 08 dashboard data](img/step08-dashboard-live.png)

## Step 09 — final state

```bash
podman ps --filter network=upf-demo
```

![step 09](img/step09-pods.png)

Five containers, all `Up`, on one CNI network: the complete distributed UPF
simulator minus the orchestrator.

## What each step proves

| Step | Evidence |
|---|---|
| 00 | production images are the runtime artifacts (quay.io/midu/*) |
| 01 | container-name DNS = headless-service equivalent |
| 02–03 | non-root GTP-U plane binds 2152–2154 with only `NET_BIND_SERVICE` |
| 04 | self-discovery via `/api/teids`, 96 flows incl. EW |
| 05 | dashboard aggregates all pods behind one endpoint |
| 06 | encaps/decap counters, zero drops under load |
| 07 | N9 east-west path: F-TEID anchor retag + symmetric 52 k pkt transit |
| 08 | ~25.6 k pps steady-state, per-interface breakdown |
| 09 | stable multi-container state |

## Cleanup

```bash
podman rm -f upf-0 upf-1 tg-0 tg-1 webui-demo
podman network rm upf-demo
```

## Variants

- **More rate:** `-e TG_PPS=500` on the generators (token bucket, ~120 k pps).
- **Bigger fabric:** add `upf-2`/`upf-3` containers, bump `UPF_NODES`, append
  them to `UPF_ENDPOINTS`/`TG_ENDPOINTS` (F-TEID node bits already cover 0–255).
- **Real DN egress:** `-e DN_TARGET=<host>:<port>` on the UPFs to watch
  decapped plain-IPv4 leave the tunnel.
- **Cluster mode:** identical env vars in `deploy/*.yaml`; StatefulSet
  ordinals supply the `POD_NAME`s, headless Services supply the DNS names.
