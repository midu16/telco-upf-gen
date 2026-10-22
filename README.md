# telco-upf-gen

Distributed 5G UPF (User Plane Function) **simulator** for OpenShift.
Native GTP-U (3GPP TS 29.281 / RFC 8180) encapsulation/decapsulation across
simulated **N3**, **N5** (DN) and **N9** (inter-UPF) interfaces, a dedicated
traffic generator, and a real-time Web UI that visualizes the packet flow
between pods.

## Architecture

```
        N3 (gNB/UE)    N5 (DN)
 trafficgen-0 ──GTP-U──► upf-0 ◄══════ N9 ═══════► upf-1 ◄──GTP-U── trafficgen-1
                                   │ decap → plain IP
                                   ▼
                            DN target (optional)

 webui ──fan-out──► /metrics + /api/flows on every pod
```

* **`upf`** — one pod per node, StatefulSet (2). Listens for GTP-U on
  N3/N5/N9, resolves F-TEIDs, forwards user payloads across the fabric.
  Deterministic F-TEID scheme, **no control plane**:
  `namespace<<24 | node<<16 | session<<0`.
  Packets whose F-TEID names a *different* node are retagged into the N9
  namespace and crossed to that peer — that is the east-west path.
* **`trafficgen`** — one pod per replica (2). Discovers UPFs via `/api/teids`,
  streams token-bucket flows: anchored N3 uplink, anchored N5 downlink,
  east-west via N3 ingress / N5 ingress / direct N9.
* **`webui`** — fans out to every pod's `/metrics` (+`/api/flows` on TGs),
  renders animated N3/N5/N9 fabric view, per-interface pps, encaps/decaps,
  drops, aggregate chart.

## Wire format (G-PDU)

```
octet 0  : 0x30   VER=001 GTPv1, T/U=1 user plane (S flag if seq ext present)
octet 1  : 0xFF   G-PDU
octet 2-3: payload length (BE)
octet 4-7: F-TEID (BE)
octet 8+ : [6-octet sequence-number ext, alternating] + IPv4 datagram
```

## Layout

```
cmd/upf  cmd/trafficgen  cmd/webui       entrypoints
internal/gtpu            GTP-U codec (+tests)
internal/upf             session table, forwarding engine (+tests)
internal/traffic         token-bucket flows, synthetic IPv4
internal/telemetry       lock-free counters + JSON snapshots
internal/web             aggregation API + embedded dashboard
deploy/                  OpenShift manifests
upf.Containerfile  trafficgen.Containerfile  webui.Containerfile
build_and_push.sh        Podman build/push → quay.io/midu/*
.github/workflows/build-and-push.yml   CI: multi-arch on push to main
```

## Build & push (local, Podman)

```bash
export QUAY_PASSWORD=***                # username defaults to midu
./build_and_push.sh                                      # native arch
PLATFORMS=linux/amd64,linux/arm64 ./build_and_push.sh    # multi-arch manifest
COMPONENTS=upf ./build_and_push.sh                       # subset
```

## CI (GitHub Actions)

`.github/workflows/build-and-push.yml` runs on every push to `main`:

1. `test` — `go vet` + `go test ./...`
2. `build-push` — buildx + QEMU, `linux/amd64,linux/arm64` per image,
   pushed to `quay.io/midu/{upf,trafficgen,webui}:latest` (GHA build cache).

Repo secrets required: `QUAY_USERNAME` (can be blank → defaults to `midu`
owner `midu`) and `QUAY_PASSWORD` (a Quay robot token or password).

## Deploy (OpenShift / SNO)

```bash
# 1. images (either CI after push to main, or locally)
export QUAY_PASSWORD=***
./build_and_push.sh

# 2. manifests (order matters: ns → scc → workloads)
oc apply -f deploy/namespace.yaml
oc apply -f deploy/scc.yaml          # SCC "upf-udp": non-root + NET_BIND_SERVICE
oc apply -f deploy/upf.yaml          # StatefulSet(2) + headless svc upf-hs
oc apply -f deploy/trafficgen.yaml   # StatefulSet(2) + headless svc tg-hs
oc apply -f deploy/webui.yaml        # Deployment + Service + Route

oc -n telco get pods -w
oc -n telco get route webui          # dashboard URL
```

Dashboard shows: animated fabric (N3 blue gNB↔UPF, N5 green DN↔UPF,
N9 orange UPF↔UPF, edge width ∝ live pps), aggregate ingest chart,
per-pod per-interface cards (rx/tx pps, pkts, encap/decap, drops)
and a full interface table.

**SCC note:** the plane uses plain UDP sockets (no AF_PACKET/raw), so the
`upf-udp` SCC only adds non-root + `NET_BIND_SERVICE` (GTP-U data ports
2152–2154 are <1024). No privileged SCC, no host namespaces.

To observe decapped plain-IP egress, set `DN_TARGET` in `deploy/upf.yaml`
to a reachable UDP sink.

## Verification status

* unit: GTP-U codec byte-exact; F-TEID math; in-process 2-node fabric test
  (N3→N9 retag EW, N9 direct, local anchor, bad-NS/unknown-session/garbage drops)
* integration: 3-process local run (UPF+TG+webui) at ~10 kpps, zero drops
* image: built with podman, container smoke-tested non-root (healthz,
  /api/teids, UDP 2152–2154 bound)
