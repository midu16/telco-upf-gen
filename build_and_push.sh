#!/usr/bin/env bash
#
# build_and_push.sh — build and push telco-upf-gen images with Podman.
#
#   quay.io/midu/upf:latest        (gtpu user plane)
#   quay.io/midu/trafficgen:latest (traffic generator)
#   quay.io/midu/webui:latest      (dashboard)
#
# Auth: $QUAY_PASSWORD required (username via $QUAY_USER, default "midu").
#
# Platforms:
#   single arch (default, native):  ./build_and_push.sh
#   multi-arch manifest:            PLATFORMS=linux/amd64,linux/arm64 ./build_and_push.sh
#   only some components:           COMPONENTS=upf,webui ./build_and_push.sh
#
set -euo pipefail

REGISTRY="quay.io"
OWNER="${QUAY_OWNER:-midu}"
QUAY_USER="${QUAY_USER:-midu}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLATFORMS="${PLATFORMS:-}"                 # empty => native only
COMPONENTS="${COMPONENTS:-upf,trafficgen,webui}"

log() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }

[[ -n "${QUAY_PASSWORD:-}" ]] || {
    echo "ERROR: \$QUAY_PASSWORD is not set" >&2
    echo "  export QUAY_PASSWORD=*** && $0" >&2
    exit 1
}
command -v podman >/dev/null || { echo "ERROR: podman not found" >&2; exit 1; }

# ---------------------------------------------------------------- 1. login
log "logging in to ${REGISTRY} as ${QUAY_USER}"
printf '%s' "${QUAY_PASSWORD}" | podman login "${REGISTRY}" \
    --authfile "$REPO_ROOT/.quay-auth.json" \
    -u "${QUAY_USER}" --password-stdin
trap 'rm -f "$REPO_ROOT/.quay-auth.json"' EXIT

# ---------------------------------------------------------------- 2. platforms
native="$(podman info --format '{{.Host.Arch}}' 2>/dev/null || uname -m)"
case "${PLATFORMS}" in
    "")  PLATFORM_LIST=("linux/${native}") ;;
    *)   IFS=',' read -r -a PLATFORM_LIST <<< "${PLATFORMS}" ;;
esac
log "platforms: ${PLATFORM_LIST[*]}"
log "components: ${COMPONENTS}"

# ---------------------------------------------------------------- 3. build+push
IFS=',' read -r -a PARTS <<< "${COMPONENTS}"
for comp in "${PARTS[@]}"; do
    comp="$(echo "${comp}" | xargs)"
    [[ -n "${comp}" ]] || continue
    cf="${REPO_ROOT}/${comp}.Containerfile"
    [[ -f "${cf}" ]] || { echo "ERROR: ${cf} not found" >&2; exit 1; }
    tag="${REGISTRY}/${OWNER}/${comp}:latest"
    tmp="${REGISTRY}/${OWNER}/${comp}:tmp-${RANDOM}"

    log "building ${comp} for ${PLATFORM_LIST[*]}"
    if [[ "${#PLATFORM_LIST[@]}" -eq 1 ]]; then
        podman build \
            --authfile "$REPO_ROOT/.quay-auth.json" \
            --platform "${PLATFORM_LIST[0]}" \
            -t "${tag}" \
            -f "${cf}" "${REPO_ROOT}"
        log "pushing ${tag} (${PLATFORM_LIST[0]})"
        podman push --authfile "$REPO_ROOT/.quay-auth.json" "${tag}"
    else
        # multi-arch: build each arch, amend into one manifest, push once
        podman manifest rm "${tmp}" >/dev/null 2>&1 || true
        podman manifest create "${tmp}"
        for p in "${PLATFORM_LIST[@]}"; do
            podman build \
                --authfile "$REPO_ROOT/.quay-auth.json" \
                --platform "${p}" \
                -t "${tmp}-${p//\//-}" \
                -f "${cf}" "${REPO_ROOT}"
            podman manifest add "${tmp}" "${tmp}-${p//\//-}"
        done
        podman manifest push --authfile "$REPO_ROOT/.quay-auth.json" "${tmp}" "${tag}"
        podman manifest rm "${tmp}" >/dev/null 2>&1 || true
        log "pushed multi-arch ${tag}"
    fi
done

log "done:"
podman manifest inspect "${REGISTRY}/${OWNER}/upf:latest" >/dev/null 2>&1 || true
for comp in "${PARTS[@]}"; do
    log "  ${REGISTRY}/${OWNER}/${comp}:latest"
done
