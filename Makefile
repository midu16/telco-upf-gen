# telco-upf-gen — centralized orchestration.
#
# All podman + oc automation lives here. Bash scripts are retired in favor
# of these targets. Run `make` (or `make help`) for the menu.
#
# ── Required environment ────────────────────────────────────────────────
#   QUAY_USERNAME   Quay user (default: midu)             — make push
#   QUAY_PASSWORD   Quay token / password                  — make push
#   KUBECONFIG      kubeconfig file for the target cluster — make deploy / e2e
#
# ── Optional overrides (defaults in brackets) ───────────────────────────
#   NAMESPACE [telco]        OpenShift namespace
#   IMAGE_REGISTRY [quay.io] image registry
#   IMAGE_OWNER [midu]       registry namespace / owner
#   TAG [latest]             image tag
#   PLATFORMS [native]       "linux/amd64,linux/arm64" for multi-arch
#   COMPONENTS [upf trafficgen webui]  images to build/push
#   DB_IMAGE [registry.redhat.io/rhel9/postgresql-16:latest]
#   DB_AUTHFILE [~/.docker/config.json]  # authfile for the local DB pull
#   UPF_NODES [2]  TG replicas [2]
#
PODMAN  ?= podman
OC      ?= oc
GO      ?= go

QUAY_USERNAME ?= midu
QUAY_PASSWORD ?=
KUBECONFIG    ?=

NAMESPACE      ?= telco
IMAGE_REGISTRY ?= quay.io
IMAGE_OWNER    ?= midu
IMAGE          := $(IMAGE_REGISTRY)/$(IMAGE_OWNER)
TAG            ?= latest
PLATFORMS      ?=
COMPONENTS     ?= upf trafficgen webui
DB_IMAGE       ?= registry.redhat.io/rhel9/postgresql-16:latest
# Auth file the local podman pull should use for DB_IMAGE (register.redhat.io
# needs a Red Hat credential; the cluster pull secret works too).
#   make test-local DB_AUTHFILE=/var/apps/sno.…/workdir/auth/pull-secret.json
DB_AUTHFILE    ?= $(HOME)/.docker/config.json

LOCAL_NET     ?= upfgen-local
LOCAL_WEBUI   ?= 127.0.0.1:3080:8080
LOCAL_TG      ?= 127.0.0.1:3081:8080
LOCAL_UPF_N3  ?= 127.0.0.1:2152:2152/udp
LOCAL_UPF_N5  ?= 127.0.0.1:2153:2153/udp
LOCAL_DB      ?= 127.0.0.1:15432:5432
# NB: "postgres" is a built-in role in the RHEL PostgreSQL image.
DB_USER       ?= upfgen
DB_PASSWORD   ?= postgres
DB_NAME       ?= telco_upf

AUTH := .quay-auth.json
BIN  := bin

# ------------------------------------------------------------------ menu

.DEFAULT_GOAL := help

## help: show this menu (default target)
help:
	@echo ""
	@echo "  telco-upf-gen — 5G UPF simulator orchestration"
	@echo "  ====================================================="
	@echo ""
	@echo "  Make targets:"
	@echo "  -----------------------------------------------------"
	@grep -hE '^## ' $(MAKEFILE_LIST) | sed 's/^## /\    /'
	@echo ""
	@echo "  Required environment:"
	@echo "    QUAY_USERNAME   Quay user (push)      [default: midu]"
	@echo "    QUAY_PASSWORD   Quay token/password   (push)"
	@echo "    KUBECONFIG      kubeconfig file       (deploy, e2e)"
	@echo ""
	@echo "  Example:"
	@echo "    export QUAY_PASSWORD=*** KUBECONFIG=/path/to/kubeconfig"
	@echo "    make build push deploy e2e"
	@echo ""

# ---------------------------------------------------------- validation

define check_env
	@if [ -z "$$($(1))" ]; then \
		echo "ERROR: \$$($(1)) is not set"; \
		echo "  export $($(1))=*** && $0 $(2)"; \
		exit 1; \
	fi
endef

# ---------------------------------------------------------------- source

## lint: gofmt check + go vet (+ staticcheck when installed)
lint:
	@echo "==> gofmt"
	@out=$$(gofmt -l .) && [ -z "$$out" ] || { echo "$$out"; echo "gofmt: run 'gofmt -w .'"; exit 1; }
	@echo "==> go vet"
	$(GO) vet ./...
	@if command -v staticcheck >/dev/null 2>&1; then \
		echo "==> staticcheck"; \
		staticcheck ./...; \
	else \
		echo "==> staticcheck (not installed, skipped)"; \
	fi

## tidy: sync go.mod / go.sum
tidy:
	$(GO) mod tidy

## test: run the Go unit + integration test suite
test:
	$(GO) test ./...

## fmt: write gofmt fixes
fmt:
	gofmt -w .

# -------------------------------------------------------------- binaries

## bin: build all Go binaries into bin/
bin:
	@mkdir -p $(BIN)
	@for c in upf trafficgen webui e2e; do \
		echo "==> bin/$$c"; \
		$(GO) build -trimpath -ldflags '-s -w -buildid=' -o $(BIN)/$$c ./cmd/$$c || exit 1; \
	done

# --------------------------------------------------------------- images

## build: binaries + local UBI container images
build: bin images

## images: build UBI container images (native arch unless PLATFORMS set)
images:
	@for c in $(COMPONENTS); do \
		echo "==> image $$c"; \
		if [ -n "$(PLATFORMS)" ]; then \
			IFS=',' read -r -a plats <<< "$(PLATFORMS)"; \
			tmp="$(IMAGE)/$$c:__mp$$_"; \
			$(PODMAN) manifest rm "$$tmp" >/dev/null 2>&1 || true; \
			$(PODMAN) manifest create "$$tmp"; \
			for p in "$${plats[@]}"; do \
				$(PODMAN) build -q --platform "$$p" -t "$$tmp-\$$p" -f $$c.Containerfile . ; \
				$(PODMAN) manifest add "$$tmp" "$$tmp-\$$p" ; \
			done; \
			$(PODMAN) manifest push "$$tmp" "$(IMAGE)/$$c:$(TAG)"; \
			$(PODMAN) manifest rm "$$tmp" >/dev/null 2>&1 || true; \
		else \
			$(PODMAN) build -q -t "$(IMAGE)/$$c:$(TAG)" -f $$c.Containerfile . ; \
		fi; \
	done

# build a single component: make image COMPONENTS=upf
.PHONY: image
image:
	$(MAKE) images COMPONENTS=$(COMPONENTS)

# ----------------------------------------------------------------- push

## login: podman login to the image registry (QUAY_USERNAME / QUAY_PASSWORD)
login:
	$(call check_env,QUAY_PASSWORD,login)
	@printf '%s' "$(QUAY_PASSWORD)" | $(PODMAN) login $(IMAGE_REGISTRY) \
		--authfile $(AUTH) -u "$(QUAY_USERNAME)" --password-stdin
	@echo "==> logged in to $(IMAGE_REGISTRY) as $(QUAY_USERNAME) (authfile: $(AUTH))"

## push: login + build (multi-arch aware) + push images to the registry
push:
	$(call check_env,QUAY_PASSWORD,push)
	@printf '%s' "$(QUAY_PASSWORD)" | $(PODMAN) login $(IMAGE_REGISTRY) \
		--authfile $(AUTH) -u "$(QUAY_USERNAME)" --password-stdin
	@set -e; trap 'rm -f $(AUTH)' EXIT
	@for c in $(COMPONENTS); do \
		echo "==> push $$c"; \
		if [ -n "$(PLATFORMS)" ]; then \
			IFS=',' read -r -a plats <<< "$(PLATFORMS)"; \
			tmp="$(IMAGE)/$$c:__mp$$_"; \
			$(PODMAN) manifest rm "$$tmp" >/dev/null 2>&1 || true; \
			$(PODMAN) manifest create "$$tmp"; \
			for p in "$${plats[@]}"; do \
				$(PODMAN) build -q --authfile $(AUTH) --platform "$$p" -t "$$tmp-\$$p" -f $$c.Containerfile . ; \
				$(PODMAN) manifest add "$$tmp" "$$tmp-\$$p" ; \
			done; \
			$(PODMAN) manifest push --authfile $(AUTH) "$$tmp" "$(IMAGE)/$$c:$(TAG)"; \
			$(PODMAN) manifest rm "$$tmp" >/dev/null 2>&1 || true; \
		else \
			$(PODMAN) build -q --authfile $(AUTH) -t "$(IMAGE)/$$c:$(TAG)" -f $$c.Containerfile . ; \
			$(PODMAN) push --authfile $(AUTH) "$(IMAGE)/$$c:$(TAG)"; \
		fi; \
	done
	@echo "==> pushed: $(COMPONENTS)"

# ------------------------------------------------------------ local stack

## test-local: build images then run the full stack with podman locally
test-local: images
	@echo "==> local network"
	@$(PODMAN) network exists $(LOCAL_NET) 2>/dev/null || $(PODMAN) network create $(LOCAL_NET)
	@echo "==> db (set DB_AUTHFILE if the RHEL9 image needs credentials)"
	@if ! $(PODMAN) image exists $(DB_IMAGE); then \
		echo "==> pulling $(DB_IMAGE)"; \
		$(PODMAN) pull --authfile $(DB_AUTHFILE) $(DB_IMAGE); \
	fi
	@$(PODMAN) rm -f upfgen-db >/dev/null 2>&1 || true
	@$(PODMAN) run -d --name upfgen-db --network $(LOCAL_NET) --network-alias db \
		-p $(LOCAL_DB) \
		-e POSTGRESQL_USER=$(DB_USER) -e POSTGRESQL_PASSWORD=$(DB_PASSWORD) \
		-e POSTGRESQL_DATABASE=$(DB_NAME) \
		$(DB_IMAGE)
	@echo "==> upf-0 / upf-1"
	@i=0; while [ $$i -lt 2 ]; do \
		$(PODMAN) rm -f upfgen-upf-$$i >/dev/null 2>&1 || true; \
		extra=""; if [ $$i -eq 0 ]; then extra="-p $(LOCAL_UPF_N3) -p $(LOCAL_UPF_N5)"; fi; \
		$(PODMAN) run -d --name upfgen-upf-$$i --network $(LOCAL_NET) --network-alias upf-$$i \
			$$extra \
			-e POD_NAME=upf-$$i -e UPF_NODES=2 -e UPF_PEER_DNS='upf-{n}' \
			-e UPF_SESSIONS=64 -e UPF_INSPECTION=true -e UPF_LI_SESSIONS=0-3 \
			-e UPF_CDR_IDLE=15s -e UPF_CDR_MIN=1s \
			$(IMAGE)/upf:$(TAG); \
		i=$$((i+1)); \
	done
	@echo "==> trafficgen-0"
	@$(PODMAN) rm -f upfgen-tg-0 >/dev/null 2>&1 || true
	@$(PODMAN) run -d --name upfgen-tg-0 --network $(LOCAL_NET) \
		--network-alias trafficgen-0 \
		-p $(LOCAL_TG) \
		-e POD_NAME=trafficgen-0 \
		-e UPF_PEER_DNS='upf-{n}' -e UPF_NODES=2 -e UPF_METRICS_PORT=8080 \
		-e TG_PPS=100 -e TG_THROTTLE=100 -e TG_MAX_SESSIONS=32 \
		$(IMAGE)/trafficgen:$(TAG)
	@echo "==> webui"
	@$(PODMAN) rm -f upfgen-webui >/dev/null 2>&1 || true
	@$(PODMAN) run -d --name upfgen-webui --network $(LOCAL_NET) \
		-p $(LOCAL_WEBUI) \
		-e UPF_ENDPOINTS=upf-0=http://upf-0:8080,upf-1=http://upf-1:8080 \
		-e TG_ENDPOINTS=tg-0=http://trafficgen-0:8080 \
		-e DB_DSN=postgres://$(DB_USER):$(DB_PASSWORD)@db:5432/$(DB_NAME)?sslmode=disable \
		$(IMAGE)/webui:$(TAG)
	@echo ""
	@echo "Local stack up:"
	@echo "  webui  http://127.0.0.1:3080"
	@echo "  tg     http://127.0.0.1:3081  (POST /api/throttle {\"pct\":N})"
	@echo "  upf N3 127.0.0.1:2152/udp   N5 127.0.0.1:2153/udp"
	@echo "  db     127.0.0.1:15432 ($(DB_USER)/$(DB_PASSWORD)/$(DB_NAME))"

## local-down: tear down the local podman stack
local-down:
	@for c in upfgen-webui upfgen-tg-0 upfgen-upf-1 upfgen-upf-0 upfgen-db; do \
		$(PODMAN) rm -f $$c >/dev/null 2>&1 || true; \
	done
	@$(PODMAN) network rm $(LOCAL_NET) >/dev/null 2>&1 || true
	@echo "==> local stack removed"

## local-logs: tail logs of the whole local stack
local-logs:
	@$(PODMAN) logs -f --tail 100 upfgen-db upfgen-upf-0 upfgen-upf-1 upfgen-tg-0 upfgen-webui

# --------------------------------------------------------------- cluster

## deploy: apply all OpenShift manifests to the cluster in $KUBECONFIG
deploy:
	$(call check_env,KUBECONFIG,deploy)
	@echo "==> $(OC) apply -f deploy/ (namespace $(NAMESPACE))"
	@$(OC) apply -f deploy/
	@echo ""
	@echo "Watching pods (Ctrl-C to detach; the apply is already complete):"
	@$(OC) -n $(NAMESPACE) get pods -w || true

## undeploy: remove the deployment manifests from the cluster
undeploy:
	$(call check_env,KUBECONFIG,undeploy)
	@$(OC) delete -f deploy/ --ignore-not-found
	@echo "==> deployment removed (namespace $(NAMESPACE))"

## pods: quick status of the live stack
pods:
	$(call check_env,KUBECONFIG,pods)
	@$(OC) -n $(NAMESPACE) get pods,svc,route -l app.kubernetes.io/part-of=telco-upf-gen

## route: print the dashboard route URL
route:
	$(call check_env,KUBECONFIG,route)
	@$(OC) -n $(NAMESPACE) get route webui -o jsonpath='{.spec.host}' && echo

# ------------------------------------------------------------------- e2e

## e2e: run the automated end-to-end suite against the live cluster
e2e: bin
	$(call check_env,KUBECONFIG,e2e)
	@echo "==> e2e (namespace $(NAMESPACE))"
	@NAMESPACE=$(NAMESPACE) E2E_DB_USER=$(DB_USER) E2E_DB_PASSWORD=$(DB_PASSWORD) \
		E2E_DB_NAME=$(DB_NAME) $(BIN)/e2e

# ----------------------------------------------------------------- clean

## clean: remove built binaries and the local authfile
clean:
	rm -rf $(BIN) $(AUTH)
	@echo "==> cleaned"
.PHONY: help lint tidy test fmt bin build images image login push test-local local-down local-logs deploy undeploy pods route e2e clean
