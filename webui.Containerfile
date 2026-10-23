# Web UI — dashboard + aggregation/bridge (the only DB writer; templates
# embedded at build time). UBI9 multi-stage build (dnf only), non-root.
FROM registry.access.redhat.com/ubi9/ubi:latest AS build

ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src

# Toolchain exclusively via dnf (Red Hat go-toolset). No other package
# manager, no upstream Go tarball.
RUN dnf -y install golang && dnf clean all && go version

COPY go.mod go.sum ./
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags '-s -w -buildid=' -o /out/app ./cmd/webui

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
LABEL name="telco-upf-gen-webui" \
      io.k8s.description="5G UPF fabric dashboard (RFC 2544, CDR/LI, knob) + persistence bridge" \
      io.k8s.display-name="telco-upf-gen web UI"
COPY --from=build /out/app /usr/local/bin/webui
USER 1001:1001
EXPOSE 8080/tcp
ENTRYPOINT ["/usr/local/bin/webui"]
