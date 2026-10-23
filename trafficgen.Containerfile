# Traffic generator — token-bucket GTP-U flows against the UPF fabric,
# RFC 2544 benchmark runner, runnable in-cluster or off-cluster.
# UBI9 multi-stage build (dnf only), statically linked, non-root.
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
    go build -trimpath -ldflags '-s -w -buildid=' -o /out/app ./cmd/trafficgen

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
LABEL name="telco-upf-gen-trafficgen" \
      io.k8s.description="5G traffic generator with RFC 2544 benchmarking (GTP-U)" \
      io.k8s.display-name="telco-upf-gen traffic generator"
COPY --from=build /out/app /usr/local/bin/trafficgen
USER 1001:1001
EXPOSE 8080/tcp
ENTRYPOINT ["/usr/local/bin/trafficgen"]
