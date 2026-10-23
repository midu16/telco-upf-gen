# UPF core — distributed 5G user-plane simulator node.
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
    go build -trimpath -ldflags '-s -w -buildid=' -o /out/app ./cmd/upf

FROM registry.access.redhat.com/ubi9/ubi-minimal:latest
LABEL name="telco-upf-gen-upf" \
      io.k8s.description="Distributed 5G UPF simulator node (GTP-U N3/N5/N9, SPI/DPI, CDR/LI)" \
      io.k8s.display-name="telco-upf-gen UPF"
COPY --from=build /out/app /usr/local/bin/upf
# uid/gid 1001: run as non-root (SCC anyuid only needed for raw UDP sockets
# under privileged ports; the UPF binds high ports, so restricted-v2 works)
USER 1001:1001
EXPOSE 2152/udp 2153/udp 2154/udp 8080/tcp
ENTRYPOINT ["/usr/local/bin/upf"]
