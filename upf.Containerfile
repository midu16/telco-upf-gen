# UPF core — distributed 5G user-plane simulator node.
# Multi-arch (BuildKit TARGETOS/TARGETARCH), static binary, non-root, ~6 MB image.
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION} AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
# no external deps: copy sources directly, skip module cache step
COPY go.mod ./
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags '-s -w -buildid=' -o /out/app ./cmd/upf

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /usr/local/bin/app
# uid/gid 10001 keeps us clear of distro defaults
USER 10001:10001
EXPOSE 2152/udp 2153/udp 2154/udp 8080/tcp
ENTRYPOINT ["/usr/local/bin/app"]
