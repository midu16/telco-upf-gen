# Traffic generator — token-bucket GTP-U flows against the UPF fabric.
ARG GO_VERSION=1.27

FROM golang:${GO_VERSION} AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags '-s -w -buildid=' -o /out/app ./cmd/trafficgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /usr/local/bin/app
USER 10001:10001
EXPOSE 8080/tcp
ENTRYPOINT ["/usr/local/bin/app"]
