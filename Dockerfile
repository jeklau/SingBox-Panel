# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM node:24-alpine AS web
WORKDIR /src/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS agent
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends xz-utils curl ca-certificates && \
    curl -fsSL https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz \
      | tar -xJ -C /usr/local && \
    ln -sf /usr/local/zig-linux-x86_64-0.13.0/zig /usr/local/bin/zig && \
    cargo install --locked cargo-zigbuild && \
    rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
COPY agent/ ./agent/
ARG AGENT_VERSION=
RUN AGENT_VER="${AGENT_VERSION:-$(cat agent/VERSION)}" && \
    mkdir -p /out/agents && cp agent/VERSION /out/agents/VERSION && \
    cd agent && AGENT_VERSION="$AGENT_VER" cargo zigbuild --release --target x86_64-unknown-linux-musl && \
    AGENT_VERSION="$AGENT_VER" cargo zigbuild --release --target aarch64-unknown-linux-musl && \
    cp target/x86_64-unknown-linux-musl/release/singbox-panel-agent /out/agents/singbox-panel-agent-linux-amd64 && \
    cp target/aarch64-unknown-linux-musl/release/singbox-panel-agent /out/agents/singbox-panel-agent-linux-arm64 && \
    chmod 755 /out/agents/singbox-panel-agent-linux-*

FROM --platform=$BUILDPLATFORM golang:1.26.6-alpine AS backend
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/panel/ ./cmd/panel/
COPY internal/ ./internal/
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -buildvcs=false -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/singbox-panel ./cmd/panel

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates && \
    addgroup -g 10001 panel && adduser -D -H -u 10001 -G panel panel && \
    mkdir -p /app/data && chown 10001:10001 /app/data
WORKDIR /app
COPY --from=backend /out/singbox-panel /usr/local/bin/singbox-panel
COPY --from=agent /out/agents/ /app/agents/
COPY --from=web /src/web/dist/ /app/web/
ENV SINGBOX_PANEL_DEPLOYMENT=docker \
    SINGBOX_PANEL_LISTEN=:8080 \
    SINGBOX_PANEL_WEB_DIR=/app/web \
    SINGBOX_PANEL_AGENTS_DIR=/app/agents \
    SINGBOX_PANEL_DB_DSN=/app/data/singbox-panel.db
USER 10001:10001
VOLUME ["/app/data"]
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD wget -q -T 3 -O /dev/null http://127.0.0.1:8080/api/ready || exit 1
ENTRYPOINT ["/usr/local/bin/singbox-panel"]
