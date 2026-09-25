# syntax=docker/dockerfile:1
# ==============================================================================
# BERMUDA Stealth Gateway NG — Production Hardened Multi-Stage Dockerfile
#
# Stage 1 (builder)    : Pure static Go 1.24 build with strip flags
# Stage 2 (downloader) : Multi-arch pinned Xray-core v26 fetcher
# Stage 3 (runtime)    : Minimal Alpine, rootless UID 10001, immutable perms
# ==============================================================================

ARG GO_VERSION=1.24
ARG ALPINE_VERSION=3.21
ARG XRAY_VERSION=v26.9.9

# ------------------------------------------------------------------------------
# Stage 1 — Go Gateway Static Builder
# ------------------------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder

WORKDIR /src

# 1. Copy module manifests and source files
COPY go.mod ./
COPY *.go ./

# 2. Compile static, stripped gateway binary
RUN set -eux; \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -buildid=" \
        -o /out/bermuda-gateway .; \
    test -s /out/bermuda-gateway; \
    chmod 0555 /out/bermuda-gateway

# ------------------------------------------------------------------------------
# Stage 2 — Multi-Arch Official Xray-core Fetcher
# ------------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION} AS xray-downloader

ARG XRAY_VERSION
ARG TARGETARCH=amd64

RUN set -eux; \
    apk add --no-cache ca-certificates curl unzip; \
    case "${TARGETARCH}" in \
        amd64) XRAY_ARCH="64" ;; \
        arm64) XRAY_ARCH="arm64-v8a" ;; \
        *) echo "Unsupported target architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    XRAY_ZIP="Xray-linux-${XRAY_ARCH}.zip"; \
    XRAY_URL="https://github.com/XTLS/Xray-core/releases/download/${XRAY_VERSION}/${XRAY_ZIP}"; \
    echo "Downloading Xray-core ${XRAY_VERSION} for ${TARGETARCH} (${XRAY_ZIP})..."; \
    curl -fsSL --retry 5 --retry-delay 2 -o /tmp/xray.zip "${XRAY_URL}"; \
    mkdir -p /out/bin /out/assets; \
    unzip -q /tmp/xray.zip xray -d /out/bin; \
    unzip -q /tmp/xray.zip geoip.dat geosite.dat -d /out/assets; \
    chmod 0555 /out/bin/xray; \
    chmod 0444 /out/assets/*.dat; \
    /out/bin/xray version | head -n 2

# ------------------------------------------------------------------------------
# Stage 3 — Hardened Rootless Runtime
# ------------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

LABEL org.opencontainers.image.title="BERMUDA Stealth Gateway NG" \
      org.opencontainers.image.description="Railway VLESS XHTTP/WS Stealth Gateway with Supervised Xray-core" \
      org.opencontainers.image.licenses="MIT"

# 1. Install bare runtime dependencies and configure unprivileged user (UID 10001)
RUN set -eux; \
    apk add --no-cache ca-certificates tzdata; \
    update-ca-certificates; \
    addgroup -g 10001 -S bermuda; \
    adduser -u 10001 -S -D -H -G bermuda -h /app -s /sbin/nologin bermuda; \
    mkdir -p /app /usr/local/share/xray /usr/local/bin; \
    chown -R bermuda:bermuda /app /usr/local/share/xray

# 2. Copy artifacts from builder and downloader stages with strict ownership
COPY --from=builder --chown=bermuda:bermuda /out/bermuda-gateway /usr/local/bin/bermuda-gateway
COPY --from=xray-downloader --chown=bermuda:bermuda /out/bin/xray /usr/local/bin/xray
COPY --from=xray-downloader --chown=bermuda:bermuda /out/assets/geoip.dat /usr/local/share/xray/geoip.dat
COPY --from=xray-downloader --chown=bermuda:bermuda /out/assets/geosite.dat /usr/local/share/xray/geosite.dat
COPY --chown=bermuda:bermuda config.json /app/config.json

# 3. Apply immutable file permissions:
#    - Binaries: read + execute only (0555)
#    - Configurations & Routing Databases: read only (0444)
RUN set -eux; \
    chmod 0555 /usr/local/bin/bermuda-gateway /usr/local/bin/xray; \
    chmod 0444 /app/config.json /usr/local/share/xray/geoip.dat /usr/local/share/xray/geosite.dat; \
    test -s /usr/local/bin/bermuda-gateway; \
    test -s /usr/local/bin/xray; \
    test -s /app/config.json; \
    test -s /usr/local/share/xray/geoip.dat; \
    test -s /usr/local/share/xray/geosite.dat

# 4. Standard runtime environment variables tuned for 2 vCPU & 1 GB RAM
ENV XRAY_LOCATION_ASSET=/usr/local/share/xray \
    BERMUDA_XRAY_BIN=/usr/local/bin/xray \
    BERMUDA_XRAY_CONFIG=/app/config.json \
    BERMUDA_BACKEND_XH=127.0.0.1:18443 \
    BERMUDA_BACKEND_WS=127.0.0.1:18444 \
    BERMUDA_PATH_XH=/bermuda-xhttp \
    BERMUDA_PATH_WS=/bermuda-ws \
    GODEBUG=madvdontneed=1 \
    GOGC=50 \
    TZ=UTC \
    PORT=8080

USER bermuda:bermuda
WORKDIR /app

# Platform dynamic port expose fallback
EXPOSE 8080

# Graceful termination signal mapping for Railway orchestration
STOPSIGNAL SIGTERM

# Container-level active healthcheck probe
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -q -T 3 -O /dev/null "http://127.0.0.1:${PORT:-8080}/healthz" || exit 1

# PID 1 Process: The Go Gateway acts as the init supervisor
CMD ["/usr/local/bin/bermuda-gateway"]
