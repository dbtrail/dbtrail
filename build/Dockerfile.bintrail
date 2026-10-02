# ── Stage 1: build ─────────────────────────────────────────
# Debian-based image required — DuckDB's pre-compiled static libs need glibc.
FROM golang:1.25.11-bookworm AS builder

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src

# Cache dependency downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build \
    -ldflags="-s -w -X main.Version=${VERSION} -X main.CommitSHA=${COMMIT} -X main.BuildDate=${BUILD_DATE}" \
    -o /bintrail ./cmd/bintrail

RUN CGO_ENABLED=1 go build \
    -ldflags="-s -w -X main.mcpVersion=${VERSION}" \
    -o /bintrail-mcp ./cmd/bintrail-mcp

# ── Stage 2: runtime ───────────────────────────────────────
# Debian slim for glibc compatibility with DuckDB.
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    # Pin uid 999: the bundled compose chowns the index password secret to
    # uid 999 (mode 0600) for this process to read — see docker-compose.yml.
    useradd --system --no-create-home --uid 999 bintrail && \
    # Writable state dir (console server registry, etc.). Pre-created and
    # chowned in the image so a named volume mounted here inherits the
    # ownership — the container runs as the non-root bintrail user.
    mkdir -p /var/lib/bintrail && chown bintrail /var/lib/bintrail && \
    # Home dir for the bintrail user: DuckDB caches its httpfs/aws extensions
    # under $HOME/.duckdb for S3 baseline/reconstruct/archive reads and fails
    # INSTALL without one. (The duckdbutil loader pins a fallback if $HOME is missing.)
    mkdir -p /home/bintrail && chown bintrail /home/bintrail

COPY --from=builder /bintrail /usr/local/bin/bintrail
COPY --from=builder /bintrail-mcp /usr/local/bin/bintrail-mcp

# License-compliance notices (#428): our license + retained upstream notices
# for the statically-linked MIT/MPL-2.0/BSD dependencies (DuckDB et al.).
COPY --from=builder /src/LICENSE /src/NOTICE /src/THIRD-PARTY-NOTICES /usr/share/doc/bintrail/

USER bintrail
ENTRYPOINT ["bintrail"]
