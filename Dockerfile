# syntax=docker/dockerfile:1

# ── build ────────────────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS build

WORKDIR /src

# Module files first, so dependency resolution caches independently of the source.
# The glob is deliberate: kyria has no external dependencies, so there is no go.sum yet, and
# `go.*` matches go.mod today and picks up go.sum automatically once one exists.
COPY go.* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# CGO_ENABLED=0 makes a fully static binary, which is what lets the final stage be scratch.
# -s -w drop the symbol table and DWARF info; the cache mounts make rebuilds fast.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kyria ./cmd/kyria

# ── runtime ──────────────────────────────────────────────────────────────────
FROM scratch

COPY --from=build /out/kyria /kyria

# Numeric UID: scratch has no /etc/passwd for a name to resolve against. 65534 is nobody.
USER 65534:65534

# Documentation only — publishing is up to the run command. 6379 is the RESP client port;
# 7946 is the conventional gossip port (UDP), used by docker-compose.yml.
EXPOSE 6379/tcp
EXPOSE 7946/udp

# ENTRYPOINT rather than CMD so flags pass straight through:
#   docker run kyria -shards 64
# Configuration is also available as KYRIA_* environment variables, which is usually the
# better fit for a container:
#   docker run -e KYRIA_SHARDS=64 kyria
# Precedence is flag > env > default.
#
# NB for clustering: -addr and -gossip-addr are advertised to peers, so each container must
# pass a routable host (its compose service name), not the default port-only form. kyria
# rejects a wildcard or port-only value at startup when -gossip-addr is set.
ENTRYPOINT ["/kyria"]
