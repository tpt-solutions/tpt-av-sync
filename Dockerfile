# tpt-av-sync-server tooling: relay + signaling servers, via tpt-av-sync-cli.
#
# Build:
#   docker build -t tpt-av-sync-server .
#
# Run a relay (persisting to /data) or a signaling server:
#   docker run -p 7000:7000 -v tpt-av-sync-data:/data tpt-av-sync-server \
#       relay 0.0.0.0:7000 /data
#   docker run -p 7001:7001 tpt-av-sync-server signaling 0.0.0.0:7001
#
# See docker-compose.yml for running both together.

FROM rust:1-slim-bookworm AS build
WORKDIR /build

# Cargo.lock is not committed (see .gitignore) — cargo resolves and writes
# a fresh one here, the same as any other from-scratch build. .dockerignore
# keeps target/ and other local-only files out of the build context.
COPY . .

# tpt-av-sync-crdt's dev-dependencies include a path dependency on a
# sibling repo (../../tpt-av-test) used for shared fuzz-harness tests on
# the author's machine; it isn't part of this repo and can't be in the
# build context. Cargo resolves the whole workspace graph (incl.
# dev-dependencies of every member) even to build one binary, so that path
# must at least exist and parse. It is dev-only and irrelevant to building
# tpt-av-sync-cli, so drop it here rather than requiring it in every build
# environment.
RUN sed -i '/tpt-av-test-fuzz/d' tpt-av-sync-crdt/Cargo.toml && \
    cargo build --release -p tpt-av-sync-cli --bin tpt-av-sync

FROM debian:bookworm-slim AS runtime
RUN useradd --system --create-home --home-dir /data tpt-av-sync
COPY --from=build /build/target/release/tpt-av-sync /usr/local/bin/tpt-av-sync
USER tpt-av-sync
WORKDIR /data
ENTRYPOINT ["tpt-av-sync"]
CMD ["relay", "0.0.0.0:7000", "/data"]
