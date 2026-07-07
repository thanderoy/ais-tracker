# Postgres 16 image for ais-tracker.
#
# Base: timescale/timescaledb-ha:pg16 — Tiger Data's HA image, which already
# bundles TimescaleDB, PostGIS, and pgvector (the `vector` extension) plus
# wal2json for logical decoding. We layer only the first-boot init scripts on
# top. If a future base image drops one of these, install it here.
FROM timescale/timescaledb-ha:pg16

# Files in this directory run once, in filename order, the first time the
# server starts against an empty data directory.
COPY init/ /docker-entrypoint-initdb.d/
