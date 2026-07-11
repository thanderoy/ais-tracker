#!/usr/bin/env bash
# Nightly Postgres backup for the ais-tracker stack. Runs pg_dump inside the
# compose `postgres` service and writes a timestamped, compressed custom-format
# dump to $BACKUP_DIR (a bind mount or synced to object storage out of band).
#
# Restore with:
#   pg_restore --clean --if-exists -d "$DATABASE_URL" ais-YYYYmmddTHHMMSSZ.dump
#
# Usage: deploy/backup.sh [retention_days]
#   BACKUP_DIR   destination directory (default ./backups)
#   COMPOSE_FILE compose file to target (default deploy/docker-compose.yml)
#   PG_SERVICE   compose service name (default postgres)
#   POSTGRES_USER / POSTGRES_DB  credentials (default ais / ais)
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.yml}"
PG_SERVICE="${PG_SERVICE:-postgres}"
PGUSER="${POSTGRES_USER:-ais}"
PGDB="${POSTGRES_DB:-ais}"
RETENTION_DAYS="${1:-14}"

mkdir -p "$BACKUP_DIR"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$BACKUP_DIR/ais-$stamp.dump"

echo "backing up $PGDB -> $out"
docker compose -f "$COMPOSE_FILE" exec -T "$PG_SERVICE" \
  pg_dump --format=custom --no-owner --username "$PGUSER" "$PGDB" > "$out"

# Fail loudly on an empty/truncated dump rather than silently keeping junk.
if [[ ! -s "$out" ]]; then
  echo "backup produced an empty file; removing" >&2
  rm -f "$out"
  exit 1
fi

echo "wrote $(du -h "$out" | cut -f1)"

# Prune dumps older than the retention window.
find "$BACKUP_DIR" -name 'ais-*.dump' -type f -mtime "+$RETENTION_DAYS" -print -delete
