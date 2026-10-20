#!/usr/bin/env bash
# Deploy the prod stack on ganymede (ais.roythan.de). Pulls the checked-out
# branch, builds the images with the commit stamped in, recreates the stack
# (the one-shot migrator runs before the tracker starts), and waits for the
# tracker's healthcheck. Idempotent, so it is safe to re-run.
#
# Usage, from the checkout on the host (CI runs the same thing over SSH):
#   deploy/deploy.sh
#
# Expects the repo-root .env (POSTGRES_PASSWORD at least; see .env.example) and
# the shared traefik-public and monitoring networks from the infrastructure repo.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE=".env"
COMPOSE=(docker compose --env-file "$ENV_FILE" -f deploy/docker-compose.yml --profile prod)

if [[ ! -f "$ENV_FILE" ]]; then
  echo "$ENV_FILE not found: copy .env.example to .env and fill it in" >&2
  exit 1
fi
if [[ -z "$(grep -E '^POSTGRES_PASSWORD=' "$ENV_FILE" | head -1 | cut -d= -f2-)" ]]; then
  echo "POSTGRES_PASSWORD is not set in $ENV_FILE" >&2
  exit 1
fi
for net in traefik-public monitoring; do
  if ! docker network inspect "$net" > /dev/null 2>&1; then
    echo "network $net is missing: bring up the infrastructure stacks first" >&2
    exit 1
  fi
done

echo "-> git pull"
git pull --ff-only

# The tag is the version when there is one, otherwise the short SHA.
VERSION="$(git describe --tags --always)"
export VERSION
echo "-> build $VERSION"
"${COMPOSE[@]}" build

echo "-> up"
if ! "${COMPOSE[@]}" up -d --remove-orphans; then
  "${COMPOSE[@]}" logs --tail=50 migrate tracker >&2
  exit 1
fi

# The tracker image is distroless (no shell, no curl), so rely on the compose
# healthcheck, which runs `tracker healthcheck` inside the container.
echo "-> waiting for the tracker to report healthy"
container="$("${COMPOSE[@]}" ps -q tracker)"
for _ in $(seq 1 40); do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$container" 2> /dev/null || echo missing)"
  if [[ "$status" == "healthy" ]]; then
    echo "healthy"
    break
  fi
  sleep 3
done
if [[ "$status" != "healthy" ]]; then
  echo "tracker is $status after 120s" >&2
  "${COMPOSE[@]}" logs --tail=50 migrate tracker >&2
  exit 1
fi

docker image prune -f > /dev/null

echo "deployed $VERSION at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
