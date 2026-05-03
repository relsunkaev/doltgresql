#!/usr/bin/env bash

set -euo pipefail

image="${1:-}"
if [ -z "$image" ]; then
  echo "usage: $0 <doltgres-image>" >&2
  exit 2
fi

password="${DOLTGRES_SMOKE_PASSWORD:-password}"
timeout="${DOLTGRES_SMOKE_TIMEOUT:-180}"
suffix="${RANDOM:-0}-$$"
volume="doltgres-smoke-${suffix}"
container_a="doltgres-smoke-a-${suffix}"
container_b="doltgres-smoke-b-${suffix}"

cleanup() {
  docker rm -f "$container_a" "$container_b" >/dev/null 2>&1 || true
  docker volume rm "$volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

run_psql() {
  local container="$1"
  shift

  docker run \
    --rm \
    --network "container:${container}" \
    --entrypoint psql \
    -e "PGPASSWORD=${password}" \
    "$image" \
    -X \
    -v ON_ERROR_STOP=1 \
    -h 127.0.0.1 \
    -U postgres \
    -d postgres \
    "$@"
}

wait_ready() {
  local container="$1"
  local start now

  start="$(date +%s)"
  while true; do
    if run_psql "$container" -c "select 1" >/dev/null 2>&1; then
      return 0
    fi

    if [ "$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)" != "true" ]; then
      docker logs "$container" >&2 || true
      echo "container exited before Doltgres became ready: $container" >&2
      exit 1
    fi

    now="$(date +%s)"
    if [ $((now - start)) -ge "$timeout" ]; then
      docker logs "$container" >&2 || true
      echo "timed out waiting for Doltgres readiness: $container" >&2
      exit 1
    fi

    sleep 1
  done
}

start_server() {
  local container="$1"

  docker run \
    --detach \
    --name "$container" \
    -e "DOLTGRES_PASSWORD=${password}" \
    -e "DOLTGRES_SERVER_TIMEOUT=${timeout}" \
    -v "${volume}:/var/lib/doltgres" \
    "$image" >/dev/null
}

need_cmd docker

docker run --rm --entrypoint doltgres "$image" --version
docker volume create "$volume" >/dev/null

start_server "$container_a"
wait_ready "$container_a"
run_psql "$container_a" -c "drop table if exists smoke_persistence"
run_psql "$container_a" -c "create table smoke_persistence (id int primary key)"
run_psql "$container_a" -c "insert into smoke_persistence values (1)"
docker rm -f "$container_a" >/dev/null

start_server "$container_b"
wait_ready "$container_b"
persisted="$(
  run_psql "$container_b" -At -c "select count(*) from smoke_persistence where id = 1" \
    | tr -d '[:space:]'
)"

if [ "$persisted" != "1" ]; then
  docker logs "$container_b" >&2 || true
  echo "expected persisted row count 1, got: $persisted" >&2
  exit 1
fi

echo "Doltgres Docker image smoke test passed: $image"
