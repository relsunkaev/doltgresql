#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:18-alpine}"
POSTGRES_PORT="${POSTGRES_PORT:-15438}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-doltgres-indexperf-postgres}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-password}"
BENCH_ITERS="${DOLTGRES_PAIRED_INDEX_BENCH_ITERS:-25}"

cleanup() {
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
docker run -d \
  --name "$POSTGRES_CONTAINER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  -p "127.0.0.1:$POSTGRES_PORT:5432" \
  "$POSTGRES_IMAGE" >/dev/null

until docker exec "$POSTGRES_CONTAINER" pg_isready -U postgres -d "$POSTGRES_DB" >/dev/null 2>&1; do
  sleep 0.2
done

export DOLTGRES_POSTGRES_BASELINE_URL="postgres://postgres:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}?sslmode=disable"
export DOLTGRES_PAIRED_INDEX_BENCH_ITERS="$BENCH_ITERS"

if [[ -z "${CGO_CPPFLAGS:-}" && -d /opt/homebrew/opt/icu4c@78/include ]]; then
  export CGO_CPPFLAGS="-I/opt/homebrew/opt/icu4c@78/include"
fi
if [[ -z "${CGO_CFLAGS:-}" && -d /opt/homebrew/opt/icu4c@78/include ]]; then
  export CGO_CFLAGS="-I/opt/homebrew/opt/icu4c@78/include"
fi
if [[ -z "${CGO_CXXFLAGS:-}" && -d /opt/homebrew/opt/icu4c@78/include ]]; then
  export CGO_CXXFLAGS="-I/opt/homebrew/opt/icu4c@78/include"
fi
if [[ -z "${CGO_LDFLAGS:-}" && -d /opt/homebrew/opt/icu4c@78/lib ]]; then
  export CGO_LDFLAGS="-L/opt/homebrew/opt/icu4c@78/lib"
fi
if [[ -z "${PKG_CONFIG_PATH:-}" && -d /opt/homebrew/opt/icu4c@78/lib/pkgconfig ]]; then
  export PKG_CONFIG_PATH="/opt/homebrew/opt/icu4c@78/lib/pkgconfig"
fi

cd "$ROOT_DIR"
go test ./testing/go \
  -run '^$' \
  -bench '^BenchmarkPairedIndexBaselines$' \
  -benchtime=1x \
  -count=1 \
  -v
