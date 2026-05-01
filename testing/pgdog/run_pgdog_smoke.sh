#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"

PGDOG_IMAGE="${PGDOG_IMAGE:-ghcr.io/pgdogdev/pgdog:latest}"
PGDOG_PORT="${PGDOG_PORT:-16432}"
DOLTGRES_SHARD0_PORT="${DOLTGRES_SHARD0_PORT:-15432}"
DOLTGRES_SHARD1_PORT="${DOLTGRES_SHARD1_PORT:-15433}"
PGDOG_DOLTGRES_HOST="${PGDOG_DOLTGRES_HOST:-host.docker.internal}"
PGDOG_CONTAINER="doltgres-pgdog-smoke-$$"

shard0_pid=""
shard1_pid=""

cleanup() {
  docker rm -f "$PGDOG_CONTAINER" >/dev/null 2>&1 || true
  if [[ -n "$shard0_pid" ]]; then
    kill "$shard0_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$shard1_pid" ]]; then
    kill "$shard1_pid" >/dev/null 2>&1 || true
  fi
  wait >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

psql_shard() {
  local port="$1"
  shift
  PGCONNECT_TIMEOUT=2 PGPASSWORD=password psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$port" -U postgres -d pgdog "$@"
}

psql_pgdog() {
  PGCONNECT_TIMEOUT=2 PGPASSWORD=password psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$PGDOG_PORT" -U postgres -d pgdog "$@"
}

write_doltgres_config() {
  local port="$1"
  local data_dir="$2"
  local config_file="$3"

  cat > "$config_file" <<EOF
log_level: warning
behavior:
  read_only: false
  dolt_transaction_commit: false
listener:
  host: 0.0.0.0
  port: $port
  read_timeout_millis: 28800000
  write_timeout_millis: 28800000
data_dir: $data_dir
cfg_dir: $data_dir/.doltcfg
auth_file: $data_dir/.doltcfg/auth.db
EOF
}

start_doltgres_shard() {
  local port="$1"
  local name="$2"
  local data_dir="$TMP_DIR/$name-data"
  local config_file="$TMP_DIR/$name-config.yaml"
  local log_file="$TMP_DIR/$name.log"

  mkdir -p "$data_dir"
  write_doltgres_config "$port" "$data_dir" "$config_file"

  DOLTGRES_USER=postgres \
    DOLTGRES_PASSWORD=password \
    DOLTGRES_DB=pgdog \
    "$DOLTGRES_BIN" --config "$config_file" >"$log_file" 2>&1 &
  echo "$!"
}

wait_for_shard() {
  local port="$1"
  local log_file="$2"

  for _ in $(seq 1 60); do
    if psql_shard "$port" -c "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Doltgres shard on port $port did not become ready" >&2
  sed -n '1,160p' "$log_file" >&2 || true
  return 1
}

write_pgdog_config() {
  mkdir -p "$TMP_DIR/pgdog"
  cat > "$TMP_DIR/pgdog/pgdog.toml" <<EOF
[general]
host = "0.0.0.0"
port = 6432
two_phase_commit = false
two_phase_commit_auto = false
prepared_statements = "extended"
read_write_split = "include_primary"
load_schema = "off"

[[databases]]
name = "pgdog"
host = "$PGDOG_DOLTGRES_HOST"
port = $DOLTGRES_SHARD0_PORT
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 0

[[databases]]
name = "pgdog"
host = "$PGDOG_DOLTGRES_HOST"
port = $DOLTGRES_SHARD1_PORT
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 1

[[sharded_tables]]
database = "pgdog"
name = "pgdog_items"
column = "tenant_id"
data_type = "bigint"
EOF

  cat > "$TMP_DIR/pgdog/users.toml" <<EOF
[[users]]
name = "postgres"
password = "password"
database = "pgdog"
server_user = "postgres"
server_password = "password"
EOF
}

wait_for_pgdog() {
  for _ in $(seq 1 60); do
    if psql_pgdog -c "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "PgDog did not become ready" >&2
  docker logs "$PGDOG_CONTAINER" >&2 || true
  return 1
}

expect_pgdog_failure() {
  local name="$1"
  local query="$2"
  local expected="$3"
  local log_file="$TMP_DIR/failure-$name.log"

  if psql_pgdog -c "$query" >"$log_file" 2>&1; then
    echo "expected PgDog query to fail: $name" >&2
    cat "$log_file" >&2
    return 1
  fi

  if ! grep -qi "$expected" "$log_file"; then
    echo "PgDog query failed with unexpected output: $name" >&2
    cat "$log_file" >&2
    return 1
  fi
}

if [[ -z "${DOLTGRES_BIN:-}" ]]; then
  if [[ -z "${CGO_CPPFLAGS:-}" ]] && command -v brew >/dev/null && brew --prefix icu4c@78 >/dev/null 2>&1; then
    icu_prefix="$(brew --prefix icu4c@78)"
    export CGO_CPPFLAGS="-I$icu_prefix/include"
    export CGO_LDFLAGS="-L$icu_prefix/lib"
    export PKG_CONFIG_PATH="$icu_prefix/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
  fi

  DOLTGRES_BIN="$TMP_DIR/doltgres"
  (cd "$ROOT_DIR" && go build -o "$DOLTGRES_BIN" ./cmd/doltgres)
fi

command -v docker >/dev/null
command -v psql >/dev/null

shard0_pid="$(start_doltgres_shard "$DOLTGRES_SHARD0_PORT" shard0)"
shard1_pid="$(start_doltgres_shard "$DOLTGRES_SHARD1_PORT" shard1)"

wait_for_shard "$DOLTGRES_SHARD0_PORT" "$TMP_DIR/shard0.log"
wait_for_shard "$DOLTGRES_SHARD1_PORT" "$TMP_DIR/shard1.log"

write_pgdog_config

docker run -d \
  --name "$PGDOG_CONTAINER" \
  --add-host=host.docker.internal:host-gateway \
  -p "127.0.0.1:$PGDOG_PORT:6432" \
  -v "$TMP_DIR/pgdog:/config:ro" \
  "$PGDOG_IMAGE" \
  pgdog --config /config/pgdog.toml --users /config/users.toml >/dev/null

wait_for_pgdog

psql_pgdog -c "CREATE TABLE pgdog_items (tenant_id BIGINT PRIMARY KEY, label TEXT);"
for tenant_id in $(seq 1 16); do
  psql_pgdog -c "INSERT INTO pgdog_items (tenant_id, label) VALUES ($tenant_id, 'tenant-$tenant_id');"
done

psql_pgdog -c "SELECT label FROM pgdog_items WHERE tenant_id = 3;" | grep -q "tenant-3"
psql_pgdog -c "SELECT count(*) FROM pgdog_items;" | grep -q "16"

shard0_count="$(psql_shard "$DOLTGRES_SHARD0_PORT" -At -c "SELECT count(*) FROM pgdog_items;")"
shard1_count="$(psql_shard "$DOLTGRES_SHARD1_PORT" -At -c "SELECT count(*) FROM pgdog_items;")"

if [[ "$shard0_count" -eq 0 || "$shard1_count" -eq 0 ]]; then
  echo "expected rows on both shards, got shard0=$shard0_count shard1=$shard1_count" >&2
  exit 1
fi

if [[ $((shard0_count + shard1_count)) -ne 16 ]]; then
  echo "expected 16 total rows, got shard0=$shard0_count shard1=$shard1_count" >&2
  exit 1
fi

expect_pgdog_failure "2pc" "PREPARE TRANSACTION 'dg_pgdog';" "syntax error"
expect_pgdog_failure "publication" "CREATE PUBLICATION dg_pgdog_pub FOR TABLE pgdog_items;" "unimplemented"
expect_pgdog_failure "copy-to" "COPY pgdog_items TO STDOUT;" "syntax error"
sql_prepare_result="$(psql_pgdog -At -c "PREPARE dg_pgdog_stmt(int) AS SELECT \$1::int + 1;" -c "EXECUTE dg_pgdog_stmt(41);")"
if ! grep -q "42" <<< "$sql_prepare_result"; then
  echo "sql-prepare: expected EXECUTE result 42, got: $sql_prepare_result" >&2
  exit 1
fi
expect_pgdog_failure "vector" "CREATE TABLE pgdog_vectors (tenant_id vector);" "vector"
expect_pgdog_failure "wal-lsn" "SELECT pg_current_wal_lsn();" "pg_current_wal_lsn"

echo "PgDog compatibility smoke passed: shard0=$shard0_count shard1=$shard1_count"
