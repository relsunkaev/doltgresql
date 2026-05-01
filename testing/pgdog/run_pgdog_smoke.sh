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

[[sharded_tables]]
database = "pgdog"
name = "pgdog_vectors"
column = "tenant_id"
data_type = "vector"
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

psql_pgdog -c "BEGIN;" -c "INSERT INTO pgdog_items VALUES (100, 'tenant-100');" -c "PREPARE TRANSACTION 'dg_pgdog';"
prepared_gid="$(psql_pgdog -At -c "SELECT gid FROM pg_catalog.pg_prepared_xacts WHERE gid = 'dg_pgdog';")"
if [[ "$prepared_gid" != "dg_pgdog" ]]; then
  echo "2pc: expected prepared transaction catalog row, got: $prepared_gid" >&2
  exit 1
fi
psql_pgdog -c "COMMIT PREPARED 'dg_pgdog';"
prepared_result="$(psql_pgdog -At -c "SELECT label FROM pgdog_items WHERE tenant_id = 100;")"
if [[ "$prepared_result" != "tenant-100" ]]; then
  echo "2pc: expected COMMIT PREPARED row tenant-100, got: $prepared_result" >&2
  exit 1
fi
psql_pgdog -c "CREATE PUBLICATION dg_pgdog_pub FOR TABLE pgdog_items;"
publication_result="$(psql_pgdog -At -c "SELECT pubname FROM pg_catalog.pg_publication WHERE pubname = 'dg_pgdog_pub';")"
if [[ "$publication_result" != "dg_pgdog_pub" ]]; then
  echo "publication: expected dg_pgdog_pub catalog row, got: $publication_result" >&2
  exit 1
fi
psql_pgdog -c "CREATE SUBSCRIPTION dg_pgdog_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pgdog_pub WITH (connect=false, enabled=false, create_slot=false, slot_name=NONE);"
subscription_result="$(psql_pgdog -At -c "SELECT subname, subenabled, subslotname IS NULL, array_to_string(subpublications, ',') FROM pg_catalog.pg_subscription WHERE subname = 'dg_pgdog_sub';")"
if [[ "$subscription_result" != "dg_pgdog_sub|f|t|dg_pgdog_pub" ]]; then
  echo "subscription: expected metadata-only subscription row, got: $subscription_result" >&2
  exit 1
fi
expect_pgdog_failure "subscription-connect" "CREATE SUBSCRIPTION dg_pgdog_bad_sub CONNECTION 'host=127.0.0.1 dbname=postgres' PUBLICATION dg_pgdog_pub;" "connect=false"
copy_to_result="$(psql_pgdog -At -c "COPY pgdog_items TO STDOUT;")"
if ! grep -q $'3\ttenant-3' <<< "$copy_to_result"; then
  echo "copy-to: expected copied row for tenant 3, got: $copy_to_result" >&2
  exit 1
fi
sql_prepare_result="$(psql_pgdog -At -c "PREPARE dg_pgdog_stmt(int) AS SELECT \$1::int + 1;" -c "EXECUTE dg_pgdog_stmt(41);")"
if ! grep -q "42" <<< "$sql_prepare_result"; then
  echo "sql-prepare: expected EXECUTE result 42, got: $sql_prepare_result" >&2
  exit 1
fi
psql_pgdog -c "CREATE TABLE pgdog_vectors (tenant_id vector PRIMARY KEY, label TEXT);"
psql_pgdog -c "INSERT INTO pgdog_vectors (tenant_id, label) VALUES ('[1,0]'::vector, 'vector-1');"
psql_pgdog -c "INSERT INTO pgdog_vectors (tenant_id, label) VALUES ('[2,0]'::vector, 'vector-2');"
vector_result="$(psql_pgdog -At -c "SELECT label FROM pgdog_vectors WHERE tenant_id = '[1,0]'::vector;")"
if [[ "$vector_result" != "vector-1" ]]; then
  echo "vector: expected routed vector shard-key lookup to return vector-1, got: $vector_result" >&2
  exit 1
fi
wal_lsn_result="$(psql_pgdog -At -c "SELECT pg_current_wal_lsn(), pg_wal_lsn_diff('0/1'::pg_lsn, '0/0'::pg_lsn);")"
if [[ "$wal_lsn_result" != "0/0|1" ]]; then
  echo "wal-lsn: expected 0/0|1, got: $wal_lsn_result" >&2
  exit 1
fi

echo "PgDog compatibility smoke passed: shard0=$shard0_count shard1=$shard1_count"
