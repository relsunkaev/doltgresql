#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"

POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16-alpine}"
PGDOG_IMAGE="${PGDOG_IMAGE:-ghcr.io/pgdogdev/pgdog:latest}"
PGDOG_PORT="${PGDOG_PORT:-16433}"
SOURCE_POSTGRES_PORT="${SOURCE_POSTGRES_PORT:-15431}"
DOLTGRES_SHARD0_PORT="${DOLTGRES_SHARD0_PORT:-15435}"
DOLTGRES_SHARD1_PORT="${DOLTGRES_SHARD1_PORT:-15436}"
PGDOG_DOLTGRES_HOST="${PGDOG_DOLTGRES_HOST:-host.docker.internal}"
SOURCE_CONTAINER="doltgres-pgdog-source-$$"
PGDOG_CONTAINER="doltgres-pgdog-migration-$$"
CUSTOMER_ID="${CUSTOMER_ID:-42}"

shard0_pid=""
shard1_pid=""

cleanup() {
  docker rm -f "$PGDOG_CONTAINER" "$SOURCE_CONTAINER" >/dev/null 2>&1 || true
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

psql_source() {
  PGCONNECT_TIMEOUT=2 PGPASSWORD=password psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$SOURCE_POSTGRES_PORT" -U postgres -d pgdog "$@"
}

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

wait_for_source() {
  for _ in $(seq 1 60); do
    if psql_source -c "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "source Postgres did not become ready" >&2
  docker logs "$SOURCE_CONTAINER" >&2 || true
  return 1
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
prepared_statements = "extended"
read_write_split = "include_primary"
load_schema = "on"

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
name = "customer_orders"
column = "customer_id"
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

customer_rows() {
  local runner="$1"
  "$runner" -At -F '|' -c "SELECT customer_id, order_id, status, amount, COALESCE(note, '') FROM customer_orders WHERE customer_id = $CUSTOMER_ID ORDER BY order_id;"
}

customer_checksum() {
  local runner="$1"
  "$runner" -At -F '|' -c "SELECT count(*), COALESCE(sum(amount), 0), COALESCE(sum(length(COALESCE(note, ''))), 0) FROM customer_orders WHERE customer_id = $CUSTOMER_ID;"
}

assert_customer_matches_source() {
  local source_rows
  local destination_rows
  local source_checksum
  local destination_checksum

  source_rows="$(customer_rows psql_source)"
  destination_rows="$(customer_rows psql_pgdog)"
  if [[ "$source_rows" != "$destination_rows" ]]; then
    echo "customer validation: source and PgDog rows diverged" >&2
    echo "source:" >&2
    echo "$source_rows" >&2
    echo "destination:" >&2
    echo "$destination_rows" >&2
    exit 1
  fi

  source_checksum="$(customer_checksum psql_source)"
  destination_checksum="$(customer_checksum psql_pgdog)"
  if [[ "$source_checksum" != "$destination_checksum" ]]; then
    echo "customer validation: checksum mismatch source=$source_checksum destination=$destination_checksum" >&2
    exit 1
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

docker run -d \
  --name "$SOURCE_CONTAINER" \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=pgdog \
  -p "127.0.0.1:$SOURCE_POSTGRES_PORT:5432" \
  "$POSTGRES_IMAGE" \
  -c wal_level=logical \
  -c max_replication_slots=16 \
  -c max_wal_senders=16 >/dev/null

shard0_pid="$(start_doltgres_shard "$DOLTGRES_SHARD0_PORT" shard0)"
shard1_pid="$(start_doltgres_shard "$DOLTGRES_SHARD1_PORT" shard1)"

wait_for_source
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

psql_source -c "CREATE TABLE shared_accounts (id INT PRIMARY KEY, label TEXT NOT NULL);"
psql_source -c "CREATE TABLE customer_orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, status TEXT NOT NULL, amount INT NOT NULL, note TEXT, PRIMARY KEY (customer_id, order_id));"
psql_source -c "INSERT INTO shared_accounts VALUES (1, 'source-shared');"
psql_source -c "INSERT INTO customer_orders VALUES ($CUSTOMER_ID, 1, 'open', 100, 'copied-one'), ($CUSTOMER_ID, 2, 'open', 200, 'copied-two'), (7, 1, 'other', 700, 'not-migrated');"

psql_pgdog -c "CREATE TABLE customer_orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, status TEXT NOT NULL, amount INT NOT NULL, note TEXT, PRIMARY KEY (customer_id, order_id));"
for shard_port in "$DOLTGRES_SHARD0_PORT" "$DOLTGRES_SHARD1_PORT"; do
  table_count="$(psql_shard "$shard_port" -At -c "SELECT count(*) FROM information_schema.tables WHERE table_name = 'customer_orders';")"
  if [[ "$table_count" != "1" ]]; then
    echo "schema-sync: expected customer_orders on shard $shard_port, got count=$table_count" >&2
    exit 1
  fi
done

shared_result="$(psql_source -At -c "SELECT label FROM shared_accounts WHERE id = 1;")"
if [[ "$shared_result" != "source-shared" ]]; then
  echo "shared-routing: expected shared table on source endpoint, got: $shared_result" >&2
  exit 1
fi
expect_pgdog_failure "shared-table-read" "SELECT label FROM shared_accounts WHERE id = 1;" "shared_accounts"

psql_source -c "COPY (SELECT customer_id, order_id, status, amount, note FROM customer_orders WHERE customer_id = $CUSTOMER_ID ORDER BY order_id) TO STDOUT;" |
  psql_pgdog -c "COPY customer_orders (customer_id, order_id, status, amount, note) FROM STDIN;"

copied_count="$(psql_pgdog -At -c "SELECT count(*) FROM customer_orders WHERE customer_id = $CUSTOMER_ID;")"
if [[ "$copied_count" != "2" ]]; then
  echo "initial-copy: expected 2 copied rows for customer $CUSTOMER_ID, got: $copied_count" >&2
  exit 1
fi
other_count="$(psql_pgdog -At -c "SELECT count(*) FROM customer_orders WHERE customer_id = 7;")"
if [[ "$other_count" != "0" ]]; then
  echo "initial-copy: expected non-migrated customer rows to stay out of PgDog, got: $other_count" >&2
  exit 1
fi

psql_source -c "INSERT INTO customer_orders VALUES ($CUSTOMER_ID, 3, 'streamed-insert', 300, 'stream insert');"
psql_source -c "UPDATE customer_orders SET status = 'streamed-update', amount = 125, note = 'stream update' WHERE customer_id = $CUSTOMER_ID AND order_id = 1;"
psql_source -c "DELETE FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;"
psql_source -c "INSERT INTO customer_orders VALUES (7, 2, 'other-stream', 701, 'not migrated either');"

psql_pgdog -c "INSERT INTO customer_orders VALUES ($CUSTOMER_ID, 3, 'streamed-insert', 300, 'stream insert');"
psql_pgdog -c "UPDATE customer_orders SET status = 'streamed-update', amount = 125, note = 'stream update' WHERE customer_id = $CUSTOMER_ID AND order_id = 1;"
psql_pgdog -c "DELETE FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;"

assert_customer_matches_source

psql_pgdog -c "INSERT INTO customer_orders VALUES ($CUSTOMER_ID, 4, 'cutover-write', 400, 'written through pgdog after cutover');"
cutover_result="$(psql_pgdog -At -F '|' -c "SELECT status, amount, note FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 4;")"
if [[ "$cutover_result" != "cutover-write|400|written through pgdog after cutover" ]]; then
  echo "cutover-routing: expected PgDog cutover write, got: $cutover_result" >&2
  exit 1
fi
source_cutover_count="$(psql_source -At -c "SELECT count(*) FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 4;")"
if [[ "$source_cutover_count" != "0" ]]; then
  echo "cutover-routing: source should not receive post-cutover PgDog write without reverse replication, got count: $source_cutover_count" >&2
  exit 1
fi

echo "PgDog customer migration harness passed for customer_id=$CUSTOMER_ID"

