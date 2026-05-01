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
DOLTGRES_DATABASE="${DOLTGRES_DATABASE:-postgres}"
PGDOG_DOLTGRES_HOST="${PGDOG_DOLTGRES_HOST:-host.docker.internal}"
SOURCE_CONTAINER="doltgres-pgdog-source-$$"
PGDOG_CONTAINER="doltgres-pgdog-migration-$$"
CUSTOMER_ID="${CUSTOMER_ID:-42}"

shard0_pid=""
shard1_pid=""
reverse_apply_pid=""

cleanup() {
  docker rm -f "$PGDOG_CONTAINER" "$SOURCE_CONTAINER" >/dev/null 2>&1 || true
  if [[ -n "$reverse_apply_pid" ]]; then
    kill "$reverse_apply_pid" >/dev/null 2>&1 || true
  fi
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
  PGCONNECT_TIMEOUT=2 PGPASSWORD=password psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$port" -U postgres -d "$DOLTGRES_DATABASE" "$@"
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
  dolt_transaction_commit: true
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
    DOLTGRES_DB="$DOLTGRES_DATABASE" \
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
database_name = "$DOLTGRES_DATABASE"
user = "postgres"
password = "password"
role = "primary"
shard = 0

[[databases]]
name = "pgdog"
host = "$PGDOG_DOLTGRES_HOST"
port = $DOLTGRES_SHARD1_PORT
database_name = "$DOLTGRES_DATABASE"
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

find_customer_shard_port() {
  local found=""
  for shard_port in "$DOLTGRES_SHARD0_PORT" "$DOLTGRES_SHARD1_PORT"; do
    count="$(psql_shard "$shard_port" -At -c "SELECT count(*) FROM customer_orders WHERE customer_id = $CUSTOMER_ID;")"
    if [[ "$count" != "0" ]]; then
      if [[ -n "$found" ]]; then
        echo "reverse-routing: customer $CUSTOMER_ID found on multiple shards: $found and $shard_port" >&2
        exit 1
      fi
      found="$shard_port"
    fi
  done
  if [[ -z "$found" ]]; then
    echo "reverse-routing: customer $CUSTOMER_ID was not found on any Doltgres shard" >&2
    exit 1
  fi
  echo "$found"
}

restart_doltgres_shard() {
  local shard_port="$1"
  if [[ "$shard_port" == "$DOLTGRES_SHARD0_PORT" ]]; then
    kill "$shard0_pid" >/dev/null 2>&1 || true
    wait "$shard0_pid" >/dev/null 2>&1 || true
    shard0_pid="$(start_doltgres_shard "$DOLTGRES_SHARD0_PORT" shard0)"
    wait_for_shard "$DOLTGRES_SHARD0_PORT" "$TMP_DIR/shard0.log"
  elif [[ "$shard_port" == "$DOLTGRES_SHARD1_PORT" ]]; then
    kill "$shard1_pid" >/dev/null 2>&1 || true
    wait "$shard1_pid" >/dev/null 2>&1 || true
    shard1_pid="$(start_doltgres_shard "$DOLTGRES_SHARD1_PORT" shard1)"
    wait_for_shard "$DOLTGRES_SHARD1_PORT" "$TMP_DIR/shard1.log"
  else
    echo "restart: unknown Doltgres shard port $shard_port" >&2
    exit 1
  fi
}

wait_for_pgdog_customer_route() {
  for _ in $(seq 1 30); do
    if psql_pgdog -c "SELECT count(*) FROM customer_orders WHERE customer_id = $CUSTOMER_ID;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "PgDog did not reconnect to restarted Doltgres shard" >&2
  docker logs "$PGDOG_CONTAINER" >&2 || true
  return 1
}

wait_for_reverse_slot_active() {
  local shard_port="$1"
  for _ in $(seq 1 30); do
    active="$(psql_shard "$shard_port" -At -c "SELECT active::text FROM pg_catalog.pg_replication_slots WHERE slot_name = 'dg_reverse_slot';")"
    if [[ "$active" == "true" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "reverse-apply: slot did not become active" >&2
  exit 1
}

start_reverse_apply() {
  local source_url="$1"
  local target_url="$2"
  local commits="$3"
  local shard_port="$4"

  (cd "$ROOT_DIR" && go run ./testing/pgdog/reverse_apply \
    -source-url "$source_url" \
    -target-url "$target_url" \
    -slot dg_reverse_slot \
    -publication dg_reverse_pub \
    -commits "$commits" \
    -timeout 45s) &
  reverse_apply_pid="$!"
  wait_for_reverse_slot_active "$shard_port"
}

wait_for_reverse_apply() {
  wait "$reverse_apply_pid"
  reverse_apply_pid=""
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

reverse_shard_port="$(find_customer_shard_port)"
reverse_source_url="postgres://postgres:password@127.0.0.1:$reverse_shard_port/$DOLTGRES_DATABASE?sslmode=disable"
reverse_target_url="postgres://postgres:password@127.0.0.1:$SOURCE_POSTGRES_PORT/pgdog?sslmode=disable"
psql_shard "$reverse_shard_port" -c "CREATE PUBLICATION dg_reverse_pub FOR TABLE customer_orders;"
(cd "$ROOT_DIR" && go run ./testing/pgdog/reverse_apply \
  -source-url "$reverse_source_url" \
  -target-url "$reverse_target_url" \
  -slot dg_reverse_slot \
  -publication dg_reverse_pub \
  -create-slot-only)

start_reverse_apply "$reverse_source_url" "$reverse_target_url" 1 "$reverse_shard_port"
psql_pgdog -c "INSERT INTO customer_orders VALUES ($CUSTOMER_ID, 4, 'cutover-write', 400, 'written through pgdog after cutover');"
cutover_result="$(psql_pgdog -At -F '|' -c "SELECT status, amount, note FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 4;")"
if [[ "$cutover_result" != "cutover-write|400|written through pgdog after cutover" ]]; then
  echo "cutover-routing: expected PgDog cutover write, got: $cutover_result" >&2
  exit 1
fi

wait_for_reverse_apply
source_cutover_result="$(psql_source -At -F '|' -c "SELECT status, amount, note FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 4;")"
if [[ "$source_cutover_result" != "cutover-write|400|written through pgdog after cutover" ]]; then
  echo "reverse-apply: expected first cutover write on rollback source, got: $source_cutover_result" >&2
  exit 1
fi

restart_doltgres_shard "$reverse_shard_port"
wait_for_pgdog_customer_route

start_reverse_apply "$reverse_source_url" "$reverse_target_url" 2 "$reverse_shard_port"
psql_pgdog -c "UPDATE customer_orders SET status = 'reverse-updated', amount = 450, note = 'reverse update after restart' WHERE customer_id = $CUSTOMER_ID AND order_id = 4;"
psql_pgdog -c "DELETE FROM customer_orders WHERE customer_id = $CUSTOMER_ID AND order_id = 1;"
wait_for_reverse_apply
assert_customer_matches_source

echo "PgDog customer migration harness passed for customer_id=$CUSTOMER_ID"
