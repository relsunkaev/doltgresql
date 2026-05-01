#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP_DIR="$(mktemp -d)"

POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16-alpine}"
PGDOG_IMAGE="${PGDOG_IMAGE:-ghcr.io/pgdogdev/pgdog:latest}"
PGDOG_PORT="${PGDOG_PORT:-16434}"
SOURCE_POSTGRES_PORT="${SOURCE_POSTGRES_PORT:-15437}"
DOLTGRES_SHARD1_PORT="${DOLTGRES_SHARD1_PORT:-15438}"
DOLTGRES_DATABASE="${DOLTGRES_DATABASE:-postgres}"
PGDOG_BACKEND_HOST="${PGDOG_BACKEND_HOST:-host.docker.internal}"
SOURCE_CONTAINER="doltgres-pgdog-schema-source-$$"
PGDOG_CONTAINER="doltgres-pgdog-schema-$$"
CUSTOMER_ID="${CUSTOMER_ID:-42}"
UNMIGRATED_CUSTOMER_ID="${UNMIGRATED_CUSTOMER_ID:-7}"

shard1_pid=""
reverse_apply_pid=""

cleanup() {
  docker rm -f "$PGDOG_CONTAINER" "$SOURCE_CONTAINER" >/dev/null 2>&1 || true
  if [[ -n "$reverse_apply_pid" ]]; then
    kill "$reverse_apply_pid" >/dev/null 2>&1 || true
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

psql_doltgres() {
  PGCONNECT_TIMEOUT=2 PGPASSWORD=password psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$DOLTGRES_SHARD1_PORT" -U postgres -d "$DOLTGRES_DATABASE" "$@"
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

start_doltgres() {
  local data_dir="$TMP_DIR/doltgres-data"
  local config_file="$TMP_DIR/doltgres-config.yaml"
  local log_file="$TMP_DIR/doltgres.log"

  mkdir -p "$data_dir"
  write_doltgres_config "$DOLTGRES_SHARD1_PORT" "$data_dir" "$config_file"

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

wait_for_doltgres() {
  for _ in $(seq 1 60); do
    if psql_doltgres -c "SELECT 1;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Doltgres shard did not become ready" >&2
  sed -n '1,160p' "$TMP_DIR/doltgres.log" >&2 || true
  return 1
}

write_pgdog_config() {
  local migrated_customer_enabled="${1:-false}"

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
host = "$PGDOG_BACKEND_HOST"
port = $SOURCE_POSTGRES_PORT
database_name = "pgdog"
user = "postgres"
password = "password"
role = "primary"
shard = 0

[[databases]]
name = "pgdog"
host = "$PGDOG_BACKEND_HOST"
port = $DOLTGRES_SHARD1_PORT
database_name = "$DOLTGRES_DATABASE"
user = "postgres"
password = "password"
role = "primary"
shard = 1

[[sharded_schemas]]
database = "pgdog"
name = "shared"
shard = 0

[[sharded_tables]]
database = "pgdog"
schema = "customer"
name = "orders"
column = "customer_id"
data_type = "bigint"

EOF

  if [[ "$migrated_customer_enabled" == "true" ]]; then
    cat >> "$TMP_DIR/pgdog/pgdog.toml" <<EOF
[[sharded_mappings]]
database = "pgdog"
schema = "customer"
table = "orders"
column = "customer_id"
kind = "list"
values = [$CUSTOMER_ID]
shard = 1

EOF
  fi

  cat >> "$TMP_DIR/pgdog/pgdog.toml" <<EOF
[[sharded_mappings]]
database = "pgdog"
schema = "customer"
table = "orders"
column = "customer_id"
kind = "default"
shard = 0
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

start_pgdog() {
  docker rm -f "$PGDOG_CONTAINER" >/dev/null 2>&1 || true
  docker run -d \
    --name "$PGDOG_CONTAINER" \
    --add-host=host.docker.internal:host-gateway \
    -p "127.0.0.1:$PGDOG_PORT:6432" \
    -v "$TMP_DIR/pgdog:/config:ro" \
    "$PGDOG_IMAGE" \
    pgdog --config /config/pgdog.toml --users /config/users.toml >/dev/null
}

restart_pgdog() {
  start_pgdog
  wait_for_pgdog
}

restart_doltgres() {
  kill "$shard1_pid" >/dev/null 2>&1 || true
  wait "$shard1_pid" >/dev/null 2>&1 || true
  shard1_pid="$(start_doltgres)"
  wait_for_doltgres
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

wait_for_pgdog_customer_route() {
  for _ in $(seq 1 30); do
    if psql_pgdog -c "SELECT count(*) FROM customer.orders WHERE customer_id = $CUSTOMER_ID;" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "PgDog did not reconnect to restarted Doltgres shard" >&2
  docker logs "$PGDOG_CONTAINER" >&2 || true
  return 1
}

wait_for_reverse_slot_active() {
  local slot="$1"
  local active

  for _ in $(seq 1 30); do
    active="$(psql_doltgres -At -c "SELECT active::text FROM pg_catalog.pg_replication_slots WHERE slot_name = '$slot';")"
    if [[ "$active" == "true" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "reverse-apply: slot $slot did not become active" >&2
  exit 1
}

run_reverse_apply() {
  local source_url="$1"
  local target_url="$2"
  local slot="$3"
  local publication="$4"
  local commits="$5"
  shift 5

  (cd "$ROOT_DIR" && go run ./testing/pgdog/reverse_apply \
    -source-url "$source_url" \
    -target-url "$target_url" \
    -slot "$slot" \
    -publication "$publication" \
    -schema customer \
    -table orders \
    -commits "$commits" \
    -timeout 45s \
    "$@")
}

start_reverse_apply() {
  local source_url="$1"
  local target_url="$2"
  local slot="$3"
  local publication="$4"
  local commits="$5"

  run_reverse_apply "$source_url" "$target_url" "$slot" "$publication" "$commits" &
  reverse_apply_pid="$!"
  wait_for_reverse_slot_active "$slot"
}

wait_for_reverse_apply() {
  wait "$reverse_apply_pid"
  reverse_apply_pid=""
}

shared_table_count_on_doltgres() {
  psql_doltgres -At -c "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'shared' AND table_name = 'accounts';"
}

customer_rows() {
  local runner="$1"
  local customer_id="$2"
  "$runner" -At -F '|' -c "SELECT customer_id, order_id, status, amount, COALESCE(note, '') FROM customer.orders WHERE customer_id = $customer_id ORDER BY order_id;"
}

customer_checksum() {
  local runner="$1"
  local customer_id="$2"
  "$runner" -At -F '|' -c "SELECT count(*), COALESCE(sum(amount), 0), COALESCE(sum(length(COALESCE(note, ''))), 0) FROM customer.orders WHERE customer_id = $customer_id;"
}

assert_rows_equal() {
  local name="$1"
  local expected="$2"
  local actual="$3"
  if [[ "$expected" != "$actual" ]]; then
    echo "$name: expected:" >&2
    echo "$expected" >&2
    echo "$name: actual:" >&2
    echo "$actual" >&2
    exit 1
  fi
}

assert_customer_rows_on_runner() {
  local name="$1"
  local runner="$2"
  local customer_id="$3"
  local expected="$4"
  local actual

  actual="$(customer_rows "$runner" "$customer_id")"
  assert_rows_equal "$name" "$expected" "$actual"
}

assert_customer_snapshot_matches_source() {
  local name="$1"
  local target_runner="$2"
  local customer_id="$3"
  local source_rows
  local target_rows
  local source_checksum
  local target_checksum

  source_rows="$(customer_rows psql_source "$customer_id")"
  target_rows="$(customer_rows "$target_runner" "$customer_id")"
  assert_rows_equal "$name-rows" "$source_rows" "$target_rows"

  source_checksum="$(customer_checksum psql_source "$customer_id")"
  target_checksum="$(customer_checksum "$target_runner" "$customer_id")"
  assert_rows_equal "$name-checksum" "$source_checksum" "$target_checksum"
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
  "$POSTGRES_IMAGE" >/dev/null

shard1_pid="$(start_doltgres)"

wait_for_source
wait_for_doltgres

psql_source -c "CREATE SCHEMA shared;"
psql_source -c "CREATE SCHEMA customer;"
psql_source -c "CREATE TABLE shared.accounts (id INT PRIMARY KEY, label TEXT NOT NULL);"
psql_source -c "CREATE TABLE customer.orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, status TEXT NOT NULL, amount INT NOT NULL, note TEXT, PRIMARY KEY (customer_id, order_id));"
psql_source -c "INSERT INTO shared.accounts VALUES (1, 'aurora-shared');"
psql_source -c "INSERT INTO customer.orders VALUES ($UNMIGRATED_CUSTOMER_ID, 1, 'source-open', 70, 'unmigrated'), ($CUSTOMER_ID, 1, 'source-open', 420, 'candidate-one'), ($CUSTOMER_ID, 2, 'source-open', 421, 'candidate-two');"

psql_doltgres -c "CREATE SCHEMA customer;"
psql_doltgres -c "CREATE TABLE customer.orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, status TEXT NOT NULL, amount INT NOT NULL, note TEXT, PRIMARY KEY (customer_id, order_id));"

write_pgdog_config
start_pgdog
wait_for_pgdog

shared_label="$(psql_pgdog -At -c "SELECT label FROM shared.accounts WHERE id = 1;")"
if [[ "$shared_label" != "aurora-shared" ]]; then
  echo "shared-routing: expected PgDog shared read from shard 0, got: $shared_label" >&2
  exit 1
fi

psql_pgdog -c "INSERT INTO shared.accounts VALUES (2, 'pgdog-shared-write');"
psql_pgdog -c "UPDATE shared.accounts SET label = 'pgdog-shared-updated' WHERE id = 2;"
shared_write="$(psql_source -At -c "SELECT label FROM shared.accounts WHERE id = 2;")"
if [[ "$shared_write" != "pgdog-shared-updated" ]]; then
  echo "shared-routing: expected shared write on source shard 0, got: $shared_write" >&2
  exit 1
fi

if [[ "$(shared_table_count_on_doltgres)" != "0" ]]; then
  echo "shared-routing: shared.accounts should not exist on Doltgres shard" >&2
  exit 1
fi

psql_pgdog -c "INSERT INTO customer.orders (customer_id, order_id, status, amount, note) VALUES ($UNMIGRATED_CUSTOMER_ID, 2, 'pgdog-unmigrated', 71, 'default mapping');"
psql_pgdog -c "UPDATE customer.orders SET status = 'pgdog-unmigrated-updated' WHERE customer_id = $UNMIGRATED_CUSTOMER_ID AND order_id = 2;"

source_rows="$(customer_rows psql_source "$UNMIGRATED_CUSTOMER_ID")"
pgdog_rows="$(customer_rows psql_pgdog "$UNMIGRATED_CUSTOMER_ID")"
doltgres_rows="$(customer_rows psql_doltgres "$UNMIGRATED_CUSTOMER_ID")"
assert_rows_equal "unmigrated-customer-source-pgdog" "$source_rows" "$pgdog_rows"
if [[ -n "$doltgres_rows" ]]; then
  echo "unmigrated-routing: expected no rows on Doltgres shard, got:" >&2
  echo "$doltgres_rows" >&2
  exit 1
fi

schema_cache_probe="$(psql_pgdog -At -F '|' -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'customer' AND table_name = 'orders';")"
if [[ "$schema_cache_probe" != "customer|orders" ]]; then
  echo "schema-cache: expected PgDog-visible customer.orders, got:" >&2
  echo "$schema_cache_probe" >&2
  exit 1
fi

pre_cutover_source_rows="$(customer_rows psql_source "$CUSTOMER_ID")"
assert_customer_rows_on_runner "pre-cutover-customer-source-pgdog" psql_pgdog "$CUSTOMER_ID" "$pre_cutover_source_rows"

if [[ -n "$(customer_rows psql_doltgres "$CUSTOMER_ID")" ]]; then
  echo "initial-copy: expected Doltgres customer target to start empty" >&2
  exit 1
fi

psql_source -q \
  -c "CREATE TEMP TABLE dg_customer_orders_copy AS SELECT customer_id, order_id, status, amount, note FROM customer.orders WHERE customer_id = $CUSTOMER_ID ORDER BY order_id;" \
  -c "COPY dg_customer_orders_copy (customer_id, order_id, status, amount, note) TO STDOUT;" |
  psql_doltgres -c "COPY customer.orders (customer_id, order_id, status, amount, note) FROM STDIN;"

assert_customer_snapshot_matches_source "initial-copy-doltgres-source" psql_doltgres "$CUSTOMER_ID"
if [[ -n "$(customer_rows psql_doltgres "$UNMIGRATED_CUSTOMER_ID")" ]]; then
  echo "initial-copy: unmigrated customer rows should not be copied to Doltgres" >&2
  exit 1
fi
if [[ "$(shared_table_count_on_doltgres)" != "0" ]]; then
  echo "initial-copy: shared.accounts should not be copied to Doltgres" >&2
  exit 1
fi

psql_source -c "BEGIN;" \
  -c "INSERT INTO customer.orders VALUES ($CUSTOMER_ID, 3, 'source-after-copy-insert', 422, 'inserted after copy');" \
  -c "UPDATE customer.orders SET status = 'source-after-copy-update', amount = 425, note = 'updated after copy' WHERE customer_id = $CUSTOMER_ID AND order_id = 1;" \
  -c "DELETE FROM customer.orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;" \
  -c "COMMIT;"
psql_source -c "INSERT INTO customer.orders VALUES ($UNMIGRATED_CUSTOMER_ID, 3, 'unmigrated-after-copy', 73, 'should stay source');"
psql_source -c "UPDATE shared.accounts SET label = 'aurora-shared-after-copy' WHERE id = 1;"

psql_doltgres -c "BEGIN;" \
  -c "INSERT INTO customer.orders VALUES ($CUSTOMER_ID, 3, 'source-after-copy-insert', 422, 'inserted after copy');" \
  -c "UPDATE customer.orders SET status = 'source-after-copy-update', amount = 425, note = 'updated after copy' WHERE customer_id = $CUSTOMER_ID AND order_id = 1;" \
  -c "DELETE FROM customer.orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;" \
  -c "COMMIT;"

assert_customer_snapshot_matches_source "change-apply-doltgres-source" psql_doltgres "$CUSTOMER_ID"
if [[ -n "$(customer_rows psql_doltgres "$UNMIGRATED_CUSTOMER_ID")" ]]; then
  echo "change-apply: unmigrated customer rows should remain only on source" >&2
  exit 1
fi
if [[ "$(shared_table_count_on_doltgres)" != "0" ]]; then
  echo "change-apply: shared.accounts should remain only on source" >&2
  exit 1
fi

write_pgdog_config true
restart_pgdog

post_cutover_pgdog_rows="$(customer_rows psql_pgdog "$CUSTOMER_ID")"
post_cutover_doltgres_rows="$(customer_rows psql_doltgres "$CUSTOMER_ID")"
assert_rows_equal "post-cutover-customer-doltgres-pgdog" "$post_cutover_doltgres_rows" "$post_cutover_pgdog_rows"

reverse_source_url="postgres://postgres:password@127.0.0.1:$DOLTGRES_SHARD1_PORT/$DOLTGRES_DATABASE?sslmode=disable"
reverse_target_url="postgres://postgres:password@127.0.0.1:$SOURCE_POSTGRES_PORT/pgdog?sslmode=disable"
reverse_slot="dg_schema_reverse_slot"
reverse_publication="dg_schema_reverse_pub"
reverse_shared_before="$(psql_source -At -F '|' -c "SELECT id, label FROM shared.accounts ORDER BY id;")"
reverse_unmigrated_before="$(customer_rows psql_source "$UNMIGRATED_CUSTOMER_ID")"

psql_doltgres -c "CREATE PUBLICATION $reverse_publication FOR TABLE customer.orders;"
run_reverse_apply "$reverse_source_url" "$reverse_target_url" "$reverse_slot" "$reverse_publication" 1 -create-slot-only

psql_pgdog -c "INSERT INTO customer.orders (customer_id, order_id, status, amount, note) VALUES ($CUSTOMER_ID, 2, 'post-cutover-insert', 430, 'written after mapping cutover');"
psql_pgdog -c "UPDATE customer.orders SET status = 'post-cutover-updated', amount = 431 WHERE customer_id = $CUSTOMER_ID AND order_id = 2;"

post_write_source_count="$(psql_source -At -c "SELECT count(*) FROM customer.orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;")"
if [[ "$post_write_source_count" != "0" ]]; then
  echo "cutover-routing: post-cutover write reached source shard 0" >&2
  exit 1
fi
post_write_doltgres_result="$(psql_doltgres -At -F '|' -c "SELECT status, amount, note FROM customer.orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;")"
if [[ "$post_write_doltgres_result" != "post-cutover-updated|431|written after mapping cutover" ]]; then
  echo "cutover-routing: expected post-cutover write on Doltgres, got: $post_write_doltgres_result" >&2
  exit 1
fi

run_reverse_apply "$reverse_source_url" "$reverse_target_url" "$reverse_slot" "$reverse_publication" 2
source_reverse_insert_result="$(psql_source -At -F '|' -c "SELECT status, amount, note FROM customer.orders WHERE customer_id = $CUSTOMER_ID AND order_id = 2;")"
if [[ "$source_reverse_insert_result" != "post-cutover-updated|431|written after mapping cutover" ]]; then
  echo "reverse-apply: expected post-cutover write on source rollback target, got: $source_reverse_insert_result" >&2
  exit 1
fi

restart_doltgres
wait_for_pgdog_customer_route

start_reverse_apply "$reverse_source_url" "$reverse_target_url" "$reverse_slot" "$reverse_publication" 2
psql_pgdog -c "UPDATE customer.orders SET status = 'reverse-updated', amount = 450, note = 'reverse update after restart' WHERE customer_id = $CUSTOMER_ID AND order_id = 2;"
psql_pgdog -c "DELETE FROM customer.orders WHERE customer_id = $CUSTOMER_ID AND order_id = 1;"
wait_for_reverse_apply
assert_customer_snapshot_matches_source "reverse-apply-doltgres-source" psql_doltgres "$CUSTOMER_ID"

source_rows_after_cutover="$(customer_rows psql_source "$UNMIGRATED_CUSTOMER_ID")"
assert_customer_rows_on_runner "unmigrated-after-cutover-source-pgdog" psql_pgdog "$UNMIGRATED_CUSTOMER_ID" "$source_rows_after_cutover"
assert_rows_equal "reverse-apply-unmigrated-source-unchanged" "$reverse_unmigrated_before" "$source_rows_after_cutover"

shared_after_cutover="$(psql_pgdog -At -c "SELECT label FROM shared.accounts WHERE id = 2;")"
if [[ "$shared_after_cutover" != "pgdog-shared-updated" ]]; then
  echo "shared-routing: expected shared row to remain routed to source after cutover, got: $shared_after_cutover" >&2
  exit 1
fi
reverse_shared_after="$(psql_source -At -F '|' -c "SELECT id, label FROM shared.accounts ORDER BY id;")"
assert_rows_equal "reverse-apply-shared-source-unchanged" "$reverse_shared_before" "$reverse_shared_after"

echo "PgDog schema-split topology, mapping cutover, and reverse apply harness passed for customer_id=$CUSTOMER_ID unmigrated_customer_id=$UNMIGRATED_CUSTOMER_ID"
