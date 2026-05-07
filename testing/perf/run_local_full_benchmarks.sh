#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:18-alpine}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-doltgres-fullbench-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-15438}"
POSTGRES_DB="${POSTGRES_DB:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-password}"

DOLTGRES_PORT="${DOLTGRES_PORT:-15439}"
DOLTGRES_DB="${DOLTGRES_DB:-postgres}"
DOLTGRES_PASSWORD="${DOLTGRES_PASSWORD:-password}"
DOLTGRES_USER="${DOLTGRES_USER:-postgres}"
DOLTGRES_BUILD="${DOLTGRES_BUILD:-1}"
DOLTGRES_BIN="${DOLTGRES_BIN:-}"

SYSBENCH_TIME="${SYSBENCH_TIME:-15}"
SYSBENCH_THREADS="${SYSBENCH_THREADS:-1}"
SYSBENCH_TABLE_SIZE="${SYSBENCH_TABLE_SIZE:-1000}"
SYSBENCH_TABLES="${SYSBENCH_TABLES:-1}"
SYSBENCH_REPORT_INTERVAL="${SYSBENCH_REPORT_INTERVAL:-0}"
SYSBENCH_LUA_REPO="${SYSBENCH_LUA_REPO:-https://github.com/dolthub/sysbench-lua-scripts.git}"

DOLTGRES_PAIRED_INDEX_BENCH_ITERS="${DOLTGRES_PAIRED_INDEX_BENCH_ITERS:-25}"
SKIP_SYSBENCH="${SKIP_SYSBENCH:-0}"
SKIP_INDEX_BENCH="${SKIP_INDEX_BENCH:-0}"
KEEP_CONTAINERS="${KEEP_CONTAINERS:-0}"

timestamp="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.local_benchmarks/full-$timestamp}"
WORK_DIR="$OUT_DIR/work"
LOG_DIR="$OUT_DIR/logs"
REPORT_MD="$OUT_DIR/report.md"
SYSBENCH_CSV="$OUT_DIR/sysbench-results.csv"

CI_READ_TESTS=(
  "oltp_read_only"
  "oltp_point_select"
  "select_random_points"
  "select_random_ranges"
  "covering_index_scan_postgres"
  "index_scan_postgres"
  "table_scan_postgres"
  "groupby_scan_postgres"
  "index_join_scan_postgres"
  "types_table_scan_postgres"
  "index_join_postgres"
)

CI_WRITE_TESTS=(
  "oltp_read_write"
  "oltp_update_index"
  "oltp_update_non_index"
  "oltp_insert"
  "oltp_write_only"
  "oltp_delete_insert_postgres"
  "types_delete_insert_postgres"
)

ALL_SYSBENCH_TESTS=("${CI_READ_TESTS[@]}" "${CI_WRITE_TESTS[@]}")
failures=0
doltgres_pid=""

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 127
  fi
}

cleanup() {
  if [[ -n "$doltgres_pid" ]] && kill -0 "$doltgres_pid" >/dev/null 2>&1; then
    kill "$doltgres_pid" >/dev/null 2>&1 || true
    wait "$doltgres_pid" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_CONTAINERS" != "1" ]]; then
    docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local label="$3"
  local deadline=$((SECONDS + 60))
  until (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "$label did not open $host:$port within 60s" >&2
      return 1
    fi
    sleep 0.2
  done
}

prepare_sysbench_scripts() {
  mkdir -p "$WORK_DIR"
  if [[ ! -d "$WORK_DIR/sysbench-lua-scripts/.git" ]]; then
    git clone --depth 1 "$SYSBENCH_LUA_REPO" "$WORK_DIR/sysbench-lua-scripts"
  fi
  cp "$WORK_DIR"/sysbench-lua-scripts/*.lua "$WORK_DIR"/
}

start_postgres() {
  docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true
  docker run -d \
    --name "$POSTGRES_CONTAINER" \
    -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
    -e POSTGRES_DB="$POSTGRES_DB" \
    -p "127.0.0.1:$POSTGRES_PORT:5432" \
    "$POSTGRES_IMAGE" >/dev/null

  until docker exec "$POSTGRES_CONTAINER" pg_isready -U postgres -d "$POSTGRES_DB" >/dev/null 2>&1; do
    sleep 0.2
  done
}

build_doltgres() {
  set_local_icu_env
  if [[ -n "$DOLTGRES_BIN" ]]; then
    return
  fi
  DOLTGRES_BIN="$WORK_DIR/doltgres"
  if [[ "$DOLTGRES_BUILD" == "1" || ! -x "$DOLTGRES_BIN" ]]; then
    (cd "$ROOT_DIR" && go build -o "$DOLTGRES_BIN" ./cmd/doltgres)
  fi
}

start_doltgres() {
  local data_dir="$WORK_DIR/doltgres-data"
  local config_file="$WORK_DIR/doltgres-config.yaml"
  rm -rf "$data_dir"
  mkdir -p "$data_dir"
  cat >"$config_file" <<YAML
log_level: warn

behavior:
  read_only: false
  disable_client_multi_statements: false
  dolt_transaction_commit: false

user:
  name: "$DOLTGRES_USER"
  password: "$DOLTGRES_PASSWORD"

listener:
  host: 127.0.0.1
  port: $DOLTGRES_PORT
  read_timeout_millis: 28800000
  write_timeout_millis: 28800000

data_dir: "$data_dir"
YAML

  DOLTGRES_USER="$DOLTGRES_USER" \
    DOLTGRES_PASSWORD="$DOLTGRES_PASSWORD" \
    DOLTGRES_DB="$DOLTGRES_DB" \
    "$DOLTGRES_BIN" --config "$config_file" >"$LOG_DIR/doltgres-server.log" 2>&1 &
  doltgres_pid="$!"
  wait_for_tcp "127.0.0.1" "$DOLTGRES_PORT" "Doltgres"
}

sysbench_supports_tables_arg() {
  case "$1" in
    covering_index_scan_postgres | \
      index_scan_postgres | \
      table_scan_postgres | \
      groupby_scan_postgres | \
      index_join_scan_postgres | \
      types_table_scan_postgres | \
      index_join_postgres | \
      types_delete_insert_postgres)
      return 1
      ;;
    *)
      return 0
      ;;
  esac
}

sysbench_common_args() {
  local test_name="$1"
  local port="$2"
  local db="$3"
  local password="$4"
  printf '%s\n' \
    "--db-driver=pgsql" \
    "--pgsql-host=127.0.0.1" \
    "--pgsql-port=$port" \
    "--pgsql-user=postgres" \
    "--pgsql-password=$password" \
    "--pgsql-db=$db" \
    "--db-ps-mode=disable" \
    "--threads=$SYSBENCH_THREADS" \
    "--table-size=$SYSBENCH_TABLE_SIZE" \
    "--report-interval=$SYSBENCH_REPORT_INTERVAL"
  if sysbench_supports_tables_arg "$test_name"; then
    printf '%s\n' "--tables=$SYSBENCH_TABLES"
  fi
}

extract_metric() {
  local pattern="$1"
  local file="$2"
  sed -nE "$pattern" "$file" | tail -n 1
}

append_sysbench_csv() {
  local engine="$1"
  local test_name="$2"
  local status="$3"
  local log_file="$4"
  local transactions=""
  local queries=""
  local p95=""
  if [[ -f "$log_file" ]]; then
    transactions="$(extract_metric 's/.*transactions:.*\(([0-9.]+) per sec\.\).*/\1/p' "$log_file")"
    queries="$(extract_metric 's/.*queries:.*\(([0-9.]+) per sec\.\).*/\1/p' "$log_file")"
    p95="$(extract_metric 's/.*95th percentile:[[:space:]]+([0-9.]+).*/\1/p' "$log_file")"
  fi
  printf '%s,%s,%s,%s,%s,%s,%s\n' \
    "$engine" "$test_name" "$status" "$transactions" "$queries" "$p95" "$log_file" >>"$SYSBENCH_CSV"
}

run_sysbench_case() {
  local engine="$1"
  local port="$2"
  local db="$3"
  local password="$4"
  local test_name="$5"
  local log_file="$LOG_DIR/sysbench-$engine-$test_name.log"
  local status="pass"
  local common_args=()
  common_args=($(sysbench_common_args "$test_name" "$port" "$db" "$password"))

  {
    echo "----$engine/$test_name----"
    echo "prepare"
  } >"$log_file"

  (cd "$WORK_DIR" && sysbench "${common_args[@]}" "$test_name" cleanup) >>"$log_file" 2>&1 || true
  if ! (cd "$WORK_DIR" && sysbench "${common_args[@]}" "$test_name" prepare) >>"$log_file" 2>&1; then
    status="prepare_failed"
  fi

  if [[ "$status" == "pass" ]]; then
    {
      echo "run"
      echo "----$test_name----"
    } >>"$log_file"
    if ! (cd "$WORK_DIR" && sysbench "${common_args[@]}" --time="$SYSBENCH_TIME" "$test_name" run) >>"$log_file" 2>&1; then
      status="run_failed"
    fi
  fi

  (cd "$WORK_DIR" && sysbench "${common_args[@]}" "$test_name" cleanup) >>"$log_file" 2>&1 || true
  echo "----$engine/$test_name----" >>"$log_file"
  append_sysbench_csv "$engine" "$test_name" "$status" "$log_file"
  if [[ "$status" != "pass" ]]; then
    failures=$((failures + 1))
    echo "$engine/$test_name failed; see $log_file" >&2
  else
    echo "$engine/$test_name complete"
  fi
}

run_sysbench_suite() {
  echo "engine,test,status,transactions_per_sec,queries_per_sec,latency_p95_ms,log" >"$SYSBENCH_CSV"
  for test_name in "${ALL_SYSBENCH_TESTS[@]}"; do
    run_sysbench_case "doltgres" "$DOLTGRES_PORT" "$DOLTGRES_DB" "$DOLTGRES_PASSWORD" "$test_name"
  done
  for test_name in "${ALL_SYSBENCH_TESTS[@]}"; do
    run_sysbench_case "postgres18" "$POSTGRES_PORT" "$POSTGRES_DB" "$POSTGRES_PASSWORD" "$test_name"
  done
}

write_sysbench_report() {
  {
    echo "# Local Full Benchmark Report"
    echo
    echo "- Doltgres: local binary on port $DOLTGRES_PORT"
    echo "- PostgreSQL: $POSTGRES_IMAGE on port $POSTGRES_PORT"
    echo "- Sysbench time: ${SYSBENCH_TIME}s"
    echo "- Sysbench threads: $SYSBENCH_THREADS"
    echo "- Sysbench table size: $SYSBENCH_TABLE_SIZE"
    echo "- Output directory: $OUT_DIR"
    echo
    echo "## Sysbench"
    echo
    if [[ -f "$SYSBENCH_CSV" ]]; then
      echo "| test | doltgres qps | postgres18 qps | doltgres/postgres qps | doltgres p95 ms | postgres18 p95 ms | status |"
      echo "| --- | ---: | ---: | ---: | ---: | ---: | --- |"
      awk -F, '
        NR == 1 { next }
        $1 == "postgres18" {
          pg_qps[$2] = $5
          pg_p95[$2] = $6
          pg_status[$2] = $3
          next
        }
        $1 == "doltgres" {
          dg_qps[$2] = $5
          dg_p95[$2] = $6
          dg_status[$2] = $3
          tests[++n] = $2
        }
        END {
          for (i = 1; i <= n; i++) {
            test = tests[i]
            ratio = ""
            if (dg_qps[test] != "" && pg_qps[test] != "" && pg_qps[test] + 0 > 0) {
              ratio = sprintf("%.2fx", (dg_qps[test] + 0) / (pg_qps[test] + 0))
            }
            status = dg_status[test] "/" pg_status[test]
            printf("| %s | %s | %s | %s | %s | %s | %s |\n", test, dg_qps[test], pg_qps[test], ratio, dg_p95[test], pg_p95[test], status)
          }
        }
      ' "$SYSBENCH_CSV"
    else
      echo "Skipped."
    fi
    echo
    echo "## Paired Index Benchmarks"
    echo
    if [[ -f "$LOG_DIR/paired-index-benchmarks.log" ]]; then
      echo "Raw output: \`$LOG_DIR/paired-index-benchmarks.log\`"
      echo
      grep 'paired-index-baseline' "$LOG_DIR/paired-index-benchmarks.log" | sed 's/^/- /' || true
    else
      echo "Skipped."
    fi
  } >"$REPORT_MD"
}

run_index_benchmarks() {
  local log_file="$LOG_DIR/paired-index-benchmarks.log"
  local postgres_url="postgres://postgres:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PORT}/${POSTGRES_DB}?sslmode=disable"

  set_local_icu_env
  if ! (
    cd "$ROOT_DIR" &&
      DOLTGRES_POSTGRES_BASELINE_URL="$postgres_url" \
        DOLTGRES_PAIRED_INDEX_BENCH_ITERS="$DOLTGRES_PAIRED_INDEX_BENCH_ITERS" \
        go test ./testing/go -run '^$' -bench '^BenchmarkPairedIndexBaselines$' -benchtime=1x -count=1 -v
  ) >"$log_file" 2>&1; then
    failures=$((failures + 1))
    echo "paired index benchmarks failed; see $log_file" >&2
  else
    echo "paired index benchmarks complete"
  fi
}

set_local_icu_env() {
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
}

main() {
  need_cmd docker
  need_cmd git
  need_cmd go
  if [[ "$SKIP_SYSBENCH" != "1" ]]; then
    need_cmd sysbench
  fi

  mkdir -p "$WORK_DIR" "$LOG_DIR"
  if [[ "$SKIP_SYSBENCH" != "1" ]]; then
    prepare_sysbench_scripts
  fi
  start_postgres

  if [[ "$SKIP_SYSBENCH" != "1" ]]; then
    build_doltgres
    start_doltgres
    run_sysbench_suite
  fi
  if [[ "$SKIP_INDEX_BENCH" != "1" ]]; then
    run_index_benchmarks
  fi
  write_sysbench_report

  echo "report: $REPORT_MD"
  echo "sysbench csv: $SYSBENCH_CSV"
  if (( failures > 0 )); then
    echo "$failures benchmark step(s) failed" >&2
    exit 1
  fi
}

main "$@"
