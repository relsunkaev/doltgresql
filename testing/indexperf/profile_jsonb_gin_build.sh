#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
timestamp="$(date +%Y%m%d-%H%M%S)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/.local_benchmarks/jsonb-gin-build-profile-$timestamp}"
NODE_TEST_BIN="$OUT_DIR/server-node.test"
NODE_BENCH_OUT="$OUT_DIR/node-stage-benchmarks.txt"
PAIRED_BENCH_OUT="$OUT_DIR/paired-index-benchmarks.txt"
CPU_PROFILE="$OUT_DIR/jsonb-gin-build.cpu.pprof"
MEM_PROFILE="$OUT_DIR/jsonb-gin-build.mem.pprof"
CPU_TOP="$OUT_DIR/jsonb-gin-build.cpu.top.txt"
MEM_TOP="$OUT_DIR/jsonb-gin-build.mem.top.txt"
REPORT_MD="$OUT_DIR/report.md"

mkdir -p "$OUT_DIR"

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

set_local_icu_env

(
  cd "$ROOT_DIR"
  go test -c -o "$NODE_TEST_BIN" ./server/node
  "$NODE_TEST_BIN" \
    -test.run '^$' \
    -test.bench 'Benchmark(JsonbGinPostingChunkRowsToSink|JsonbGinPostingChunkRowsToSinkWorkers|BuildSortedPrimaryRowIndexPostingRows|SortedPrimaryRowIndexBuilderPostingRows)$' \
    -test.benchtime=1x \
    -test.count=1 \
    -test.benchmem \
    -test.cpuprofile "$CPU_PROFILE" \
    -test.memprofile "$MEM_PROFILE" | tee "$NODE_BENCH_OUT"
  go tool pprof -top "$NODE_TEST_BIN" "$CPU_PROFILE" >"$CPU_TOP" || true
  go tool pprof -top "$NODE_TEST_BIN" "$MEM_PROFILE" >"$MEM_TOP" || true
  DOLTGRES_PAIRED_INDEX_BENCH_ITERS="${DOLTGRES_PAIRED_INDEX_BENCH_ITERS:-1}" \
    testing/indexperf/run_paired_benchmarks.sh | tee "$PAIRED_BENCH_OUT"
)

{
  echo "# JSONB GIN Build Profile"
  echo
  echo "Generated: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  echo
  echo "## Stage Benchmarks"
  echo
  echo '```'
  grep -E 'Benchmark(JsonbGinPostingChunkRowsToSink|JsonbGinPostingChunkRowsToSinkWorkers|BuildSortedPrimaryRowIndexPostingRows|SortedPrimaryRowIndexBuilderPostingRows)' "$NODE_BENCH_OUT" || true
  echo '```'
  echo
  echo "## Paired PostgreSQL 18 Build Benchmarks"
  echo
  echo '```'
  grep -E 'paired-index-baseline name=jsonb_gin/build' "$PAIRED_BENCH_OUT" || true
  echo '```'
  echo
  echo "## CPU Top"
  echo
  echo '```'
  head -40 "$CPU_TOP" || true
  echo '```'
  echo
  echo "## Artifacts"
  echo
  echo "- node benchmark output: $NODE_BENCH_OUT"
  echo "- paired benchmark output: $PAIRED_BENCH_OUT"
  echo "- CPU profile: $CPU_PROFILE"
  echo "- memory profile: $MEM_PROFILE"
  echo "- CPU top: $CPU_TOP"
  echo "- memory top: $MEM_TOP"
} >"$REPORT_MD"

echo "jsonb gin build profile complete"
echo "report: $REPORT_MD"
