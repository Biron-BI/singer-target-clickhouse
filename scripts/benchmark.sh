#!/usr/bin/env bash
# Benchmark the Kotlin port vs the published TS target on the same JSONL input.
#
# Both implementations are invoked as Docker containers against the ClickHouse
# instance on the host (default: http://localhost:8123, user `default`, no password).
# The Kotlin image is built locally from the current working tree.
#
# Usage: scripts/benchmark.sh [options] <input.jsonl.gz>
#
# Options:
#   -n <iters>          Number of iterations per implementation (default: 1)
#   --skip-build        Skip rebuilding the Kotlin jar / image
#   --skip-pull         Skip pulling the TS image
#   --only <kotlin|ts>  Run only one implementation
#   --ch-host <host>    ClickHouse host (default: localhost)
#   --ch-port <port>    ClickHouse HTTP port (default: 8123)
#   --ch-user <user>    ClickHouse user (default: default)
#   --ch-password <pw>  ClickHouse password (default: empty)
#
# Notes:
# - Requires Linux-style `--network=host` for Docker (Linux only; Docker Desktop
#   users on macOS/Windows would need `host.docker.internal` tweaks).
# - The script drops & recreates the target databases before each run.
# - CPU time is measured from the container's cgroup v2 `cpu.stat` (`usage_usec`,
#   cumulative across all threads). `eff_cores` = cpu_ms / wall_ms tells you how
#   many CPU cores the implementation uses on average — useful to plan how many
#   targets you can run in parallel on a given host. Requires cgroups v2 with the
#   systemd driver (Docker default on recent Ubuntu/Debian/Fedora).

set -euo pipefail
export LC_ALL=C

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ITERATIONS=1
SKIP_BUILD=0
SKIP_PULL=0
ONLY=""
CH_HOST="localhost"
CH_PORT="8123"
CH_USER="default"
CH_PASSWORD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n) ITERATIONS="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-pull) SKIP_PULL=1; shift ;;
    --only) ONLY="$2"; shift 2 ;;
    --ch-host) CH_HOST="$2"; shift 2 ;;
    --ch-port) CH_PORT="$2"; shift 2 ;;
    --ch-user) CH_USER="$2"; shift 2 ;;
    --ch-password) CH_PASSWORD="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 2
      ;;
    *)
      INPUT="$1"; shift ;;
  esac
done

if [[ -z "${INPUT:-}" ]]; then
  echo "usage: $0 [options] <input.jsonl.gz>" >&2
  exit 2
fi
if [[ ! -f "$INPUT" ]]; then
  echo "input file not found: $INPUT" >&2
  exit 2
fi

KOTLIN_IMAGE="target-clickhouse-kotlin:local"
TS_IMAGE="ghcr.io/biron-bi/target-clickhouse:2.11.0"
DB_KOTLIN="bench_kotlin"
DB_TS="bench_ts"

ch_curl() {
  local query="$1"
  local auth=""
  if [[ -n "$CH_PASSWORD" ]]; then
    auth="-u ${CH_USER}:${CH_PASSWORD}"
  elif [[ "$CH_USER" != "default" ]]; then
    auth="-u ${CH_USER}:"
  fi
  # shellcheck disable=SC2086
  curl -sS $auth -X POST "http://${CH_HOST}:${CH_PORT}/" --data-binary "$query"
}

check_ch() {
  if ! ch_curl "SELECT 1" | grep -q '^1$'; then
    echo "cannot reach ClickHouse at ${CH_HOST}:${CH_PORT}" >&2
    exit 1
  fi
}

build_kotlin_image() {
  echo "[build] gradle bootJar"
  (cd "$PROJECT_ROOT" && ./gradlew -q bootJar)
  echo "[build] docker build $KOTLIN_IMAGE"
  docker build --quiet -t "$KOTLIN_IMAGE" "$PROJECT_ROOT" >/dev/null
}

pull_ts_image() {
  echo "[pull] $TS_IMAGE"
  docker pull --quiet "$TS_IMAGE" >/dev/null
}

write_config() {
  # $1 = database
  local db="$1"
  local file
  file="$(mktemp "${TMPDIR:-/tmp}/bench-config.XXXXXX.json")"
  cat >"$file" <<EOF
{
  "host": "${CH_HOST}",
  "port": ${CH_PORT},
  "username": "${CH_USER}",
  "password": "${CH_PASSWORD}",
  "database": "${db}"
}
EOF
  echo "$file"
}

reset_db() {
  local db="$1"
  ch_curl "DROP DATABASE IF EXISTS \`${db}\` SYNC" >/dev/null
  ch_curl "CREATE DATABASE \`${db}\`" >/dev/null
}

# Returns row count across every table in a given database.
total_rows() {
  local db="$1"
  ch_curl "SELECT sum(total_rows) FROM system.tables WHERE database = '${db}'" | tr -d '\n'
}

table_count() {
  local db="$1"
  ch_curl "SELECT count() FROM system.tables WHERE database = '${db}'" | tr -d '\n'
}

# Locate a container's cgroup-v2 cpu.stat file given its ID (systemd driver is the
# Docker default on recent distros). Falls back to the legacy `docker/<id>` path
# used by the cgroupfs driver. Returns nothing (and exit 0) if neither exists —
# the trailing `return 0` matters: under `set -e`, a failing `[[ -r ]]` as the
# last command would otherwise bubble up through `$(…)` and kill the caller.
cpu_stat_path_for() {
  local cid="$1"
  local candidates=(
    "/sys/fs/cgroup/system.slice/docker-${cid}.scope/cpu.stat"
    "/sys/fs/cgroup/docker/${cid}/cpu.stat"
  )
  local path
  for path in "${candidates[@]}"; do
    [[ -r "$path" ]] && { echo "$path"; return 0; }
  done
  return 0
}

# Runs one ingestion. Prints `<wall_ms> <cpu_ms>` on stdout.
#
# CPU time is read from the container's cgroup cpu.stat `usage_usec` (cumulative
# across all threads/cores). A background poller tails the counter at ~50 Hz
# because the cgroup scope disappears shortly after the container exits.
# $1 = image, $2 = database, $3 = config file path on host
run_once() {
  local image="$1" db="$2" config="$3"
  local cidfile cpufile start end rc
  reset_db "$db"
  : >/tmp/bench.log

  cidfile=$(mktemp -u "${TMPDIR:-/tmp}/bench-cid.XXXXXX")
  cpufile=$(mktemp "${TMPDIR:-/tmp}/bench-cpu.XXXXXX")
  : >"$cpufile"

  # Background poller: waits for the cidfile, then samples cgroup cpu.stat until
  # the scope disappears. Each sample overwrites `$cpufile`, so on exit it holds
  # the last observed `usage_usec`.
  # NB: `local` is not valid inside a ( … ) subshell — declare vars plainly.
  (
    while [[ ! -s "$cidfile" ]]; do sleep 0.01; done
    poller_cid=$(cat "$cidfile")
    # The scope is created slightly after the cidfile; retry briefly.
    poller_scope=""
    for _ in $(seq 1 50); do
      poller_scope=$(cpu_stat_path_for "$poller_cid")
      [[ -n "$poller_scope" ]] && break
      sleep 0.02
    done
    [[ -z "$poller_scope" ]] && exit 0
    while [[ -r "$poller_scope" ]]; do
      poller_v=$(awk '$1=="usage_usec"{print $2; exit}' "$poller_scope" 2>/dev/null) || break
      [[ -n "$poller_v" ]] && printf '%s' "$poller_v" >"$cpufile"
      sleep 0.02
    done
  ) &
  local poller=$!

  start=$(date +%s%N)
  set +e
  zcat "$INPUT" | docker run --cidfile "$cidfile" --rm -i \
    --network=host \
    -v "$config:/config.json:ro" \
    "$image" --config /config.json \
    >/dev/null 2>>/tmp/bench.log
  rc="${PIPESTATUS[1]}"
  set -e
  end=$(date +%s%N)

  wait "$poller" 2>/dev/null || true

  if [[ "$rc" != "0" ]]; then
    echo "container exited non-zero ($rc). see /tmp/bench.log:" >&2
    tail -n 20 /tmp/bench.log >&2
    rm -f "$cidfile" "$cpufile"
    return 1
  fi

  local wall_ms cpu_usec cpu_ms
  wall_ms=$(( (end - start) / 1000000 ))
  cpu_usec=$(cat "$cpufile" 2>/dev/null || true)
  cpu_usec="${cpu_usec//[^0-9]/}"
  [[ -z "$cpu_usec" ]] && cpu_usec=0
  cpu_ms=$(( cpu_usec / 1000 ))

  rm -f "$cidfile" "$cpufile"
  echo "$wall_ms $cpu_ms"
}

# Divide CPU ms by wall ms → "effective cores" used on average across the run.
eff_cores() {
  local cpu="$1" wall="$2"
  awk -v c="$cpu" -v w="$wall" 'BEGIN { if (w>0) printf "%.2f\n", c/w; else print "n/a" }'
}

median() {
  # Print the median of the stdin numbers.
  sort -n | awk '{a[NR]=$1} END {
    if (NR % 2) print a[(NR+1)/2];
    else printf "%.1f\n", (a[NR/2] + a[NR/2+1]) / 2;
  }'
}

check_ch

if [[ "$SKIP_BUILD" == 0 && "$ONLY" != "ts" ]]; then
  build_kotlin_image
fi
if [[ "$SKIP_PULL" == 0 && "$ONLY" != "kotlin" ]]; then
  pull_ts_image
fi

KOTLIN_CFG=$(write_config "$DB_KOTLIN")
TS_CFG=$(write_config "$DB_TS")
trap 'rm -f "$KOTLIN_CFG" "$TS_CFG"' EXIT

declare -a KOTLIN_WALL=() KOTLIN_CPU=()
declare -a TS_WALL=() TS_CPU=()

printf "\n%-12s %-6s %-10s %-10s %-10s %-10s %-10s\n" \
  "impl" "iter" "wall_ms" "cpu_ms" "eff_cores" "rows" "tables"
printf '%s\n' "---------------------------------------------------------------------------"

record() {
  # $1 = label, $2 = iter, $3 = db, $4 = wall_ms, $5 = cpu_ms
  local label="$1" iter="$2" db="$3" wall="$4" cpu="$5"
  printf "%-12s %-6s %-10s %-10s %-10s %-10s %-10s\n" \
    "$label" "$iter" "$wall" "$cpu" "$(eff_cores "$cpu" "$wall")" \
    "$(total_rows "$db")" "$(table_count "$db")"
}

for i in $(seq 1 "$ITERATIONS"); do
  if [[ "$ONLY" != "ts" ]]; then
    read -r k_wall k_cpu < <(run_once "$KOTLIN_IMAGE" "$DB_KOTLIN" "$KOTLIN_CFG")
    KOTLIN_WALL+=("$k_wall"); KOTLIN_CPU+=("$k_cpu")
    record "kotlin" "$i" "$DB_KOTLIN" "$k_wall" "$k_cpu"
  fi
  if [[ "$ONLY" != "kotlin" ]]; then
    read -r t_wall t_cpu < <(run_once "$TS_IMAGE" "$DB_TS" "$TS_CFG")
    TS_WALL+=("$t_wall"); TS_CPU+=("$t_cpu")
    record "typescript" "$i" "$DB_TS" "$t_wall" "$t_cpu"
  fi
done

summarize() {
  # $1 = label, $2 = wall array name, $3 = cpu array name
  local label="$1"
  local -n wall_arr="$2"
  local -n cpu_arr="$3"
  [[ ${#wall_arr[@]} -eq 0 ]] && return
  local w_med c_med
  w_med=$(printf "%s\n" "${wall_arr[@]}" | median)
  c_med=$(printf "%s\n" "${cpu_arr[@]}" | median)
  printf "%-12s median: wall=%s ms  cpu=%s ms  eff_cores=%s\n" \
    "$label" "$w_med" "$c_med" "$(eff_cores "$c_med" "$w_med")"
  # echo them back for the caller via globals
  eval "${label}_WALL_MED=\"$w_med\""
  eval "${label}_CPU_MED=\"$c_med\""
}

echo
summarize "kotlin" KOTLIN_WALL KOTLIN_CPU
summarize "typescript" TS_WALL TS_CPU

if [[ ${#KOTLIN_WALL[@]} -gt 0 && ${#TS_WALL[@]} -gt 0 ]]; then
  wall_ratio=$(awk -v k="${kotlin_WALL_MED:-0}" -v t="${typescript_WALL_MED:-0}" \
    'BEGIN { if (k>0) printf "%.2fx\n", t/k; else print "n/a" }')
  cpu_ratio=$(awk -v k="${kotlin_CPU_MED:-0}" -v t="${typescript_CPU_MED:-0}" \
    'BEGIN { if (k>0) printf "%.2fx\n", t/k; else print "n/a" }')
  echo "wall speedup (ts / kotlin): $wall_ratio"
  echo "cpu  ratio   (ts / kotlin): $cpu_ratio   (<1 means kotlin burns more CPU to get its wall-time win)"

  r_kotlin=$(total_rows "$DB_KOTLIN")
  r_ts=$(total_rows "$DB_TS")
  if [[ "$r_kotlin" == "$r_ts" ]]; then
    echo "row-count parity: OK (both $r_kotlin)"
  else
    echo "row-count parity: MISMATCH (kotlin=$r_kotlin, ts=$r_ts)"
  fi
fi

echo
echo "(container stderr captured at /tmp/bench.log)"
