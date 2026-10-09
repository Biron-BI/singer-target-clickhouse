#!/usr/bin/env bash
# Benchmark two versions of the target on the same JSONL input — typically a
# modification (the working tree) against the version it starts from.
#
# Each version is built into a Docker image and run against the ClickHouse
# instance on the host (default: http://localhost:8123, user `default`, no
# password), each into its own database (bench_baseline / bench_candidate).
# Once all iterations are done, scripts/compare-databases.sh checks that both
# versions produced the same content.
#
# Usage: scripts/benchmark.sh [options] --baseline <git-ref> <input.jsonl.gz>
#
# Options:
#   --baseline <ref>    Git ref of the reference version (required)
#   --candidate <ref>   Git ref of the version under test (default: the working
#                       tree, uncommitted changes included)
#   -n <iters>          Number of iterations per version (default: 1)
#   --skip-build        Reuse the existing working-tree image instead of rebuilding it
#   --ch-host <host>    ClickHouse host (default: localhost)
#   --ch-port <port>    ClickHouse HTTP port (default: 8123)
#   --ch-user <user>    ClickHouse user (default: default)
#   --ch-password <pw>  ClickHouse password (default: empty)
#
# Examples:
#   scripts/benchmark.sh -n 3 --baseline master input.jsonl.gz
#   scripts/benchmark.sh --baseline v3.1.0 --candidate my-branch input.jsonl.gz
#
# Notes:
# - A git ref is built from a temporary `git worktree` into the image
#   `target-clickhouse-bench:<sha>`. That image is reused as long as it exists,
#   so benchmarking against the same baseline again doesn't rebuild it. The
#   working tree is built in place into `target-clickhouse-bench:worktree`.
# - Requires Linux-style `--network=host` for Docker (Linux only; Docker Desktop
#   users on macOS/Windows would need `host.docker.internal` tweaks).
# - The script drops & recreates the target databases before each run.
# - CPU time is measured from the container's cgroup v2 `cpu.stat` (`usage_usec`,
#   cumulative across all threads). `eff_cores` = cpu_ms / wall_ms tells you how
#   many CPU cores the version uses on average — useful to plan how many
#   targets you can run in parallel on a given host. Requires cgroups v2 with the
#   systemd driver (Docker default on recent Ubuntu/Debian/Fedora).
# - Background MergeTree merges are disabled on the ClickHouse server for the
#   duration of the run (`SYSTEM STOP MERGES`) and re-enabled on exit. This
#   keeps CH's CPU isolated from the target's CPU budget, matching the
#   production topology where CH runs on a separate host. Side effects: this
#   flag is *server-global* (it affects every DB on that server), and with
#   merges frozen parts accumulate — CH delays inserts at ~150 parts/partition
#   and refuses at ~300. Fine for fixture-sized inputs, watch out on large ones.

set -euo pipefail
export LC_ALL=C

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ITERATIONS=1
SKIP_BUILD=0
BASELINE_REF=""
CANDIDATE_REF=""
CH_HOST="localhost"
CH_PORT="8123"
CH_USER="default"
CH_PASSWORD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n) ITERATIONS="$2"; shift 2 ;;
    --baseline) BASELINE_REF="$2"; shift 2 ;;
    --candidate) CANDIDATE_REF="$2"; shift 2 ;;
    --skip-build) SKIP_BUILD=1; shift ;;
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

if [[ -z "${INPUT:-}" || -z "$BASELINE_REF" ]]; then
  echo "usage: $0 [options] --baseline <git-ref> <input.jsonl.gz>" >&2
  exit 2
fi
if [[ ! -f "$INPUT" ]]; then
  echo "input file not found: $INPUT" >&2
  exit 2
fi

IMAGE_REPO="target-clickhouse-bench"
DB_BASELINE="bench_baseline"
DB_CANDIDATE="bench_candidate"
LOG_FILE="${TMPDIR:-/tmp}/bench.log"

# Temporary resources released by `cleanup` on exit.
WORKTREES=()
CONFIGS=()
MERGES_STOPPED=0

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

cleanup() {
  if [[ "$MERGES_STOPPED" == 1 ]]; then
    ch_curl "SYSTEM START MERGES" >/dev/null 2>&1 || true
  fi
  local dir
  for dir in "${WORKTREES[@]}"; do
    git -C "$PROJECT_ROOT" worktree remove --force "$dir" >/dev/null 2>&1 || true
    rm -rf "$dir"
  done
  rm -f "${CONFIGS[@]}"
}
trap cleanup EXIT

resolve_sha() {
  git -C "$PROJECT_ROOT" rev-parse --verify --quiet "${1}^{commit}" || {
    echo "unknown git ref: $1" >&2
    exit 2
  }
}

image_exists() {
  docker image inspect "$1" >/dev/null 2>&1
}

# Builds the jar + image of the working tree, in place.
build_worktree_image() {
  local image="$1"
  if [[ "$SKIP_BUILD" == 1 ]]; then
    if image_exists "$image"; then
      echo "[build] reusing $image"
      return
    fi
    echo "--skip-build: image $image doesn't exist yet" >&2
    exit 1
  fi
  echo "[build] working tree -> $image"
  (cd "$PROJECT_ROOT" && ./gradlew -q bootJar)
  docker build --quiet -f "$PROJECT_ROOT/docker/Dockerfile" -t "$image" "$PROJECT_ROOT" >/dev/null
}

# Builds the jar + image of a commit from a temporary git worktree, unless the
# image already exists.
build_commit_image() {
  local sha="$1" image="$2" dir
  if image_exists "$image"; then
    echo "[build] reusing $image"
    return
  fi
  echo "[build] ${sha:0:7} -> $image"
  dir="$(mktemp -d "${TMPDIR:-/tmp}/bench-worktree.XXXXXX")"
  WORKTREES+=("$dir")
  git -C "$PROJECT_ROOT" worktree add --quiet --detach "$dir" "$sha"
  if [[ ! -f "$dir/docker/Dockerfile" ]]; then
    echo "no docker/Dockerfile at ${sha:0:7}: can't build an image for it" >&2
    exit 1
  fi
  # --no-watch-fs: the daemon would otherwise keep watching the throwaway worktree
  (cd "$dir" && ./gradlew -q --no-watch-fs bootJar)
  docker build --quiet -f "$dir/docker/Dockerfile" -t "$image" "$dir" >/dev/null
  git -C "$PROJECT_ROOT" worktree remove --force "$dir"
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

# Mean CPU frequency in MHz across all online cores (from cpufreq sysfs).
# Prints empty string if cpufreq isn't exposed (containers, some VMs/WSL).
avg_cpu_mhz() {
  awk '{s+=$1; n++} END { if (n>0) printf "%d", s/n/1000 }' \
    /sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq 2>/dev/null
}

# Max temperature in °C across all thermal zones. Filters out obvious junk
# values (ACPI chassis zones sometimes pin at 127°C). Prints empty if unreadable.
max_temp_c() {
  awk '{v=$1+0; if (v>0 && v<150000 && v>m) m=v} END { if (m>0) printf "%d", m/1000 }' \
    /sys/class/thermal/thermal_zone*/temp 2>/dev/null
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

# Runs one ingestion. Prints `<wall_ms> <cpu_ms> <mhz_avg> <mhz_min> <temp_C>` on stdout.
#
# CPU time is read from the container's cgroup cpu.stat `usage_usec` (cumulative
# across all threads/cores). A background poller tails the counter at ~50 Hz
# because the cgroup scope disappears shortly after the container exits. The
# same poller also samples cpufreq + thermal sysfs at ~5 Hz to detect throttling
# across iterations — cheap enough not to perturb the measurement.
# $1 = image, $2 = database, $3 = config file path on host
run_once() {
  local image="$1" db="$2" config="$3"
  local cidfile cpufile freqfile start end rc
  reset_db "$db"
  : >"$LOG_FILE"

  cidfile=$(mktemp -u "${TMPDIR:-/tmp}/bench-cid.XXXXXX")
  cpufile=$(mktemp "${TMPDIR:-/tmp}/bench-cpu.XXXXXX")
  freqfile=$(mktemp "${TMPDIR:-/tmp}/bench-freq.XXXXXX")
  : >"$cpufile"
  : >"$freqfile"

  # Background poller: waits for the cidfile, then samples cgroup cpu.stat until
  # the scope disappears. Each sample overwrites `$cpufile`, so on exit it holds
  # the last observed `usage_usec`. Every 10th tick (~200 ms) it also samples
  # cpufreq + thermal and accumulates running avg/min MHz and max °C; those are
  # written to `$freqfile` as a single line once the scope disappears.
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
    mhz_sum=0; mhz_n=0; mhz_min=9999999; temp_max=0; tick=0
    while [[ -r "$poller_scope" ]]; do
      poller_v=$(awk '$1=="usage_usec"{print $2; exit}' "$poller_scope" 2>/dev/null) || break
      [[ -n "$poller_v" ]] && printf '%s' "$poller_v" >"$cpufile"
      if (( tick % 10 == 0 )); then
        mhz=$(avg_cpu_mhz)
        if [[ -n "$mhz" && "$mhz" -gt 0 ]]; then
          mhz_sum=$(( mhz_sum + mhz ))
          mhz_n=$(( mhz_n + 1 ))
          (( mhz < mhz_min )) && mhz_min=$mhz
        fi
        tc=$(max_temp_c)
        if [[ -n "$tc" && "$tc" -gt "$temp_max" ]]; then temp_max=$tc; fi
      fi
      tick=$(( tick + 1 ))
      sleep 0.02
    done
    mhz_avg=0
    (( mhz_n > 0 )) && mhz_avg=$(( mhz_sum / mhz_n ))
    (( mhz_min == 9999999 )) && mhz_min=0
    echo "$mhz_avg $mhz_min $temp_max" >"$freqfile"
  ) &
  local poller=$!

  start=$(date +%s%N)
  set +e
  zcat "$INPUT" | docker run --cidfile "$cidfile" --rm -i \
    --network=host \
    -v "$config:/config.json:ro" \
    "$image" --config /config.json \
    >/dev/null 2>>"$LOG_FILE"
  rc="${PIPESTATUS[1]}"
  set -e
  end=$(date +%s%N)

  wait "$poller" 2>/dev/null || true

  if [[ "$rc" != "0" ]]; then
    echo "container exited non-zero ($rc). see $LOG_FILE:" >&2
    tail -n 20 "$LOG_FILE" >&2
    rm -f "$cidfile" "$cpufile" "$freqfile"
    return 1
  fi

  local wall_ms cpu_usec cpu_ms
  wall_ms=$(( (end - start) / 1000000 ))
  cpu_usec=$(cat "$cpufile" 2>/dev/null || true)
  cpu_usec="${cpu_usec//[^0-9]/}"
  [[ -z "$cpu_usec" ]] && cpu_usec=0
  cpu_ms=$(( cpu_usec / 1000 ))

  local mhz_avg mhz_min temp_c
  read -r mhz_avg mhz_min temp_c <"$freqfile" 2>/dev/null || true
  [[ -z "${mhz_avg:-}" ]] && mhz_avg=0
  [[ -z "${mhz_min:-}" ]] && mhz_min=0
  [[ -z "${temp_c:-}" ]] && temp_c=0

  rm -f "$cidfile" "$cpufile" "$freqfile"
  echo "$wall_ms $cpu_ms $mhz_avg $mhz_min $temp_c"
}

# Divide CPU ms by wall ms → "effective cores" used on average across the run.
eff_cores() {
  local cpu="$1" wall="$2"
  awk -v c="$cpu" -v w="$wall" 'BEGIN { if (w>0) printf "%.2f\n", c/w; else print "n/a" }'
}

average() {
  # Print the average of the stdin numbers.
  awk '{s+=$1; n++} END { if (n>0) printf "%.1f\n", s/n; else print 0 }'
}

check_ch

BASELINE_SHA="$(resolve_sha "$BASELINE_REF")"
BASELINE_IMAGE="${IMAGE_REPO}:${BASELINE_SHA:0:12}"
BASELINE_DESC="$BASELINE_REF (${BASELINE_SHA:0:7})"
if [[ -n "$CANDIDATE_REF" ]]; then
  CANDIDATE_SHA="$(resolve_sha "$CANDIDATE_REF")"
  CANDIDATE_IMAGE="${IMAGE_REPO}:${CANDIDATE_SHA:0:12}"
  CANDIDATE_DESC="$CANDIDATE_REF (${CANDIDATE_SHA:0:7})"
  if [[ "$CANDIDATE_SHA" == "$BASELINE_SHA" ]]; then
    echo "warning: baseline and candidate are the same commit (${BASELINE_SHA:0:7})" >&2
  fi
else
  CANDIDATE_IMAGE="${IMAGE_REPO}:worktree"
  CANDIDATE_DESC="working tree (on $(git -C "$PROJECT_ROOT" rev-parse --short HEAD))"
fi

build_commit_image "$BASELINE_SHA" "$BASELINE_IMAGE"
if [[ -n "$CANDIDATE_REF" ]]; then
  build_commit_image "$CANDIDATE_SHA" "$CANDIDATE_IMAGE"
else
  build_worktree_image "$CANDIDATE_IMAGE"
fi

echo
echo "baseline : $BASELINE_DESC -> $BASELINE_IMAGE"
echo "candidate: $CANDIDATE_DESC -> $CANDIDATE_IMAGE"

BASELINE_CFG=$(write_config "$DB_BASELINE")
CANDIDATE_CFG=$(write_config "$DB_CANDIDATE")
CONFIGS+=("$BASELINE_CFG" "$CANDIDATE_CFG")

# Freeze background merges so CH's CPU doesn't compete with the target for
# cores on this host. Re-enabled unconditionally on exit. This matches the
# production topology where CH runs on a separate host and merges don't steal
# target CPU.
ch_curl "SYSTEM STOP MERGES" >/dev/null
MERGES_STOPPED=1

declare -a BASELINE_WALL=() BASELINE_CPU=()
declare -a CANDIDATE_WALL=() CANDIDATE_CPU=()

printf "\n%-12s %-6s %-10s %-10s %-10s %-9s %-9s %-7s %-10s %-10s\n" \
  "version" "iter" "wall_ms" "cpu_ms" "eff_cores" "mhz_avg" "mhz_min" "temp_C" "rows" "tables"
printf '%s\n' "---------------------------------------------------------------------------------------------------"

record() {
  # $1 = label, $2 = iter, $3 = db, $4 = wall_ms, $5 = cpu_ms,
  # $6 = mhz_avg, $7 = mhz_min, $8 = temp_C
  local label="$1" iter="$2" db="$3" wall="$4" cpu="$5"
  local mhz_avg="$6" mhz_min="$7" temp_c="$8"
  printf "%-12s %-6s %-10s %-10s %-10s %-9s %-9s %-7s %-10s %-10s\n" \
    "$label" "$iter" "$wall" "$cpu" "$(eff_cores "$cpu" "$wall")" \
    "$mhz_avg" "$mhz_min" "$temp_c" \
    "$(total_rows "$db")" "$(table_count "$db")"
}

for i in $(seq 1 "$ITERATIONS"); do
  read -r b_wall b_cpu b_mhz_avg b_mhz_min b_temp < <(run_once "$BASELINE_IMAGE" "$DB_BASELINE" "$BASELINE_CFG")
  BASELINE_WALL+=("$b_wall"); BASELINE_CPU+=("$b_cpu")
  record "baseline" "$i" "$DB_BASELINE" "$b_wall" "$b_cpu" "$b_mhz_avg" "$b_mhz_min" "$b_temp"

  read -r c_wall c_cpu c_mhz_avg c_mhz_min c_temp < <(run_once "$CANDIDATE_IMAGE" "$DB_CANDIDATE" "$CANDIDATE_CFG")
  CANDIDATE_WALL+=("$c_wall"); CANDIDATE_CPU+=("$c_cpu")
  record "candidate" "$i" "$DB_CANDIDATE" "$c_wall" "$c_cpu" "$c_mhz_avg" "$c_mhz_min" "$c_temp"
done

summarize() {
  # $1 = label, $2 = wall array name, $3 = cpu array name
  local label="$1"
  local -n wall_arr="$2"
  local -n cpu_arr="$3"
  local w_avg c_avg
  w_avg=$(printf "%s\n" "${wall_arr[@]}" | average)
  c_avg=$(printf "%s\n" "${cpu_arr[@]}" | average)
  printf "%-12s average: wall=%s ms  cpu=%s ms  eff_cores=%s\n" \
    "$label" "$w_avg" "$c_avg" "$(eff_cores "$c_avg" "$w_avg")"
  # echo them back for the caller via globals
  eval "${label}_WALL_AVG=\"$w_avg\""
  eval "${label}_CPU_AVG=\"$c_avg\""
}

echo
summarize "baseline" BASELINE_WALL BASELINE_CPU
summarize "candidate" CANDIDATE_WALL CANDIDATE_CPU

wall_ratio=$(awk -v b="${baseline_WALL_AVG:-0}" -v c="${candidate_WALL_AVG:-0}" \
  'BEGIN { if (c>0) printf "%.2fx\n", b/c; else print "n/a" }')
cpu_ratio=$(awk -v b="${baseline_CPU_AVG:-0}" -v c="${candidate_CPU_AVG:-0}" \
  'BEGIN { if (c>0) printf "%.2fx\n", b/c; else print "n/a" }')
echo "wall speedup (baseline / candidate): $wall_ratio   (>1 means the candidate is faster)"
echo "cpu  ratio   (baseline / candidate): $cpu_ratio   (>1 means the candidate burns less CPU)"

echo
parity_report="$("$PROJECT_ROOT/scripts/compare-databases.sh" \
  --db-a "$DB_BASELINE" --db-b "$DB_CANDIDATE" \
  --ch-host "$CH_HOST" --ch-port "$CH_PORT" --ch-user "$CH_USER" --ch-password "$CH_PASSWORD" 2>&1)" \
  && parity_rc=0 || parity_rc=$?
if [[ "$parity_rc" == 0 ]]; then
  echo "content parity: OK ($(printf '%s\n' "$parity_report" | tail -n 1))"
else
  echo "content parity: MISMATCH"
  printf '%s\n' "$parity_report" | sed 's/^/  /'
fi

echo
echo "(container stderr of the last run captured at $LOG_FILE)"
exit "$parity_rc"
