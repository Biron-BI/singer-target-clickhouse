#!/usr/bin/env bash
# Compare the contents of two ClickHouse databases — typically the two outputs
# of scripts/benchmark.sh (bench_kotlin vs bench_ts) — to verify content parity.
#
# For each table present in both databases the script compares:
#   - row count (after ReplacingMergeTree dedup via FINAL)
#   - content hash: sum(cityHash64(col1, col2, …)) over all user columns
#
# The internal version columns (`_ver`, `_root_ver`) are excluded: two runs can
# legitimately assign different version sequences while producing the same
# user-visible data. sum() of a commutative hash is insensitive to row ordering.
#
# Usage: scripts/compare-databases.sh [options]
#
# Options:
#   --db-a <name>       First database (default: bench_kotlin)
#   --db-b <name>       Second database (default: bench_ts)
#   --ch-host <host>    ClickHouse host (default: localhost)
#   --ch-port <port>    ClickHouse HTTP port (default: 8123)
#   --ch-user <user>    ClickHouse user (default: default)
#   --ch-password <pw>  ClickHouse password (default: empty)
#   -v | --verbose      Print hashes and per-column details on mismatch
#   -h | --help         Show this help
#
# Exit code: 0 if every common table matches and no table is missing from
# either side, 1 otherwise.

set -euo pipefail
export LC_ALL=C

DB_A="bench_kotlin"
DB_B="bench_ts"
CH_HOST="localhost"
CH_PORT="8123"
CH_USER="default"
CH_PASSWORD=""
VERBOSE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db-a) DB_A="$2"; shift 2 ;;
    --db-b) DB_B="$2"; shift 2 ;;
    --ch-host) CH_HOST="$2"; shift 2 ;;
    --ch-port) CH_PORT="$2"; shift 2 ;;
    --ch-user) CH_USER="$2"; shift 2 ;;
    --ch-password) CH_PASSWORD="$2"; shift 2 ;;
    -v|--verbose) VERBOSE=1; shift ;;
    -h|--help)
      sed -n '1,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    -*)
      echo "unknown option: $1" >&2
      exit 2
      ;;
    *)
      echo "unexpected argument: $1" >&2
      exit 2
      ;;
  esac
done

ch_curl() {
  local query="$1"
  local auth=""
  if [[ -n "$CH_PASSWORD" ]]; then
    auth="-u ${CH_USER}:${CH_PASSWORD}"
  elif [[ "$CH_USER" != "default" ]]; then
    auth="-u ${CH_USER}:"
  fi
  # shellcheck disable=SC2086
  curl -sSf $auth -X POST "http://${CH_HOST}:${CH_PORT}/" --data-binary "$query"
}

require_db() {
  local db="$1"
  if [[ -z "$(ch_curl "SELECT count() FROM system.databases WHERE name = '$db'" | tr -d '[:space:]')" ]]; then
    echo "could not query system.databases" >&2
    exit 1
  fi
  local exists
  exists="$(ch_curl "SELECT count() FROM system.databases WHERE name = '$db'" | tr -d '[:space:]')"
  if [[ "$exists" != "1" ]]; then
    echo "database not found: $db" >&2
    exit 1
  fi
}

list_tables() {
  local db="$1"
  ch_curl "SELECT name FROM system.tables
           WHERE database = '$db' AND engine LIKE '%MergeTree%'
           ORDER BY name FORMAT TSVRaw"
}

# Columns used in the content hash. Excludes per-impl versioning columns.
list_hash_columns() {
  local db="$1" table="$2"
  ch_curl "SELECT name FROM system.columns
           WHERE database = '$db' AND table = '$table'
             AND name NOT IN ('_ver', '_root_ver')
           ORDER BY name FORMAT TSVRaw"
}

# FINAL is rejected by plain MergeTree (only Replacing/Collapsing/... support it).
# Return "FINAL" only when the table engine accepts it.
final_modifier() {
  local db="$1" table="$2"
  local engine
  engine="$(ch_curl "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'" | tr -d '[:space:]')"
  case "$engine" in
    *Replacing*|*Collapsing*|*Summing*|*Aggregating*) printf 'FINAL' ;;
    *) printf '' ;;
  esac
}

# "<count>\t<hash>" for a table, over the provided comma-separated backticked columns.
table_digest() {
  local db="$1" table="$2" cols_csv="$3"
  local fin
  fin="$(final_modifier "$db" "$table")"
  if [[ -z "$cols_csv" ]]; then
    # Empty schema: just return count with a fixed hash placeholder.
    local n
    n="$(ch_curl "SELECT count() FROM \`$db\`.\`$table\` $fin" | tr -d '[:space:]')"
    printf '%s\t%s\n' "$n" "0"
  else
    ch_curl "SELECT count(), toString(sum(cityHash64($cols_csv)))
             FROM \`$db\`.\`$table\` $fin FORMAT TSV"
  fi
}

require_db "$DB_A"
require_db "$DB_B"

mapfile -t TABLES_A < <(list_tables "$DB_A")
mapfile -t TABLES_B < <(list_tables "$DB_B")

declare -A SB=()
for t in "${TABLES_B[@]}"; do SB["$t"]=1; done
declare -A SA=()
for t in "${TABLES_A[@]}"; do SA["$t"]=1; done

only_a=(); only_b=(); common=()
for t in "${TABLES_A[@]}"; do
  if [[ -n "${SB[$t]:-}" ]]; then common+=("$t"); else only_a+=("$t"); fi
done
for t in "${TABLES_B[@]}"; do
  if [[ -z "${SA[$t]:-}" ]]; then only_b+=("$t"); fi
done

echo "comparing [$DB_A] (${#TABLES_A[@]} tables) vs [$DB_B] (${#TABLES_B[@]} tables)"
echo

if [[ ${#only_a[@]} -gt 0 ]]; then
  echo "tables only in $DB_A:"
  printf '  - %s\n' "${only_a[@]}"
  echo
fi
if [[ ${#only_b[@]} -gt 0 ]]; then
  echo "tables only in $DB_B:"
  printf '  - %s\n' "${only_b[@]}"
  echo
fi

matched=0
mismatched=0

printf "%-45s %-20s %-6s %s\n" "table" "rows (A | B)" "match" "notes"
printf -- '-%.0s' {1..95}; echo

for t in "${common[@]}"; do
  cols_a_nl="$(list_hash_columns "$DB_A" "$t")"
  cols_b_nl="$(list_hash_columns "$DB_B" "$t")"

  if [[ "$cols_a_nl" != "$cols_b_nl" ]]; then
    printf "%-45s %-20s %-6s %s\n" "$t" "-" "no" "column set differs"
    if [[ "$VERBOSE" == "1" ]]; then
      diff <(echo "$cols_a_nl") <(echo "$cols_b_nl") | sed 's/^/    /'
    fi
    mismatched=$((mismatched + 1))
    continue
  fi

  cols_csv=""
  while IFS= read -r col; do
    [[ -z "$col" ]] && continue
    cols_csv+="\`$col\`,"
  done <<< "$cols_a_nl"
  cols_csv="${cols_csv%,}"

  digest_a="$(table_digest "$DB_A" "$t" "$cols_csv")"
  digest_b="$(table_digest "$DB_B" "$t" "$cols_csv")"

  IFS=$'\t' read -r count_a hash_a <<< "$digest_a"
  IFS=$'\t' read -r count_b hash_b <<< "$digest_b"

  if [[ "$count_a" == "$count_b" && "$hash_a" == "$hash_b" ]]; then
    printf "%-45s %-20s %-6s\n" "$t" "$count_a | $count_b" "yes"
    matched=$((matched + 1))
  else
    note=""
    [[ "$count_a" != "$count_b" ]] && note+="count differs. "
    [[ "$hash_a"  != "$hash_b"  ]] && note+="content differs. "
    printf "%-45s %-20s %-6s %s\n" "$t" "$count_a | $count_b" "no" "$note"
    if [[ "$VERBOSE" == "1" ]]; then
      echo "    hash A=$hash_a"
      echo "    hash B=$hash_b"
    fi
    mismatched=$((mismatched + 1))
  fi
done

echo
echo "summary: matched=$matched mismatched=$mismatched only_in_${DB_A}=${#only_a[@]} only_in_${DB_B}=${#only_b[@]}"

if [[ $mismatched -gt 0 || ${#only_a[@]} -gt 0 || ${#only_b[@]} -gt 0 ]]; then
  exit 1
fi
