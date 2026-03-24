#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  profile_callgrind.sh [-t seconds] [-o name] -- <command> [args...]

Examples:
  profile_callgrind.sh -t 30 -o datanode -- target/release/datanode --config-path services/datanode/config.toml -d testing/datanode1
  profile_callgrind.sh -t 20 -o iozone -- iozone -i 0 -i 1 -r 1m -s 256m -f /tmp/DAFS/testfile

Notes:
  - Generates callgrind output under profiling/.
  - Open with: kcachegrind <output-file>
EOF
}

TIMEOUT_SECS=30
OUT_NAME="callgrind"

while getopts ":t:o:h" opt; do
  case "$opt" in
    t) TIMEOUT_SECS="$OPTARG" ;;
    o) OUT_NAME="$OPTARG" ;;
    h) usage; exit 0 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

if [[ "$#" -lt 2 || "$1" != "--" ]]; then
  echo "Error: command must be provided after --" >&2
  usage
  exit 2
fi
shift

if ! command -v valgrind >/dev/null 2>&1; then
  echo "Error: valgrind is not installed" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$ROOT_DIR/profiling"
mkdir -p "$OUT_DIR"
TS="$(date +%Y%m%d-%H%M%S)"
OUT_FILE="$OUT_DIR/${OUT_NAME}-${TS}.callgrind.out"

CMD=("$@")
echo "Running under callgrind for up to ${TIMEOUT_SECS}s..."
timeout "$TIMEOUT_SECS" valgrind \
  --tool=callgrind \
  --trace-children=yes \
  --callgrind-out-file="$OUT_FILE" \
  "${CMD[@]}" || true

echo
echo "Done."
echo "callgrind output: $OUT_FILE"
echo "Open with: kcachegrind $OUT_FILE"
