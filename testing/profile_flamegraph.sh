#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  profile_flamegraph.sh [-d seconds] [-f freq] [-o name] [-b benchmark_script]

Examples:
  profile_flamegraph.sh
  profile_flamegraph.sh -d 45 -o iozone-run
  profile_flamegraph.sh -b testing/bechmark.py -o dfs-benchmark

Notes:
  - Requires perf, cargo and uv.
  - Cleans environment with `bash clean_env.sh`.
  - Launches ONLY master and datanode automatically.
  - fuse_daemon must be started manually by the user.
  - Waits until /tmp/DAFS is mounted before profiling.
  - Runs benchmark script and profiles while benchmark is running.
  - If inferno is not installed, script auto-clones FlameGraph tools into tools/FlameGraph.
  - Output files are written to profiling/.
  - Uses userspace-focused perf event (cpu-clock:u) and DWARF call stacks.
EOF
}

DURATION=30
FREQ=99
OUT_NAME="profile"
BENCHMARK_SCRIPT="testing/bechmark.py"
PERF_EVENT="cpu-clock:u"
CALL_GRAPH="dwarf,16384"

while getopts ":d:f:o:b:h" opt; do
  case "$opt" in
    d) DURATION="$OPTARG" ;;
    f) FREQ="$OPTARG" ;;
    o) OUT_NAME="$OPTARG" ;;
    b) BENCHMARK_SCRIPT="$OPTARG" ;;
    h) usage; exit 0 ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

if ! command -v perf >/dev/null 2>&1; then
  echo "Error: perf is not installed" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "Error: cargo is not installed" >&2
  exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "Error: uv is not installed" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$ROOT_DIR/profiling"
mkdir -p "$OUT_DIR"

MASTER_LOG="$OUT_DIR/master.profile.log"
DATANODE_LOG="$OUT_DIR/datanode.profile.log"

cleanup() {
  set +e
  pkill -f 'target/release/datanode' >/dev/null 2>&1 || true
  pkill -f 'target/release/master' >/dev/null 2>&1 || true
}
trap cleanup EXIT

cd "$ROOT_DIR"

if [[ ! -f "$BENCHMARK_SCRIPT" ]]; then
  echo "Error: benchmark script not found: $BENCHMARK_SCRIPT" >&2
  exit 1
fi

wait_for_pid() {
  local name="$1"
  local retries=60
  local delay=1
  local pid=""
  for _ in $(seq 1 "$retries"); do
    pid="$(pgrep -n "$name" || true)"
    if [[ -n "$pid" ]]; then
      echo "$pid"
      return 0
    fi
    sleep "$delay"
  done
  return 1
}

wait_for_master_tcp() {
  local retries=60
  local delay=1
  for _ in $(seq 1 "$retries"); do
    if timeout 1 bash -c 'cat < /dev/null > /dev/tcp/::1/9090' 2>/dev/null; then
      return 0
    fi
    sleep "$delay"
  done
  return 1
}

wait_for_mount() {
  local path="$1"
  local retries=180
  local delay=1
  for _ in $(seq 1 "$retries"); do
    if mount | grep -q " $path "; then
      return 0
    fi
    sleep "$delay"
  done
  return 1
}

wait_for_new_fuse_pid() {
  local old_pid="$1"
  local retries=180
  local delay=1
  local pid=""
  for _ in $(seq 1 "$retries"); do
    pid="$(pgrep -n fuse_daemon || true)"
    if [[ -n "$pid" && "$pid" != "$old_pid" ]]; then
      echo "$pid"
      return 0
    fi
    sleep "$delay"
  done
  return 1
}

best_effort_unmount() {
  local path="$1"
  if mount | grep -q " $path "; then
    fusermount3 -u "$path" >/dev/null 2>&1 || \
      fusermount -u "$path" >/dev/null 2>&1 || \
      umount "$path" >/dev/null 2>&1 || \
      umount -l "$path" >/dev/null 2>&1 || true
  fi
}

echo "[0/6] Cleaning previous environment"
bash clean_env.sh

# Ensure old service processes do not interfere with this run.
cleanup

echo "[1/6] Building release binaries (debug symbols + frame pointers for better perf symbols)"
CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=1 RUSTFLAGS="-C force-frame-pointers=yes" \
  cargo build --release --bin master --bin datanode --bin fuse_daemon >/dev/null

echo "[2/6] Starting master"
nohup target/release/master \
  --config-path services/master/config-node0.toml \
  --raft-bootstrap true \
  --raft-node-id 0 \
  -l "[::1]:9090" \
  > "$MASTER_LOG" 2>&1 &

echo "[3/6] Starting datanode"
nohup target/release/datanode \
  --config-path services/datanode/config.toml \
  -d testing/datanode1 \
  > "$DATANODE_LOG" 2>&1 &

MASTER_PID="$(wait_for_pid master)" || {
  echo "Error: master did not start. Check $MASTER_LOG" >&2
  exit 1
}
DATANODE_PID="$(wait_for_pid datanode)" || {
  echo "Error: datanode did not start. Check $DATANODE_LOG" >&2
  exit 1
}

echo "[4/6] Waiting for master to be reachable on [::1]:9090"
wait_for_master_tcp || {
  echo "Error: master is not reachable on [::1]:9090. Check $MASTER_LOG" >&2
  exit 1
}

OLD_FUSE_PID="$(pgrep -n fuse_daemon || true)"
best_effort_unmount "/tmp/DAFS"

echo "[4/6] Please start fuse_daemon manually in another terminal"
echo "      Command: CARGO_PROFILE_RELEASE_STRIP=none CARGO_PROFILE_RELEASE_DEBUG=1 RUSTFLAGS='-C force-frame-pointers=yes' cargo run --release --bin fuse_daemon -- -m http://[::1]:9090 --access-token-path .token"
echo "      Waiting for a NEW fuse_daemon process and /tmp/DAFS mount..."
FUSE_PID="$(wait_for_new_fuse_pid "$OLD_FUSE_PID" || true)"
if [[ -z "$FUSE_PID" ]]; then
  echo "Error: did not detect a new fuse_daemon process in time." >&2
  exit 1
fi

wait_for_mount "/tmp/DAFS" || {
  echo "Error: fuse_daemon started (pid=$FUSE_PID) but /tmp/DAFS is not mounted." >&2
  exit 1
}

PIDS="${MASTER_PID},${DATANODE_PID},${FUSE_PID}"

TS="$(date +%Y%m%d-%H%M%S)"
PERF_DATA="$OUT_DIR/${OUT_NAME}-${TS}.perf.data"
FOLDED="$OUT_DIR/${OUT_NAME}-${TS}.folded"
SVG="$OUT_DIR/${OUT_NAME}-${TS}.svg"

echo "[5/6] Recording perf for PIDs: $PIDS (event=${PERF_EVENT}, freq=${FREQ}Hz, call-graph=${CALL_GRAPH})"
perf record \
  -e "$PERF_EVENT" \
  -F "$FREQ" \
  --call-graph "$CALL_GRAPH" \
  -p "$PIDS" \
  -o "$PERF_DATA" \
  -- sleep 1000000 &
PERF_PID=$!

echo "[5/6] Running benchmark script: $BENCHMARK_SCRIPT"
if [[ "$DURATION" -gt 0 ]]; then
  timeout "$DURATION" uv run --with pandas --with plotly "$BENCHMARK_SCRIPT" || true
else
  uv run --with pandas --with plotly "$BENCHMARK_SCRIPT"
fi

# Ask perf to stop and flush data to perf.data.
kill -INT "$PERF_PID" >/dev/null 2>&1 || true
wait "$PERF_PID" >/dev/null 2>&1 || true

if command -v inferno-collapse-perf >/dev/null 2>&1 && command -v inferno-flamegraph >/dev/null 2>&1; then
  echo "[6/6] Collapsing stacks with inferno"
  perf script -i "$PERF_DATA" | inferno-collapse-perf > "$FOLDED"
  echo "[6/6] Rendering flamegraph"
  inferno-flamegraph < "$FOLDED" > "$SVG"
else
  TOOLS_DIR="$ROOT_DIR/tools/FlameGraph"
  if [[ ! -x "$TOOLS_DIR/flamegraph.pl" ]]; then
    echo "[6/6] Cloning FlameGraph tools into $TOOLS_DIR"
    mkdir -p "$ROOT_DIR/tools"
    if [[ -d "$TOOLS_DIR/.git" ]]; then
      git -C "$TOOLS_DIR" pull --ff-only
    else
      rm -rf "$TOOLS_DIR"
      git clone --depth 1 https://github.com/brendangregg/FlameGraph "$TOOLS_DIR"
    fi
  fi
  echo "[6/6] Collapsing stacks"
  perf script -i "$PERF_DATA" | "$TOOLS_DIR/stackcollapse-perf.pl" > "$FOLDED"
  echo "[6/6] Rendering flamegraph"
  "$TOOLS_DIR/flamegraph.pl" "$FOLDED" > "$SVG"
fi

echo
echo "Done."
echo "perf data:    $PERF_DATA"
echo "folded:       $FOLDED"
echo "flamegraph:   $SVG"
echo "master log:   $MASTER_LOG"
echo "datanode log: $DATANODE_LOG"
