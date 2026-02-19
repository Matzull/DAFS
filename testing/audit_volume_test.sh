#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLI_BIN="target/release/cli"
MASTER_BIN="target/release/master"
DATANODE_BIN="target/release/datanode"
CLI=("$CLI_BIN" --config-path client/cli/config.toml)

USERS="${USERS:-5}"
ITERATIONS="${ITERATIONS:-10}"
FILE_SIZE_BYTES="${FILE_SIZE_BYTES:-262144}"
START_SERVICES="${START_SERVICES:-1}"

RUN_ID="audit_vol_$(date +%s)"
BASE_DIR="testing/audit_volume/$RUN_ID"
TOKENS_DIR="testing/audit_tokens/$RUN_ID"
DOWNLOAD_DIR="testing/audit_downloads/$RUN_ID"
MASTER_LOG="testing/master_audit_volume.log"
DATANODE1_LOG="testing/datanode1_audit_volume.log"
DATANODE2_LOG="testing/datanode2_audit_volume.log"

MASTER_PID=""
DATANODE1_PID=""
DATANODE2_PID=""

if [ -f "./clean_env.ss" ]; then
	CLEAN_SCRIPT="./clean_env.ss"
elif [ -f "./clean_env.sh" ]; then
	CLEAN_SCRIPT="./clean_env.sh"
else
	CLEAN_SCRIPT=""
fi

mkdir -p "$BASE_DIR" "$TOKENS_DIR" "$DOWNLOAD_DIR"

pass() {
	echo "✅ $1"
}

progress() {
	echo "⏳ $1"
}

fail() {
	echo "❌ $1"
	dump_service_logs
	exit 1
}

dump_service_logs() {
	echo "--- master log (tail) ---"
	tail -n 80 "$MASTER_LOG" 2>/dev/null || true
	echo "--- datanode1 log (tail) ---"
	tail -n 80 "$DATANODE1_LOG" 2>/dev/null || true
	echo "--- datanode2 log (tail) ---"
	tail -n 80 "$DATANODE2_LOG" 2>/dev/null || true
}

cleanup() {
	set +e

	if [ -n "$DATANODE2_PID" ] && kill -0 "$DATANODE2_PID" 2>/dev/null; then
		kill "$DATANODE2_PID" 2>/dev/null || true
		wait "$DATANODE2_PID" 2>/dev/null || true
	fi

	if [ -n "$DATANODE1_PID" ] && kill -0 "$DATANODE1_PID" 2>/dev/null; then
		kill "$DATANODE1_PID" 2>/dev/null || true
		wait "$DATANODE1_PID" 2>/dev/null || true
	fi

	if [ -n "$MASTER_PID" ] && kill -0 "$MASTER_PID" 2>/dev/null; then
		kill "$MASTER_PID" 2>/dev/null || true
		wait "$MASTER_PID" 2>/dev/null || true
	fi

	if [ -n "$CLEAN_SCRIPT" ] && [ "$START_SERVICES" = "1" ]; then
		bash "$CLEAN_SCRIPT" >/dev/null 2>&1 || true
	fi

	rm -rf "$BASE_DIR" "$DOWNLOAD_DIR" "$TOKENS_DIR"
	rm -f "$MASTER_LOG" "$DATANODE1_LOG" "$DATANODE2_LOG"
	rmdir testing/audit_volume 2>/dev/null || true
	rmdir testing/audit_downloads 2>/dev/null || true
	rmdir testing/audit_tokens 2>/dev/null || true
}
trap cleanup EXIT

run_cli_with_token() {
	local token_path="$1"
	shift
	"${CLI[@]}" --access-token-path "$token_path" "$@"
}

build_binaries() {
	echo "== Building binaries once (release) =="
	cargo build --release --bin cli --bin master --bin datanode >/dev/null

	[ -x "$CLI_BIN" ] || fail "CLI binary not found at $CLI_BIN"
	[ -x "$MASTER_BIN" ] || fail "Master binary not found at $MASTER_BIN"
	[ -x "$DATANODE_BIN" ] || fail "Datanode binary not found at $DATANODE_BIN"

	pass "Binaries ready"
}

start_services() {
	if [ "$START_SERVICES" != "1" ]; then
		echo "== START_SERVICES=0, assuming services are already running =="
		return
	fi

	echo "== Cleaning environment before load test =="
	if [ -n "$CLEAN_SCRIPT" ]; then
		bash "$CLEAN_SCRIPT"
	else
		echo "⚠️ clean_env.ss/clean_env.sh not found; continuing without pre-clean"
	fi

	echo "== Starting master =="
	"$MASTER_BIN" --config-path services/master/config.toml >"$MASTER_LOG" 2>&1 &
	MASTER_PID=$!

	echo "== Starting datanode1 =="
	"$DATANODE_BIN" --config-path services/datanode/config.toml -d testing/datanode1 >"$DATANODE1_LOG" 2>&1 &
	DATANODE1_PID=$!

	echo "== Starting datanode2 =="
	"$DATANODE_BIN" --config-path services/datanode/config.toml --listen-address "[::1]:8081" -d testing/datanode2 >"$DATANODE2_LOG" 2>&1 &
	DATANODE2_PID=$!

	echo "== Waiting for services startup =="
	local stable_checks=0
	for _ in {1..40}; do
		if ! kill -0 "$MASTER_PID" 2>/dev/null; then
			fail "Master exited during startup"
		fi
		if ! kill -0 "$DATANODE1_PID" 2>/dev/null; then
			fail "Datanode1 exited during startup"
		fi
		if ! kill -0 "$DATANODE2_PID" 2>/dev/null; then
			fail "Datanode2 exited during startup"
		fi

		stable_checks=$((stable_checks + 1))
		if [ "$stable_checks" -ge 3 ]; then
			pass "Services started"
			return
		fi
		sleep 1
	done

	fail "Timeout waiting for services"
}

run_user_flow() {
	local user_idx="$1"
	local username="${RUN_ID}_user_${user_idx}"
	local password="${RUN_ID}_pass_${user_idx}"
	local token_path="$TOKENS_DIR/${username}.token"
	local user_dir="$BASE_DIR/user_${user_idx}"
	local user_download_dir="$DOWNLOAD_DIR/user_${user_idx}"
	local total_iterations="$ITERATIONS"

	progress "[$username] Starting user flow ($total_iterations iterations)"

	mkdir -p "$user_dir" "$user_download_dir"

	run_cli_with_token "$token_path" register --username "$username" --password "$password" >/dev/null
	run_cli_with_token "$token_path" login --username "$username" --password "$password" >/dev/null

	for iteration in $(seq 1 "$ITERATIONS"); do
		local local_file="$user_dir/file_${iteration}.bin"
		local renamed_remote="$user_dir/file_${iteration}_renamed.bin"
		local download_file="$user_download_dir/file_${iteration}.bin"

		progress "[$username] Iteration $iteration/$total_iterations"
		progress "[$username][it:$iteration] prepare local file"

		truncate -s "$FILE_SIZE_BYTES" "$local_file"
		progress "[$username][it:$iteration] local file ready ($FILE_SIZE_BYTES bytes)"

		progress "[$username][it:$iteration] login"
		run_cli_with_token "$token_path" login --username "$username" --password "$password" >/dev/null
		progress "[$username][it:$iteration] login done"

		progress "[$username][it:$iteration] upload"
		run_cli_with_token "$token_path" upload --file "$local_file" >/dev/null
		progress "[$username][it:$iteration] upload done"

		progress "[$username][it:$iteration] list"
		run_cli_with_token "$token_path" list --path "$user_dir" >/dev/null
		progress "[$username][it:$iteration] list done"

		progress "[$username][it:$iteration] rename"
		run_cli_with_token "$token_path" rename --old-path "$local_file" --new-path "$renamed_remote" >/dev/null
		progress "[$username][it:$iteration] rename done"

		progress "[$username][it:$iteration] wait for replication"
		local replicated=0

		for _ in {1..20}; do
			if out=$(run_cli_with_token "$token_path" list --path "$user_dir" 2>/dev/null); then
				if grep -q "file_${iteration}_renamed.bin" <<< "$out" && grep -q "Replicated" <<< "$out"; then
					replicated=1
					break
				fi
			fi
			sleep 1
		done

		if [ "$replicated" -eq 1 ]; then
			progress "[$username][it:$iteration] replication done"
		else
			fail "Replication timeout for $username iteration $iteration"
		fi

		progress "[$username][it:$iteration] download"

		run_cli_with_token "$token_path" download --remote-path "$renamed_remote" --output "$download_file" >/dev/null
		progress "[$username][it:$iteration] download done"

		progress "[$username][it:$iteration] integrity check"

		if ! cmp -s "$local_file" "$download_file"; then
			fail "Integrity mismatch for $username iteration $iteration"
		fi
		progress "[$username][it:$iteration] integrity check done"

		progress "[$username] Iteration $iteration/$total_iterations completed"
	done

	pass "User flow complete: $username ($ITERATIONS iterations)"
}

echo "== Audit volume test =="
echo "Run id: $RUN_ID"
echo "Users: $USERS | Iterations per user: $ITERATIONS | File size: $FILE_SIZE_BYTES bytes"

total_ops_per_iteration=6
total_ops=$((USERS * ITERATIONS * total_ops_per_iteration + USERS * 2))
echo "Estimated CLI operations: $total_ops"

build_binaries
start_services

pids=()
for user_idx in $(seq 1 "$USERS"); do
	run_user_flow "$user_idx" &
	pids+=("$!")
done

completed_users=0
for pid in "${pids[@]}"; do
	wait "$pid"
	completed_users=$((completed_users + 1))
	progress "Users completed: $completed_users/$USERS"
done

pass "Audit volume test completed successfully"
echo "Generated operations for audit logs with run id: $RUN_ID"
