#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLI=(cargo run --release --bin cli -- --config-path client/cli/config.toml)
TOKEN_PATH="client/cli/.token"
NO_TOKEN_PATH="testing/.missing_token_for_no_auth_checks"

MASTER_LOG="testing/master_test.log"
DATANODE_LOG="testing/datanode_test.log"
DATANODE2_LOG="testing/datanode2_test.log"
MASTER_PID=""
DATANODE_PID=""
DATANODE2_PID=""

if [ -f "./clean_env.ss" ]; then
	CLEAN_SCRIPT="./clean_env.ss"
elif [ -f "./clean_env.sh" ]; then
	CLEAN_SCRIPT="./clean_env.sh"
else
	CLEAN_SCRIPT=""
fi

TEST_ID="client_e2e_$(date +%s)"
USERNAME="${TEST_ID}_user"
PASSWORD="${TEST_ID}_pass"

MASTER_BLOCK_SIZE=$(awk -F= '/^[[:space:]]*block_size[[:space:]]*=/{gsub(/[[:space:]]/,"",$2); print $2; exit}' services/master/config.toml)
MASTER_BLOCK_SIZE="${MASTER_BLOCK_SIZE:-65536}"
TEST_FILE_SIZE=$((MASTER_BLOCK_SIZE * 3 + 12345))
EXPECTED_BLOCKS=$(((TEST_FILE_SIZE + MASTER_BLOCK_SIZE - 1) / MASTER_BLOCK_SIZE))

LOCAL_FILE="testing/${TEST_ID}.bin"
REMOTE_FILE="$LOCAL_FILE"
RENAMED_REMOTE_FILE="testing/${TEST_ID}_renamed.bin"
DOWNLOADED_FILE="testing/${TEST_ID}.download.bin"

cleanup() {
	set +e

	if [ -n "$DATANODE_PID" ] && kill -0 "$DATANODE_PID" 2>/dev/null; then
		kill "$DATANODE_PID" 2>/dev/null || true
		wait "$DATANODE_PID" 2>/dev/null || true
	fi

	if [ -n "$DATANODE2_PID" ] && kill -0 "$DATANODE2_PID" 2>/dev/null; then
		kill "$DATANODE2_PID" 2>/dev/null || true
		wait "$DATANODE2_PID" 2>/dev/null || true
	fi

	if [ -n "$MASTER_PID" ] && kill -0 "$MASTER_PID" 2>/dev/null; then
		kill "$MASTER_PID" 2>/dev/null || true
		wait "$MASTER_PID" 2>/dev/null || true
	fi

	if [ -n "$CLEAN_SCRIPT" ]; then
		bash "$CLEAN_SCRIPT" >/dev/null 2>&1 || true
	fi

	rm -f "$LOCAL_FILE" "$DOWNLOADED_FILE" "$NO_TOKEN_PATH"
}
trap cleanup EXIT

pass() {
	echo "✅ $1"
}

dump_service_logs() {
	echo "--- master log (tail) ---"
	tail -n 80 "$MASTER_LOG" 2>/dev/null || true
	echo "--- datanode log (tail) ---"
	tail -n 80 "$DATANODE_LOG" 2>/dev/null || true
	echo "--- datanode2 log (tail) ---"
	tail -n 80 "$DATANODE2_LOG" 2>/dev/null || true
}

fail() {
	echo "❌ $1"
	dump_service_logs
	exit 1
}

start_services() {
	echo "== 0) Cleaning previous environment =="
	if [ -n "$CLEAN_SCRIPT" ]; then
		bash "$CLEAN_SCRIPT"
	else
		echo "⚠️ clean_env.ss/clean_env.sh not found; continuing without pre-clean"
	fi

	echo "== Starting master =="
	cargo run --release --bin master -- --config-path services/master/config.toml >"$MASTER_LOG" 2>&1 &
	MASTER_PID=$!

	echo "== Starting datanode =="
	cargo run --release --bin datanode -- --config-path services/datanode/config.toml -d testing/datanode1 >"$DATANODE_LOG" 2>&1 &
	DATANODE_PID=$!

	echo "== Starting datanode2 =="
	cargo run --release --bin datanode -- --config-path services/datanode/config.toml --listen-address "[::1]:8081" -d testing/datanode2 >"$DATANODE2_LOG" 2>&1 &
	DATANODE2_PID=$!

	echo "== Waiting for services startup =="
	local stable_checks=0
	for _ in {1..30}; do
		if ! kill -0 "$MASTER_PID" 2>/dev/null; then
			dump_service_logs
			fail "Master exited during startup"
		fi

		if ! kill -0 "$DATANODE_PID" 2>/dev/null; then
			dump_service_logs
			fail "Datanode exited during startup"
		fi

		if ! kill -0 "$DATANODE2_PID" 2>/dev/null; then
			dump_service_logs
			fail "Datanode2 exited during startup"
		fi

		stable_checks=$((stable_checks + 1))
		if [ "$stable_checks" -ge 3 ]; then
			pass "Services started"
			return
		fi

		sleep 1
	done

	dump_service_logs
	fail "Timeout waiting for master/datanodes to be ready"
}

collect_block_ids() {
	local datanode_dir="$1"
	find "$datanode_dir" -type f -printf '%f\n' 2>/dev/null | sort -u
}

wait_for_replication() {
	local filename
	filename=$(basename "$RENAMED_REMOTE_FILE")

	echo "== Waiting for asynchronous replication =="
	for _ in {1..30}; do
		if output=$("${CLI[@]}" list --path testing 2>&1); then
			if grep -q "$filename" <<< "$output" && grep -q "Replicated" <<< "$output"; then
				pass "Replicated status confirmed"
				return
			fi
		fi
		sleep 1
	done

	fail "Timeout waiting for Replicated status for $filename"
}

assert_replication_integrity() {
	mapfile -t blocks_node1 < <(collect_block_ids testing/datanode1)
	mapfile -t blocks_node2 < <(collect_block_ids testing/datanode2)

	local count1=${#blocks_node1[@]}
	local count2=${#blocks_node2[@]}

	if [ "$count1" -ne "$EXPECTED_BLOCKS" ]; then
		fail "Datanode1 has $count1 blocks, expected $EXPECTED_BLOCKS"
	fi

	if [ "$count2" -ne "$EXPECTED_BLOCKS" ]; then
		fail "Datanode2 has $count2 blocks, expected $EXPECTED_BLOCKS"
	fi

	for block_id in "${blocks_node1[@]}"; do
		if ! grep -Fxq "$block_id" <(printf '%s\n' "${blocks_node2[@]}"); then
			fail "Block not replicated on datanode2: $block_id"
		fi
	done

	pass "Replication verified for all blocks ($EXPECTED_BLOCKS blocks)"
}

run_cli_expect_success() {
	local name="$1"
	shift
	local output

	if output=$("${CLI[@]}" "$@" 2>&1); then
		pass "$name"
	else
		echo "$output"
		fail "$name"
	fi
}

run_cli_expect_login_error() {
	local name="$1"
	shift
	local output

	rm -f "$NO_TOKEN_PATH"

	if output=$("${CLI[@]}" --access-token-path "$NO_TOKEN_PATH" "$@" 2>&1); then
		echo "$output"
		fail "$name (should fail without login)"
	fi

	if grep -Eq "Cannot read access token|Please log in first|register with 'register' command" <<< "$output"; then
		pass "$name"
	else
		echo "$output"
		fail "$name (failed, but not due to missing login)"
	fi
}

echo "== Preparing test data =="
start_services
truncate -s "$TEST_FILE_SIZE" "$LOCAL_FILE"

echo "== 1) Validate behavior without login =="
rm -f "$TOKEN_PATH"
run_cli_expect_login_error "Upload without login" upload --file "$LOCAL_FILE"
run_cli_expect_login_error "Download without login" download --remote-path "$REMOTE_FILE" --output "$DOWNLOADED_FILE"

echo "== 2) Register and login user =="
run_cli_expect_success "Register" register --username "$USERNAME" --password "$PASSWORD"
rm -f "$TOKEN_PATH"
run_cli_expect_success "Login" login --username "$USERNAME" --password "$PASSWORD"
[ -s "$TOKEN_PATH" ] || fail "Token was not generated after login"
pass "Token generated after login"

echo "== 3) Authenticated flows =="
run_cli_expect_success "Upload" upload --file "$LOCAL_FILE"
run_cli_expect_success "List" list --path testing
run_cli_expect_success "Rename" rename --old-path "$REMOTE_FILE" --new-path "$RENAMED_REMOTE_FILE"
wait_for_replication
assert_replication_integrity
run_cli_expect_success "Download" download --remote-path "$RENAMED_REMOTE_FILE" --output "$DOWNLOADED_FILE"

cmp -s "$LOCAL_FILE" "$DOWNLOADED_FILE" || fail "Downloaded file does not match the original"
pass "Downloaded file integrity"
rm -f testing/master_test.log testing/datanode_test.log testing/datanode2_test.log
echo "🎉 Suite completed successfully"