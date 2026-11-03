#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose)
RESET_SERVICES=(mongo rabbitmq redis receiver_api worker)
RUNNER_PATH=/dp3/scripts/run_api_test_shard.py
TEST_API_DIR=tests/test_api

# Shard B is listed explicitly; shard A gets every other test module.
# Keep both shards at roughly equal wall time (the shard runner prints
# per-module durations at the end of each shard's output).
PARALLEL_SHARD_B=(
  test_garbage_collector
  test_get_distinct_attr_values
  test_telemetry
)

PARALLEL_SHARD_A=()

build_shard_a() {
  local b_module
  local module_path
  local module_name
  declare -A b_modules=()

  for b_module in "${PARALLEL_SHARD_B[@]}"; do
    b_modules["$b_module"]=1
  done

  mapfile -t PARALLEL_SHARD_A < <(
    find "$TEST_API_DIR" -maxdepth 1 -type f -name 'test_*.py' \
      | sort \
      | while read -r module_path; do
          module_name="${module_path##*/}"
          module_name="${module_name%.py}"
          if [[ -z "${b_modules[$module_name]:-}" ]]; then
            printf '%s\n' "$module_name"
          fi
        done
  )
}

cleanup_stack() {
  # The Mongo image uses anonymous volumes for /data/db and /data/configdb.
  # Removing containers alone leaves prior test data behind and makes API tests flaky.
  "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

run_shard() {
  local log_file=$1
  shift

  local api_container_id
  api_container_id="$("${COMPOSE[@]}" ps -q receiver_api)"

  docker run \
    --rm \
    --volume "$ROOT_DIR/scripts/run_api_test_shard.py:$RUNNER_PATH:ro" \
    --env CONF_DIR=/dp3/tests/test_config \
    --network "container:${api_container_id}" \
    dp3_interpreter \
    python "$RUNNER_PATH" "$@" >"$log_file" 2>&1
}

if [[ "${1:-}" == "cleanup" ]]; then
  cleanup_stack
  exit 0
fi

build_shard_a

echo "Starting local API test run from $ROOT_DIR"
echo "Recreating: ${RESET_SERVICES[*]}"
echo "Parallel shard A: ${PARALLEL_SHARD_A[*]}"
echo "Parallel shard B: ${PARALLEL_SHARD_B[*]}"

cleanup_stack

# Build the interpreter and RabbitMQ images in parallel.
docker build -f docker/python/Dockerfile --target base -t dp3_interpreter . &
docker build -f docker/rabbitmq/Dockerfile -t dp3_rabbitmq docker/rabbitmq &
wait

"${COMPOSE[@]}" up -d --force-recreate "${RESET_SERVICES[@]}"

sleep 10

shard_a_log="$(mktemp)"
shard_b_log="$(mktemp)"
cleanup_logs() {
  rm -f "$shard_a_log" "$shard_b_log"
}
trap cleanup_logs EXIT

set +e
run_shard "$shard_a_log" "${PARALLEL_SHARD_A[@]}" &
shard_a_pid=$!
run_shard "$shard_b_log" "${PARALLEL_SHARD_B[@]}" &
shard_b_pid=$!

wait $shard_a_pid
shard_a_status=$?
wait $shard_b_pid
shard_b_status=$?
set -e

cat "$shard_a_log"
cat "$shard_b_log"

status=0
if [[ $shard_a_status -ne 0 || $shard_b_status -ne 0 ]]; then
  status=1
  echo "API tests failed; recent service logs follow." >&2
  "${COMPOSE[@]}" logs --tail=200 "${RESET_SERVICES[@]}" >&2 || true
fi

exit "$status"
