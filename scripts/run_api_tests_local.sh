#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

COMPOSE=(docker compose)
RESET_SERVICES=(mongo rabbitmq redis receiver_api worker)
RUNNER_PATH=/dp3/scripts/run_api_test_shard.py
TEST_API_DIR=tests/test_api

PARALLEL_GC_SHARD=(
  test_garbage_collector
)

PARALLEL_NON_GC_SHARD=()

build_non_gc_shard() {
  local gc_module
  local module_path
  local module_name
  declare -A gc_modules=()

  for gc_module in "${PARALLEL_GC_SHARD[@]}"; do
    gc_modules["$gc_module"]=1
  done

  mapfile -t PARALLEL_NON_GC_SHARD < <(
    find "$TEST_API_DIR" -maxdepth 1 -type f -name 'test_*.py' \
      | sort \
      | while read -r module_path; do
          module_name="${module_path##*/}"
          module_name="${module_name%.py}"
          if [[ -z "${gc_modules[$module_name]:-}" ]]; then
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

build_non_gc_shard

echo "Starting local API test run from $ROOT_DIR"
echo "Recreating: ${RESET_SERVICES[*]}"
echo "Parallel shard A: ${PARALLEL_NON_GC_SHARD[*]}"
echo "Parallel shard B: ${PARALLEL_GC_SHARD[*]}"

cleanup_stack

# Build the interpreter and RabbitMQ images in parallel.
docker build -f docker/python/Dockerfile --target base -t dp3_interpreter . &
docker build -f docker/rabbitmq/Dockerfile -t dp3_rabbitmq docker/rabbitmq &
wait

"${COMPOSE[@]}" up -d --force-recreate "${RESET_SERVICES[@]}"

sleep 10

parallel_non_gc_log="$(mktemp)"
parallel_gc_log="$(mktemp)"
cleanup_logs() {
  rm -f "$parallel_non_gc_log" "$parallel_gc_log"
}
trap cleanup_logs EXIT

set +e
run_shard "$parallel_non_gc_log" "${PARALLEL_NON_GC_SHARD[@]}" &
non_gc_pid=$!
run_shard "$parallel_gc_log" "${PARALLEL_GC_SHARD[@]}" &
gc_pid=$!

wait $non_gc_pid
non_gc_status=$?
wait $gc_pid
gc_status=$?
set -e

cat "$parallel_non_gc_log"
cat "$parallel_gc_log"

status=0
if [[ $non_gc_status -ne 0 || $gc_status -ne 0 ]]; then
  status=1
  echo "API tests failed; recent service logs follow." >&2
  "${COMPOSE[@]}" logs --tail=200 "${RESET_SERVICES[@]}" >&2 || true
fi

exit "$status"
