#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_NAME="${DP3_RECOVERY_PROJECT:-dp3_worker_recovery_demo}"
LOG_DIR="${DP3_RECOVERY_LOG_DIR:-$ROOT_DIR/tests/worker_dependency_recovery_logs/$PROJECT_NAME}"
OUTAGE_SECONDS="${DP3_RECOVERY_OUTAGE_SECONDS:-15}"
RABBITMQ_OUTAGE_SECONDS="${DP3_RECOVERY_RABBITMQ_OUTAGE_SECONDS:-75}"
RECOVERY_TIMEOUT="${DP3_RECOVERY_TIMEOUT:-150}"
COMPOSE_FILE="$(mktemp)"
TRAFFIC_PID=""
TRAFFIC_CONTROL_FILE=""
TRAFFIC_LOG=""
SCENARIO_FAILURES=0
SCENARIO_RESULTS=()

write_compose_file() {
  cat >"$COMPOSE_FILE" <<YAML
services:
  rabbitmq:
    image: "dp3_rabbitmq"
    build: "$ROOT_DIR/docker/rabbitmq"
    environment:
      DP3_APP_NAME: test
      DP3_WORKER_NUM: 1

  mongo:
    image: mongo:latest
    environment:
      MONGO_INITDB_ROOT_USERNAME: test
      MONGO_INITDB_ROOT_PASSWORD: test

  redis:
    image: redis
    command: ["redis-server", "--appendonly", "yes"]

  receiver_api:
    image: "dp3_interpreter"
    build:
      context: "$ROOT_DIR"
      dockerfile: "docker/python/Dockerfile"
      target: "base"
    working_dir: "/dp3/dp3/api"
    environment:
      HOST: "0.0.0.0"
      APP_NAME: "test"
      CONF_DIR: "/dp3/tests/test_config"
    command: ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5000"]
    depends_on:
      - rabbitmq
      - mongo
      - redis

  worker:
    image: "dp3_interpreter"
    build:
      context: "$ROOT_DIR"
      dockerfile: "docker/python/Dockerfile"
      target: "base"
    working_dir: "/dp3/"
    command:
      - /bin/sh
      - -c
      - |
        while true; do
          echo "mock-supervisor: starting worker"
          dp3 worker test /dp3/tests/test_config 0
          code=\$\$?
          echo "mock-supervisor: worker exited with code \$\${code}"
          if [ "\$\${code}" -eq 0 ]; then
            echo "mock-supervisor: clean worker exit, stopping supervisor loop"
            exit 0
          fi
          echo "mock-supervisor: restarting worker after failure"
          sleep 2
        done
    depends_on:
      - rabbitmq
      - mongo
      - redis
YAML
}

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

step() {
  printf '[%(%Y-%m-%dT%H:%M:%S%z)T] %s\n' -1 "$*" >&2
}
usage() {
  cat <<EOF
Usage: $0 [run|cleanup|logs|status]

Runs a manual worker dependency recovery demonstration with one DP3 worker,
the test API, and isolated Docker Compose dependencies.

The demo sends datapoint traffic while dependencies are unavailable. Idle
workers do not necessarily observe Redis or MongoDB outages; the traffic is what
exercises task processing and makes the expected recovery/failure modes visible.
The worker runs under a small shell-based mock supervisor that restarts it after
non-zero exits and logs each restart.

Environment:
  DP3_RECOVERY_PROJECT         Compose project name (default: $PROJECT_NAME)
  DP3_RECOVERY_LOG_DIR         Log output directory (default: $LOG_DIR)
  DP3_RECOVERY_OUTAGE_SECONDS           Redis/MongoDB outage duration (default: $OUTAGE_SECONDS)
  DP3_RECOVERY_RABBITMQ_OUTAGE_SECONDS RabbitMQ outage duration (default: $RABBITMQ_OUTAGE_SECONDS)
  DP3_RECOVERY_TIMEOUT                  Wait timeout for recovery checks (default: $RECOVERY_TIMEOUT)

Subcommands:
  run      Build/start the demo stack and run recovery checks (default)
  status   Show current demo containers
  logs     Save and print recent demo logs
  cleanup  Remove demo containers, networks, and volumes
EOF
}

cleanup_stack() {
  step "Cleaning up Docker Compose project '$PROJECT_NAME'"
  compose down -v --remove-orphans
}

save_logs() {
  mkdir -p "$LOG_DIR"
  compose logs --no-color >"$LOG_DIR/compose.log" 2>&1 || true
  compose ps --all >"$LOG_DIR/compose.ps" 2>&1 || true
  for service in rabbitmq mongo redis receiver_api worker; do
    compose logs --no-color "$service" >"$LOG_DIR/${service}.log" 2>&1 || true
  done
  {
    echo "# DP3 worker dependency recovery demo summary"
    echo
    echo "## Containers"
    cat "$LOG_DIR/compose.ps" 2>/dev/null || true
    echo
    echo "## Scenario results"
    if (( ${#SCENARIO_RESULTS[@]} > 0 )); then
      printf '%s\n' "${SCENARIO_RESULTS[@]}"
    else
      echo "No scenarios recorded."
    fi
    echo
    echo "## Per-scenario worker/API logs"
    find "$LOG_DIR/scenarios" -maxdepth 1 -type f -name '*.log' -print 2>/dev/null | sort || true
    echo
    echo "## Mock supervisor worker restarts"
    grep 'mock-supervisor:' "$LOG_DIR/worker.log" 2>/dev/null || true
    echo
    echo "## Worker fatal/error lines"
    grep -E "Unhandled worker error|Finished, main thread exiting with code|Forcing shutdown|successfully connected|Redis|Mongo|DB error|RabbitMQ" "$LOG_DIR/worker.log" 2>/dev/null || true
    echo
    echo "## Recent traffic"
    if [[ -n "$TRAFFIC_LOG" && -f "$TRAFFIC_LOG" ]]; then
      tail -n 80 "$TRAFFIC_LOG"
    fi
  } >"$LOG_DIR/recovery_summary.log"
  step "Saved demo logs to $LOG_DIR"
  step "Summary: $LOG_DIR/recovery_summary.log"
}

print_status() {
  compose ps --all
}

print_logs() {
  save_logs
  tail -n 300 "$LOG_DIR/compose.log" || true
}

wait_for_exec() {
  local service=$1
  local description=$2
  shift 2

  step "Waiting for $description"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  until compose exec -T "$service" "$@" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $description" >&2
      return 1
    fi
    sleep 2
  done
}

wait_for_rabbitmq_queues() {
  step "Waiting for DP3 RabbitMQ queues"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  until compose exec -T rabbitmq sh -c '
    rabbitmqadmin list queues name -f tsv | grep -qx test-worker-0 &&
    rabbitmqadmin list queues name -f tsv | grep -qx test-worker-0-pri &&
    rabbitmqadmin list queues name -f tsv | grep -qx test-worker-0-snapshots &&
    rabbitmqadmin list queues name -f tsv | grep -qx test-worker-0-control
  ' >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for DP3 RabbitMQ queues" >&2
      compose logs --tail=100 rabbitmq >&2 || true
      return 1
    fi
    sleep 2
  done
}

wait_for_api() {
  step "Waiting for receiver API"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  until compose exec -T receiver_api python - <<'PY' >/dev/null 2>&1
import requests
requests.get("http://127.0.0.1:5000", timeout=2).raise_for_status()
PY
  do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for receiver API" >&2
      compose logs --tail=100 receiver_api >&2 || true
      return 1
    fi
    sleep 2
  done
}

wait_for_worker_running() {
  step "Waiting for worker container to be running"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  local container_id
  while true; do
    container_id="$(compose ps -q worker || true)"
    if [[ -n "$container_id" ]]; then
      if [[ "$(docker inspect -f '{{.State.Running}}' "$container_id" 2>/dev/null || true)" == "true" ]]; then
        return 0
      fi
    fi
    if (( SECONDS >= deadline )); then
      echo "Worker did not become/stay running" >&2
      compose ps --all >&2 || true
      return 1
    fi
    sleep 2
  done
}

supervisor_failure_count() {
  compose logs --no-color worker 2>/dev/null \
    | grep -c 'mock-supervisor: worker exited with code 1' || true
}

wait_for_supervisor_failure_after_count() {
  local previous_count=$1
  step "Waiting for mock supervisor to observe a worker failure"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  while true; do
    if (( $(supervisor_failure_count) > previous_count )); then
      step "Mock supervisor observed worker failure and restart"
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "Mock supervisor did not observe a worker failure before timeout" >&2
      compose logs --tail=200 worker >&2 || true
      return 1
    fi
    sleep 2
  done
}

assert_supervisor_failure_count_unchanged() {
  local previous_count=$1
  local current_count
  current_count="$(supervisor_failure_count)"
  if (( current_count != previous_count )); then
    echo "Expected no new worker restart, but supervisor failure count changed " \
      "from $previous_count to $current_count" >&2
    compose logs --tail=200 worker >&2 || true
    return 1
  fi
}

service_log_line_count() {
  local service=$1
  compose logs --no-color "$service" 2>/dev/null | wc -l
}

scenario_slug() {
  echo "$1" | tr '[:upper:] ' '[:lower:]_' | tr -cd '[:alnum:]_-'
}

save_service_log_delta() {
  local service=$1
  local slug=$2
  local start_line=$3
  local output_file="$LOG_DIR/scenarios/${slug}_${service}.log"

  mkdir -p "$LOG_DIR/scenarios"
  compose logs --no-color "$service" 2>/dev/null | tail -n +$((start_line + 1)) >"$output_file" || true
}

save_scenario_logs() {
  local name=$1
  local worker_start_line=$2
  local api_start_line=$3
  local slug
  slug="$(scenario_slug "$name")"

  save_service_log_delta worker "$slug" "$worker_start_line"
  save_service_log_delta receiver_api "$slug" "$api_start_line"
  step "Scenario logs saved: $LOG_DIR/scenarios/${slug}_{worker,receiver_api}.log"
}

worker_log_pattern_count() {
  local pattern=$1
  compose logs --no-color worker 2>/dev/null | grep -c "$pattern" || true
}

wait_for_worker_log_count_after() {
  local pattern=$1
  local previous_count=$2
  step "Waiting for new worker log pattern: $pattern"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  while true; do
    if (( $(worker_log_pattern_count "$pattern") > previous_count )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for new worker log pattern: $pattern" >&2
      compose logs --tail=200 worker >&2 || true
      return 1
    fi
    sleep 2
  done
}

traffic_line_count() {
  local traffic_log=$1
  if [[ -f "$traffic_log" ]]; then
    wc -l <"$traffic_log"
  else
    echo 0
  fi
}

traffic_success_count() {
  local traffic_log=$1
  if [[ -f "$traffic_log" ]]; then
    grep -c 'status=200' "$traffic_log" || true
  else
    echo 0
  fi
}

wait_for_traffic_successes() {
  local traffic_log=$1
  local minimum=${2:-3}
  step "Waiting for at least $minimum accepted datapoint requests in $traffic_log"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  while true; do
    if (( $(traffic_success_count "$traffic_log") >= minimum )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for accepted datapoint traffic" >&2
      [[ -f "$traffic_log" ]] && tail -n 100 "$traffic_log" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_traffic_after_count() {
  local traffic_log=$1
  local previous_count=$2
  local minimum_new_lines=${3:-3}
  step "Waiting for traffic to continue in $traffic_log"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  while true; do
    if (( $(traffic_line_count "$traffic_log") >= previous_count + minimum_new_lines )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for continued datapoint traffic" >&2
      [[ -f "$traffic_log" ]] && tail -n 100 "$traffic_log" >&2
      return 1
    fi
    sleep 1
  done
}

wait_for_successes_after_count() {
  local traffic_log=$1
  local previous_count=$2
  local minimum_new_successes=${3:-3}
  step "Waiting for accepted datapoints to continue in $traffic_log"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  while true; do
    if (( $(traffic_success_count "$traffic_log") >= previous_count + minimum_new_successes )); then
      return 0
    fi
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for accepted datapoint traffic to resume" >&2
      [[ -f "$traffic_log" ]] && tail -n 100 "$traffic_log" >&2
      return 1
    fi
    sleep 1
  done
}

verify_marker_processed() {
  local name=$1
  step "Verifying worker processed marker datapoint '$name'"
  compose exec -T receiver_api python - "$name" "$RECOVERY_TIMEOUT" <<'PY'
import sys
import time
import requests

name = sys.argv[1]
timeout = int(sys.argv[2])
eid = f"recovery_marker_{name}_{time.time_ns()}"
value = int(time.time()) % 1000000
base_url = "http://127.0.0.1:5000"

response = requests.post(
    f"{base_url}/datapoints",
    json=[{"type": "test_entity_type", "id": eid, "attr": "test_attr_int", "v": value}],
    timeout=5,
)
response.raise_for_status()

deadline = time.monotonic() + timeout
last_payload = None
while time.monotonic() < deadline:
    response = requests.get(
        f"{base_url}/entity/test_entity_type/{eid}/get/test_attr_int",
        timeout=5,
    )
    if response.status_code == 200:
        last_payload = response.json()
        if last_payload.get("current_value") == value:
            print(f"marker processed: eid={eid} value={value}")
            raise SystemExit(0)
    time.sleep(1)

raise RuntimeError(f"marker was not processed: eid={eid} value={value} last={last_payload}")
PY
}

wait_for_worker_log() {
  local pattern=$1
  step "Waiting for worker log pattern: $pattern"
  local deadline=$((SECONDS + RECOVERY_TIMEOUT))
  until compose logs --no-color worker 2>/dev/null | grep -q "$pattern"; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for worker log pattern: $pattern" >&2
      compose logs --tail=200 worker >&2 || true
      return 1
    fi
    sleep 2
  done
}

start_api() {
  step "Starting receiver API"
  compose up -d --no-deps --force-recreate receiver_api
  wait_for_api
}

restart_worker() {
  step "Starting worker"
  compose up -d --no-deps --force-recreate worker
  wait_for_worker_running
  wait_for_worker_log "Initialization completed"
}

start_traffic() {
  local name=$1
  mkdir -p "$LOG_DIR"
  local traffic_log="$LOG_DIR/${name}_traffic.log"
  TRAFFIC_CONTROL_FILE="$LOG_DIR/${name}_traffic.keep_running"
  TRAFFIC_LOG="$traffic_log"
  : >"$traffic_log"
  : >"$TRAFFIC_CONTROL_FILE"

  step "Starting datapoint traffic generator '$name'"
  (
    counter=0
    while [[ -e "$TRAFFIC_CONTROL_FILE" ]]; do
      counter=$((counter + 1))
      set +e
      output="$(compose exec -T receiver_api python - "$name" "$counter" <<'PY' 2>&1
import sys
import time
import requests

name = sys.argv[1]
counter = sys.argv[2]
payload = [
    {
        "type": "test_entity_type",
        "id": f"{name}_{time.time_ns()}_{counter}_{idx}",
        "attr": "test_attr_int",
        "v": int(counter),
    }
    for idx in range(20)
]
response = requests.post("http://127.0.0.1:5000/datapoints", json=payload, timeout=5)
print(f"status={response.status_code} body={response.text[:200]!r}")
PY
)"
      status=$?
      set -e
      printf '[%(%Y-%m-%dT%H:%M:%S%z)T] exit=%s %s\n' -1 "$status" "$output" >>"$traffic_log"
      sleep 0.1
    done
  ) &
  TRAFFIC_PID=$!
}

stop_traffic() {
  if [[ -n "$TRAFFIC_CONTROL_FILE" ]]; then
    rm -f "$TRAFFIC_CONTROL_FILE"
  fi
  if [[ -n "$TRAFFIC_PID" ]]; then
    wait "$TRAFFIC_PID" 2>/dev/null || true
  fi
  TRAFFIC_PID=""
  TRAFFIC_CONTROL_FILE=""
}

record_scenario_result() {
  local name=$1
  local status=$2

  if (( status == 0 )); then
    SCENARIO_RESULTS+=("PASS $name")
    step "Scenario passed: $name"
  else
    SCENARIO_RESULTS+=("FAIL $name")
    SCENARIO_FAILURES=$((SCENARIO_FAILURES + 1))
    step "Scenario failed: $name"
  fi
}

print_scenario_results() {
  step "Scenario results"
  for result in "${SCENARIO_RESULTS[@]}"; do
    step "  $result"
  done
}

restore_stack_after_scenario() {
  step "Restoring demo stack before next scenario"
  compose start rabbitmq mongo redis >/dev/null 2>&1 || true
  wait_for_exec redis "Redis after scenario" redis-cli ping || true
  wait_for_exec rabbitmq "RabbitMQ after scenario" rabbitmq-diagnostics -q ping || true
  wait_for_rabbitmq_queues || true
  wait_for_api || true

  local container_id
  container_id="$(compose ps -q worker || true)"
  if [[ -z "$container_id" ]] \
    || [[ "$(docker inspect -f '{{.State.Running}}' "$container_id" 2>/dev/null || true)" != "true" ]]; then
    step "Worker is not running after scenario; recreating it for the next scenario"
    restart_worker || true
  else
    wait_for_worker_running || true
  fi
}

run_scenario() {
  local name=$1
  local restore_after=${2:-yes}
  shift 2
  local worker_start_line
  local api_start_line

  worker_start_line="$(service_log_line_count worker)"
  api_start_line="$(service_log_line_count receiver_api)"

  step "=== Scenario: $name ==="
  set +e
  (set -e; "$@")
  local status=$?
  set -e

  save_scenario_logs "$name" "$worker_start_line" "$api_start_line"
  record_scenario_result "$name" "$status"
  if [[ "$restore_after" == "yes" ]]; then
    restore_stack_after_scenario
  fi
  return 0
}

run_rabbitmq_scenario() {
  step "RabbitMQ outage with continuous traffic: worker should keep running and reconnect"
  rabbit_lines_before="$(traffic_line_count "$traffic_log")"
  rabbit_successes_before="$(traffic_success_count "$traffic_log")"
  rabbit_failures_before="$(supervisor_failure_count)"
  rabbit_errors_before="$(worker_log_pattern_count 'RabbitMQ connection error')"
  rabbit_reconnects_before="$(worker_log_pattern_count "it's OK now, we're successfully connected")"
  compose stop rabbitmq
  sleep "$RABBITMQ_OUTAGE_SECONDS"
  wait_for_traffic_after_count "$traffic_log" "$rabbit_lines_before" 3
  wait_for_worker_log_count_after "RabbitMQ connection error" "$rabbit_errors_before"
  wait_for_worker_running
  assert_supervisor_failure_count_unchanged "$rabbit_failures_before"
  compose start rabbitmq
  wait_for_exec rabbitmq "RabbitMQ after restart" rabbitmq-diagnostics -q ping
  wait_for_rabbitmq_queues
  wait_for_worker_log_count_after "it's OK now, we're successfully connected" "$rabbit_reconnects_before"
  wait_for_worker_running
  wait_for_successes_after_count "$traffic_log" "$rabbit_successes_before" 3
  verify_marker_processed rabbitmq_recovery
  step "RabbitMQ recovery mode observed: marker datapoint processed and worker was not restarted"
}

run_redis_scenario() {
  step "Redis outage with continuous traffic: mock supervisor should restart the worker"
  redis_lines_before="$(traffic_line_count "$traffic_log")"
  redis_successes_before="$(traffic_success_count "$traffic_log")"
  redis_failures_before="$(supervisor_failure_count)"
  redis_init_before="$(worker_log_pattern_count 'Initialization completed')"
  compose stop redis
  wait_for_traffic_after_count "$traffic_log" "$redis_lines_before" 3
  wait_for_supervisor_failure_after_count "$redis_failures_before"
  compose start redis
  wait_for_exec redis "Redis after restart" redis-cli ping
  wait_for_worker_log_count_after "Initialization completed" "$redis_init_before"
  wait_for_worker_running
  wait_for_successes_after_count "$traffic_log" "$redis_successes_before" 3
  verify_marker_processed redis_recovery
  step "Redis failure mode observed: worker exited non-zero and mock supervisor restarted it"
}

run_mongo_scenario() {
  step "MongoDB outage with continuous traffic: mock supervisor should restart the worker"
  mongo_lines_before="$(traffic_line_count "$traffic_log")"
  mongo_successes_before="$(traffic_success_count "$traffic_log")"
  mongo_failures_before="$(supervisor_failure_count)"
  mongo_init_before="$(worker_log_pattern_count 'Initialization completed')"
  compose stop mongo
  wait_for_traffic_after_count "$traffic_log" "$mongo_lines_before" 3
  wait_for_supervisor_failure_after_count "$mongo_failures_before"
  compose start mongo
  sleep 5
  wait_for_worker_log_count_after "Initialization completed" "$mongo_init_before"
  wait_for_worker_running
  wait_for_successes_after_count "$traffic_log" "$mongo_successes_before" 3
  verify_marker_processed mongo_recovery
  step "MongoDB failure mode observed: worker exited non-zero and mock supervisor restarted it"
}

run_demo() {
  cd "$ROOT_DIR"
  mkdir -p "$LOG_DIR"

  step "Using Docker Compose project '$PROJECT_NAME'"
  step "Using log directory '$LOG_DIR'"

  cleanup_stack >/dev/null 2>&1 || true

  step "Building demo images"
  compose build rabbitmq worker receiver_api

  step "Starting dependencies"
  compose up -d rabbitmq mongo redis
  wait_for_exec redis "Redis" redis-cli ping
  wait_for_exec rabbitmq "RabbitMQ" rabbitmq-diagnostics -q ping
  wait_for_rabbitmq_queues
  sleep 5

  start_api
  restart_worker

  step "Starting continuous datapoint traffic for all outage scenarios"
  start_traffic continuous
  traffic_log="$TRAFFIC_LOG"
  wait_for_traffic_successes "$traffic_log" 3

  run_scenario "RabbitMQ resilient reconnect" yes run_rabbitmq_scenario
  run_scenario "Redis fatal restart" yes run_redis_scenario
  run_scenario "MongoDB fatal restart" no run_mongo_scenario

  stop_traffic

  save_logs
  print_scenario_results
  step "Traffic log: $traffic_log"
  if (( SCENARIO_FAILURES > 0 )); then
    step "Demo completed with $SCENARIO_FAILURES failed scenario(s)"
    return 1
  fi
  step "Demo completed successfully"
  cleanup_stack
}

run_failure_cleanup() {
  local status=$?
  stop_traffic
  if (( status != 0 )); then
    save_logs
    cat >&2 <<EOF

Demo failed. Containers and logs were preserved for inspection.
Useful commands:
  DP3_RECOVERY_PROJECT=$PROJECT_NAME $0 status
  DP3_RECOVERY_PROJECT=$PROJECT_NAME $0 logs
  DP3_RECOVERY_PROJECT=$PROJECT_NAME $0 cleanup

EOF
  fi
  rm -f "$COMPOSE_FILE"
  exit "$status"
}

main() {
  write_compose_file
  trap 'rm -f "$COMPOSE_FILE"' EXIT

  case "${1:-run}" in
    run)
      trap run_failure_cleanup EXIT
      run_demo
      ;;
    cleanup)
      cleanup_stack
      ;;
    logs)
      print_logs
      ;;
    status)
      print_status
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage >&2
      return 2
      ;;
  esac
}

main "$@"
