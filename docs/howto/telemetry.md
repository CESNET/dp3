# How to inspect DP³ telemetry

This guide is a beginner-friendly checklist for answering three questions about a running DP³ application:

- Are input sources still sending data?
- Is DP³ keeping up with the data it receives?
- Are secondary modules running as expected?

`dp3 sh telemetry` is the packaged client for the telemetry exposed by the DP³ API. The examples below use it for convenience. You can also call the corresponding [telemetry API endpoints](../api.md#other-endpoints) directly.

You will end up with:

- a current view of active and stale input sources
- counts of the data present in the database
- a quick assessment of RabbitMQ queues and snapshot processing
- commands for investigating periodic processes and secondary modules

## Before you start

You need:

- access to a running DP³ API
- the `dp3` command and the application's configuration directory, or its generated `<APPNAME>sh` wrapper
- access to worker logs for secondary-module diagnostics
- `jq` for the optional metadata examples

On a deployment host, use `<APPNAME>sh` in place of `dp3 sh` below. The wrapper already knows the application's production configuration. For another API URL, pass `-u`, for example `dp3 sh -u http://localhost:5000 telemetry sources-validity`.

## 1. Check for datapoints rejected by the API

When an input module reports successful sends but no data appears in DP³, first verify that
the module checks the HTTP response status and body. **Completing an HTTP request does not
mean that DP³ accepted its datapoints.**

If `api.datapoint_logger.bad_log` is configured in
[`api.yml`](../configuration/api.md), inspect that file for payload validation errors:

```shell
grep -n 'bad_log' /path/to/config/api.yml
tail -n 100 /path/to/configured/bad_dp.json.log
```

The bad datapoint log includes the rejected input and its validation error. Typical causes
include an unknown entity or attribute, an invalid entity id, a value of the wrong type, or
missing timestamps. A request rejected by the API never reaches a worker, so it will not
appear in source-validity telemetry, Redis task counters, raw datapoints, or entity data.

If `bad_log` is `false`, this diagnostic log is disabled. To use it, configure an absolute
path whose parent directory exists and is writable by the API process, then restart the API.

## 2. Check RabbitMQ queues

Inspect queue totals, consumers, and rates:

```shell
dp3 sh telemetry rabbitmq-queues
```

For a compact table of queue names, totals, and rates, format the result with `jq` and `column`:

```shell
dp3 sh telemetry rabbitmq-queues | jq '.queues[] | [.name,.total, .incoming, .outgoing] | @csv' -cr | column -ts ','
```

Read the output as follows:

- `total`, `ready`, and `unacked` show queued work. A large or continuously growing total indicates a backlog.
- `incoming` and `outgoing` are message rates. Sustained `incoming` much greater than `outgoing` means the backlog is growing.
- `outgoing` equal to or greater than `incoming` normally means the workers are keeping up or draining a backlog.
- `consumers` should match the processes expected to consume that queue. A zero rate is only healthy when no work is arriving.

Rates are a momentary view. Repeat the command before drawing conclusions from a single sample.

## 3. Check whether workers receive inputs

Start with either view of source activity:

```shell
dp3 sh telemetry sources-validity
dp3 sh telemetry source-age
dp3 sh telemetry source-age -u seconds
```

`sources-validity` and `source-age` are two views of the same telemetry records. For each datapoint source tag, they show either the latest datapoint validity timestamp (`t2`, or `t1` when `t2` is absent) or its age. The timestamp is recorded by an `on_task_start` hook, before task validation, database insertion, and attribute hooks.

Use this to find sources that have stopped sending data. Keep these limitations in mind:

- It shows that a worker received a task carrying the source, not that the task completed successfully. Validation, database insertion, or a later callback can still fail.
- A source remains listed after it becomes inactive. It can therefore appear here even after HistoryManager has deleted all of its old entity data.
- It does not report whether a secondary module processed the datapoint successfully.

## 4. Check how much data reached the database

Get current entity counts at attribute granularity:

```shell
dp3 sh telemetry entities-per-attr
```

This answers “how many entities currently have each attribute?”. It counts value presence in the database, rather than the number of datapoints received over time.

To drill into a particular attribute, list each distinct latest value and its entity count:

```shell
dp3 sh entity <ETYPE> attr-values <ATTR>
```

The equivalent HTTP endpoint is [`GET /entity/<entity_type>/_/distinct/<attr_id>`](../api.md#get-distinct-values).

## 5. Check secondary-module hooks

Task-executor hooks publish execution statistics to the `secondary_hooks` EventCountLogger group. Read the counters for the current and last intervals with:

```shell
dp3 sh telemetry event-counts --group secondary_hooks --interval 5m --both
```

Counter names identify the hook family, entity or attribute context, callback, and metric. All task-executor hooks report `executions`, `failures`, and `duration_ns`. Hooks that can return datapoint tasks also report `created_tasks` when they create at least one. `allow_entity_creation` hooks instead report `decisions_allowed` or `decisions_denied` for successful calls.

An allowed decision only means that one hook returned a truthy value. A later hook can still deny creation, and a later processing error can prevent the entity from being stored. A hook that denies creation also stops subsequent allow hooks from running.

Use `duration_ns / executions` to calculate the mean callback duration in an interval. The duration includes failed executions, while `created_tasks` counts tasks returned by the hook rather than tasks subsequently processed successfully.

The counters identify a failing callback, but the worker log contains its exception details:

```shell
grep -F '<ModuleClass>' /var/log/<APPNAME>/worker*.log
grep 'Exception\|Error\|Traceback\|File "' -B1 -A1 /var/log/<APPNAME>/worker*.log
```

Callback registration is also logged during application startup. If no counters exist for a callback, restart the affected workers and search the startup logs for the module class name. This confirms whether its hooks were registered and exposes import or configuration errors.

## 6. Read Redis event counters

Every standard DP³ deployment has Redis-backed event counters configured through [`event_logging.yml`](../configuration/event_logging.md). Read the last completed interval by selecting a configured group and interval:

```shell
dp3 sh telemetry event-counts -g te -i 5m
dp3 sh telemetry event-counts --group tasks_by_src --interval 2h
dp3 sh telemetry event-counts -g secondary_hooks -i 5m
```

The command defaults to `--last`. Use `--current` for the incomplete interval or `--both` to return both periods:

```shell
dp3 sh telemetry event-counts -g te -i 5m --both
```

The `te` group contains task-processing and error counters. The `tasks_by_src` group contains one counter per observed datapoint source. The `secondary_hooks` group contains task-executor hook statistics described above. Groups and intervals are application-configurable; check `event_logging.yml` for the available values.

This command uses the Redis connection from the selected DP³ configuration directory and must run from a host that can reach that Redis instance. Current counts may lag behind workers by each group's configured EventCountLogger synchronization interval. Last counts cover the most recently completed interval.

## 7. Check snapshot processing

Use the summary for a quick view of recent snapshot runs:

```shell
dp3 sh telemetry snapshot-summary
```

The result reports:

- `latest_age`: seconds since the newest snapshot run started
- `finished_age`: seconds since the newest completed run started
- `entities`: entities handled by that completed run
- `total_s`: duration of that completed run

Unexpectedly old ages or unusually long durations are a reason to inspect metadata and worker logs. The summary is a formatted view of `SnapShooter` records in the internal `#metadata` collection.

## 8. Investigate periodic processes through metadata

The metadata command gives a lower-level view of records produced by internal periodic processes:

```shell
dp3 sh telemetry metadata -m <MODULE> -l 5
```

It supports module filtering with `-m`/`--module`, time bounds with `-f`/`--from` and `-t`/`--to`, pagination with `-s`/`--skip` and `-l`/`--limit`, ordering with `-S`/`--sort newest|oldest`, and output selection with `-F`/`--format json|ndjson`. Its default newline-delimited JSON output works well with `jq`:

```shell
dp3 sh telemetry metadata -m SnapShooter -l 5 -f "2026-07-30T12:00:00" \
  | jq '{entities: .entities, components: .components, w_done: .workers_finished, start: .task_creation_start, end: ."#last_update"}' -c

dp3 sh telemetry metadata -m HistoryManager -l 4 -f "2026-07-30T12:00:00" \
  | jq '{id: ._id, entities: .entities, updated: .updated, rev_conflicts: .revision_conflicts, retries: .retries, retry_fail: .retry_exhausted, end: ."#last_update"}' -c

dp3 sh telemetry metadata -m GarbageCollector -l 12 \
  | jq '{id: ._id, etype: .entity, seen: .entities, deleted: .deleted, end: ."#last_update"}' -c
```

Replace the example timestamps with the period you are investigating. For `SnapShooter`, `workers_finished` equal to the configured worker count means that all workers finished their part of the run. The snapshot summary treats a run as complete after linked-entity processing has also finished.

Metadata is an internal diagnostic format, so fields differ by module and may change as the corresponding process evolves.

## Related pages

- [API reference](../api.md)
- [How to deploy a DP³ application](deploy-app.md)
- [Event logging configuration](../configuration/event_logging.md)
- [Processing core configuration](../configuration/processing_core.md)
