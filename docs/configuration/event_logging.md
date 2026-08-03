# Event logging

Event logging is done using Redis and allows to count arbitrary events across
multiple processes (using shared counters in Redis) and in various time
intervals.

More information can be found in [Github repository of EventCountLogger](https://github.com/CESNET/EventCountLogger).

Configuration file `event_logging.yml` looks like this:

```yaml
redis:
  host: localhost
  port: 6379
  db: 1

groups:
  # Main events of Task execution
  te:
    events:
      - task_processed
      - task_processing_error
    intervals: [ "5m", "2h" ] # (1)!
    sync_interval: 1 # (2)!
  # Number of processed tasks by their "src" attribute
  tasks_by_src:
    events: [ ]
    auto_declare_events: true
    intervals: [ "5s", "5m" ]
    sync_interval: 1
  # Task-executor hook execution statistics
  secondary_hooks:
    events: [ ]
    auto_declare_events: true
    intervals: [ "5m", "2h" ]
    sync_interval: 1
```

1. Two intervals - 5 min and 2 hours for longer-term history in Munin/Icinga
2. Cache counts locally, push to Redis every second

# Redis

This section describes Redis connection details:

| Parameter | Data-type | Default value | Description                                                                       |
|-----------|-----------|---------------|-----------------------------------------------------------------------------------|
| `host`    | string    | `localhost`   | IP address or hostname for connection to Redis.                                   |
| `port`    | int       | 6379          | Listening port of Redis.                                                          |
| `db`      | int       | 0             | Index of Redis DB used for the counters (it shouldn't be used for anything else). |

# Groups

The default groups record task execution, processed tasks by source, and secondary-module hook
statistics. Hook event names are declared dynamically and use the namespace
`<hook-family>/<callback>/<context>/<metric>`. The callback is module-qualified but omits bound
`partial` arguments, while the single context component identifies the hook's entity, attribute,
or snapshot scope.
EventCountLogger buffers each group's increments in memory and flushes them according to its
`sync_interval` or `sync_limit` setting. See the
[telemetry guide](../howto/telemetry.md#5-check-secondary-module-hooks) for the hook metrics and
their interpretation.

To learn more about the group configuration for EventCountLogger,
please refer to the official [documentation](https://github.com/CESNET/EventCountLogger#configuration).
