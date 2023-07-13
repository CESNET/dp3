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
    sync-interval: 1 # (2)!
  # Number of processed tasks by their "src" attribute
  tasks_by_src:
    events: [ ]
    auto_declare_events: true
    intervals: [ "5s", "5m" ]
    sync-interval: 1
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

The default configuration groups enables logging of events in task execution, namely
`task_processed` and `task_processing_error`.

To learn more about the group configuration for EventCountLogger, 
please refer to the official [documentation](https://github.com/CESNET/EventCountLogger#configuration).
