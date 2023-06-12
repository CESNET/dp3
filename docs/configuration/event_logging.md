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
  # TODO
```

# Redis

This section describes Redis connection details:

| Parameter | Data-type | Default value | Description                                                                       |
|-----------|-----------|---------------|-----------------------------------------------------------------------------------|
| `host`    | string    | `localhost`   | IP address or hostname for connection to Redis.                                   |
| `port`    | int       | 6379          | Listening port of Redis.                                                          |
| `db`      | int       | 0             | Index of Redis DB used for the counters (it shouldn't be used for anything else). |

# Groups

TODO
