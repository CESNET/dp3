# Telemetry configuration

Telemetry records operational information and caches statistics used by the telemetry API.

## Attribute BSON-size schedule

`attribute_bson_size_schedule` is a cron expression controlling calculation of logical BSON-size statistics for configured attributes. The default runs daily at 03:15 UTC:

```
attribute_bson_size_schedule:
  hour: 3
  minute: 15
```

The syntax is the same as other DP³ cron schedules. For example, `hour: "*/6"` runs every six hours. The job scans each entity type's master collection once, so choose an interval appropriate to the size and load of the database. Only worker process 0 runs it, preventing duplicate full scans in a multi-process deployment.

When worker process 0 starts, it schedules a one-off sweep if any configured entity type does not have a cached result. This populates a new or incomplete cache without waiting for the next regular run.

Each entity type is published to the Telemetry cache only after its scan succeeds. A failed scan is logged and leaves that entity type's previous cached result available to API readers.
