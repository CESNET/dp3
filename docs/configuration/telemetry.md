# Telemetry configuration

Telemetry records operational information and caches statistics used by the telemetry API.

## Entities-per-attribute schedule

`entities_per_attr_schedule` controls refreshes of the cached entity counts for each attribute.
The default runs every 30 minutes, at minutes 7 and 37 (UTC):

```yaml
entities_per_attr_schedule:
  minute: "7,37"
```

This offset avoids the five-minute boundaries used by the default updater, history maintenance,
snapshot, and garbage-collection schedules. Long-running jobs can still overlap; adjust the
schedule to suit deployment-specific load.

Counting can be expensive even with index coverage. API requests only read cached results and
never trigger counts. Only worker process 0 collects these statistics, with a one-off sweep on
startup if any configured entity type lacks a cached result. Each entity type is published
atomically after all its counts succeed; failures are logged and retain the previous result.
Until a result is available, that entity type is omitted (an empty cache returns `{}`).
The response format is unchanged, but counts reflect the last successful refresh rather than
the time of the request.

## Attribute BSON-size schedule

`attribute_bson_size_schedule` is a cron expression controlling calculation of logical BSON-size
statistics for configured attributes. The default runs daily at 03:15 UTC:

```yaml
attribute_bson_size_schedule:
  hour: 3
  minute: 15
```

The syntax is the same as other DP³ cron schedules. For example, `hour: "*/6"` runs every six
hours. The job scans each entity type's master collection once, so choose an interval appropriate
to the size and load of the database. Only worker process 0 runs it, preventing duplicate full
scans in a multi-process deployment.

When worker process 0 starts, it schedules a one-off sweep if any configured entity type does not
have a cached result. This populates a new or incomplete cache without waiting for the next regular
run.

Each entity type is published to the Telemetry cache only after its scan succeeds. A failed scan
is logged and leaves that entity type's previous cached result available to API readers.

## Collection records in `#metadata`

Each run of the two collections above saves a `Telemetry` record in `#metadata`, readable with
`dp3 sh telemetry metadata -m Telemetry`:

- `telemetry_type` is `entities_per_attr` or `attribute_bson_sizes`,
- `collection_start` and `collection_end` are the times of the run,
- `collection_metrics.entity_types` lists for each entity type the `duration` of its scan
  (seconds), the number of its `attributes` and how many of them are stored in at least one
  entity (`stored_attributes`),
- an entity type which fails has an `error` instead of the counts and is counted in `failures`.

The collected values themselves are kept only in the cache, as their latest version.
