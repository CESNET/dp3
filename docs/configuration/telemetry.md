# Telemetry configuration

Telemetry records operational information and caches statistics used by the telemetry API.

## Entities-per-attribute schedule

`entities_per_attr_schedule` controls refreshes of the cached entity counts for each attribute.
The default runs every 30 minutes, at minutes 7 and 37 (UTC):

```yaml
entities_per_attr_schedule:
  minute: "7,37"
```

The default avoids the times of the other default schedules. Long-running jobs can still
overlap; adjust the schedule to suit deployment-specific load.

Counting can be expensive, so API requests only read the cached counts, which reflect the last
successful refresh rather than the time of the request. A new or incomplete cache is filled when
the workers start, without waiting for the next scheduled run. Until the first result is
available, an entity type is omitted from the response. When a refresh of an entity type fails,
the failure is logged and its previous counts stay available.

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
to the size and load of the database.

The results are cached in the same way as the entity counts: a new or incomplete cache is filled
when the workers start, and a failed scan of an entity type is logged and leaves its previous
result available.

## Records of collection runs

Each run of the two collections above is recorded with its start and end times, and the duration
and failures per entity type. Read the records with `dp3 sh telemetry metadata -m Telemetry`.
The collected values themselves are kept only in their latest version.
