# History manager

The concepts of history management in DP3 are described [here](https://cesnet.github.io/dp3/history_management/index.md). History manager is responsible for:

- Datapoint aggregation - merging identical value datapoints in master records
- Deleting old datapoints from master records
- Deleting old snapshots
- Archiving old datapoints from raw collections

Configuration file `history_manager.yml` is very simple:

```
aggregation_schedule:  # (1)!
  minute: "*/10"

datapoint_cleaning:
  period: 1d  # (2)!

snapshot_cleaning:
  schedule: {minute: "15,45"}  # (3)!
  older_than: 7d  # (4)!

datapoint_archivation:
  schedule: {hour: 2, minute: 0}  # (5)!
  older_than: 7d  # (6)!
  archive_dir: "data/datapoints/"  # (7)!
```

1. Parameter `aggregation_schedule` sets the interval for DP³ to aggregate observation datapoints in master records. This should be scheduled more often than cleaning of datapoints.
1. Parameter `datapoint_cleaning.period` (default `1d`) sets how often is each master record checked for datapoints of observations and timeseries attributes that are too old, which are then removed. To control what is considered as "too old", see parameter `max_age` in *Database entities* configuration. See [Datapoint cleaning](#datapoint-cleaning).
1. Parameter `snapshot_cleaning.schedule` sets the interval for DP³ to clean the snapshots collection. Optimally should be scheduled outside the snapshot creation window. See *Snapshots* configuration for more.
1. Parameter `snapshot_cleaning.older_than` sets how old must a snapshot be to be deleted.
1. Parameter `datapoint_archivation.schedule` sets interval for DP³ to archive datapoints from raw collections. Preferably schedule it outside the snapshot creation window.
1. Parameter `datapoint_archivation.older_than` sets how long ago must raw datapoints have been received to be archived.
1. Parameter `datapoint_archivation.archive_dir` sets directory where should be archived old datapoints. If directory doesn't exist, it will be created, but write priviledges must be set correctly. Can be also set to `null` (or not set) to disable archivation and only delete old data.

The schedule dictionaries are transformed to cron expressions, see CronExpression docs for details.

## Datapoint cleaning

Expired datapoints are removed continuously: every record is cleaned once per `datapoint_cleaning.period`, with the work spread evenly over the period. A shorter period keeps master records slimmer, but increases the database load, as each cleaned record is rewritten. Expired datapoints may remain in a record for up to one `period` past their `max_age`.

The `datapoint_cleaning_schedule`, `datapoint_cleaning.chunk_size` and `datapoint_cleaning.chunk_pause` options are deprecated and ignored.

## Datapoint archivation

Raw datapoints are archived in batches by the time they were received, not by their `t1`. Each run archives the datapoints received in a period between two runs once they are older than `older_than`. With a daily schedule and `older_than: 1d`, each run archives the datapoints received during the day before the previous run.

Archived datapoints are written to `archive_dir` as `dp-log-<min t1>--<max t1>.jsonl.gz`, named by the range of `t1` values in the file. Datapoints dated in the future at the time they were received are logged as a warning.

If an archivation run fails, the datapoints are kept in the database and archived by the next run.

## Maintenance metrics in `#metadata`

HistoryManager records run start/end times and metrics in `#metadata`:

- `aggregation_metrics.entity_types`: per-attribute datapoint counts before/after aggregation, alongside the existing record-update and retry counters. Counts describe initial-pass computations, not guaranteed persisted changes; retries are excluded.
- `cleaning_metrics`: one record per entity type and cleaning period, saved when the period ends (`cleaning_start`, `cleaning_end`, `entity`). It contains the number of `batches` and `failed_batches`, the master records with expired datapoints (`records`) and the modified ones (`modified`), the total `update_time` and the `slowest_batch` (seconds).
- `archivation_metrics`: processed and dropped archive collections, exported datapoints (and how many of them were future-dated), the export file, and failures. Run start and end are recorded in `archivation_start` and `archivation_end`.

Metrics add no database reads. Interrupted runs may lack final metrics.

## Repairing history markers

Master documents use `#min_t2s.<attribute>` markers to select histories that may contain expired datapoints. DP³ initializes these markers when history is appended and updates them when datapoints are deleted, so no periodic full-collection marking is required.

The `mark_datapoints_schedule` option is deprecated and ignored. Existing configurations that contain it remain valid and produce a deprecation warning; it can be removed when convenient.

After upgrading an existing deployment, deploy the updated workers first and inspect the existing master collections:

```
python scripts/repair_min_t2s.py <config_dir> count
```

Fill markers missing from non-empty histories before relying on regular datapoint cleanup:

```
python scripts/repair_min_t2s.py <config_dir> fill
```

Use `--entity-type <entity>` one or more times to restrict either operation. The fill operation is atomic per master document and increments `#revision`, so concurrent aggregation detects and retries conflicts instead of overwriting the repaired marker.
