# History manager

The concepts of history management in DP3 are described [here](../history_management.md). History manager is responsible for:

- Datapoint aggregation - merging identical value datapoints in master records
- Deleting old datapoints from master records
- Deleting old snapshots
- Archiving old datapoints from raw collections

Configuration file `history_manager.yml` is very simple:

```yaml
aggregation_schedule:  # (1)!
  minute: "*/10"

datapoint_cleaning_schedule:  # (2)!
  minute: "*/30"

snapshot_cleaning:
  schedule: {minute: "15,45"}  # (3)!
  older_than: 7d  # (4)!

datapoint_archivation:
  schedule: {hour: 2, minute: 0}  # (5)!
  older_than: 7d  # (6)!
  archive_dir: "data/datapoints/"  # (7)!
```

1. Parameter `aggregation_schedule` sets the interval for DP³ to aggregate observation datapoints in master records. This should be scheduled more often than cleaning of datapoints.
2. Parameter `datapoint_cleaning_schedule` sets interval when should DP³ check if any data in master record of observations and timeseries attributes isn't too old and if there's something too old, removes it. To control what is considered as "too old", see parameter `max_age` in *Database entities* configuration.
3. Parameter `snapshot_cleaning.schedule` sets the interval for DP³ to clean the snapshots collection. Optimally should be scheduled outside the snapshot creation window. See *Snapshots* configuration for more.
4. Parameter `snapshot_cleaning.older_than` sets how old must a snapshot be to be deleted.
5. Parameter `datapoint_archivation.schedule` sets interval for DP³ to archive datapoints from raw collections.
6. Parameter `datapoint_archivation.older_than` sets how old must a datapoint be to be archived.
7. Parameter `datapoint_archivation.archive_dir` sets directory where should be archived old datapoints. If directory doesn't exist, it will be created, but write priviledges must be set correctly. Can be also set to `null` (or not set) to disable archivation and only delete old data.

The schedule dictionaries are transformed to cron expressions, see [CronExpression docs][dp3.common.config.CronExpression] for details.

## Repairing history markers

Master documents use `#min_t2s.<attribute>` markers to select histories that may contain expired
datapoints. DP³ initializes these markers when history is appended and updates them when datapoints
are deleted, so no periodic full-collection marking is required.

The `mark_datapoints_schedule` option is deprecated and ignored. Existing configurations that
contain it remain valid and produce a deprecation warning; it can be removed when convenient.

After upgrading an existing deployment, deploy the updated workers first and inspect the existing
master collections:

```shell
python scripts/repair_min_t2s.py <config_dir> count
```

Fill markers missing from non-empty histories before relying on regular datapoint cleanup:

```shell
python scripts/repair_min_t2s.py <config_dir> fill
```

Use `--entity-type <entity>` one or more times to restrict either operation. The fill operation is
atomic per master document and increments `#revision`, so concurrent aggregation detects and retries
conflicts instead of overwriting the repaired marker.
