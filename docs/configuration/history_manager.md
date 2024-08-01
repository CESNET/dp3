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

mark_datapoints_schedule: # (2)!
  hour: "7,19"
  minute: "45"
datapoint_cleaning_schedule:  # (3)!
  minute: "*/30"

snapshot_cleaning:
  schedule: {minute: "15,45"}  # (4)!
  older_than: 7d  # (5)!

datapoint_archivation:
  schedule: {hour: 2, minute: 0}  # (6)!
  older_than: 7d  # (7)!
  archive_dir: "data/datapoints/"  # (8)!
```

1. Parameter `aggregation_schedule` sets the interval for DP³ to aggregate observation datapoints in master records. This should be scheduled more often than cleaning of datapoints.
2. Parameter `mark_datapoints_schedule` sets the interval when the datapoint timestamps are marked for all entities in a master collection. This should be scheduled very rarely, as it's a very expensive operation. 
3. Parameter `datapoint_cleaning_schedule` sets interval when should DP³ check if any data in master record of observations and timeseries attributes isn't too old and if there's something too old, removes it. To control what is considered as "too old", see parameter `max_age` in *Database entities* configuration.
4. Parameter `snapshot_cleaning.schedule` sets the interval for DP³ to clean the snapshots collection. Optimally should be scheduled outside the snapshot creation window. See *Snapshots* configuration for more.  
5. Parameter `snapshot_cleaning.older_than` sets how old must a snapshot be to be deleted.
6. Parameter `datapoint_archivation.schedule` sets interval for DP³ to archive datapoints from raw collections.
7. Parameter `datapoint_archivation.older_than` sets how old must a datapoint be to be archived.
8. Parameter `datapoint_archivation.archive_dir` sets directory where should be archived old datapoints. If directory doesn't exist, it will be created, but write priviledges must be set correctly. Can be also set to `null` (or not set) to disable archivation and only delete old data.

The schedule dictionaries are transformed to cron expressions, see [CronExpression docs][dp3.common.config.CronExpression] for details.