# Updater

The updater module is responsible for updating all entities in the database over a longer time frame.
The main use-case is to execute queries to external systems and update the entities with the results,
avoiding the present rate limits and other restrictions.
However, it can be used for any kind of long-running operation.
This functionality is exposed to modules using the [registrar API](../modules.md#periodic-update-callbacks).

For better robustness, the updates happen in batches, which are executed on a configurable schedule.

The global updater configuration requires setting of the batch update period.
This is done using the `update_batch_cron` - a cron schedule for the batch update. See [CronExpression docs][dp3.common.config.CronExpression] for details.
The second item is `update_batch_period` - the period of the batch update. This is a string matching the format of `\d+[smhd]` (for second, minute, hour, day respectively).

!!! warning "`update_batch_cron` and `update_batch_period` must be set to equivalent values!"

    Both items are **required** and must be set to **equivalent time values**.
    For example, if `update_batch_cron` is set to run every 30 seconds - 
    `{ minute: "*", second: "*/30" }`, `update_batch_period` should be set to `30s`.

The default configuration of `updater.yml` looks like this:

```yaml
update_batch_cron: { minute: "*/5", second: "0" }
update_batch_period: "5m"
```

Try to find a balance between having batches run too often (some batches may execute empty,
leading to unnecessary overhead) and too rarely.

Additional options are available for the updater module, but they are not required.
They are used to configure when cache management runs and the maximum number of entries in the cache.
These are the options with their default values:

```yaml
cache_management_cron: { hour: "2", minute: "0", second: "0" }  # (1)!
cache_max_entries: 32  # (2)!
```

1. `cache_management_cron` - a cron schedule for the cache management. See [CronExpression docs][dp3.common.config.CronExpression] for details.
2. `cache_max_entries` - the maximum number of entries in the cache. If the number of cache items (counted for each individual period) exceeds this number, the oldest entries are removed.
