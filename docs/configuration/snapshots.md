# Snapshots

Snapshots configuration is straightforward. You can set the `creation_rate` - a cron schedule for creating new snapshots (every 30 minutes by default). See [CronExpression docs][dp3.common.config.CronExpression] for details.

The second option is `keep_empty`. If set to `true`, the links to entities that are `null` (i.e., no such entity exists) will create an empty entity for the purpose of the snapshot, and these entities will be saved as snapshots that batch. Otherwise, the `null` links will be deleted. This option is `true` by default.

File `snapshots.yml` looks like this:

```yaml
creation_rate:
  minute: "*/30"

keep_empty: true
```
