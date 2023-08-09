# Snapshots

Snapshots configuration is straightforward. Currently, it only sets `creation_rate` - a cron schedule for creating new snapshots (every 30 minutes by default). See [CronExpression docs][dp3.common.config.CronExpression] for details.

File `snapshots.yml` looks like this:

```yaml
creation_rate:
  minute: "*/30"
```
