# Garbage Collector

There is only one parameter for the `GarbageCollector` as of now, the `collection_rate`.
It is a cron schedule for running the garbage collector procedures, for collecting entities with both TTL and weak [lifetimes](lifetimes.md).
See [CronExpression docs][dp3.common.config.CronExpression] for details.
By default, this will be set to once a day at 3:00 AM.

File `garbage_collector.yml` looks like this:

```yaml
collection_rate:
  minute: "5,25,45"
```
