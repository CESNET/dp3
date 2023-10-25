# Garbage Collector

The `GarbageCollector` is responsible for removing entities that have expired [lifetimes](lifetimes.md).

There is only one parameter for the `GarbageCollector` as of now, the `collection_rate`.
It is a cron schedule for running the garbage collector procedures, for collecting entities with both [TTL](lifetimes.md#time-to-live-tokens) and [weak](lifetimes.md#weak-entities) lifetimes.
See [CronExpression docs][dp3.common.config.CronExpression] for details.
By default, this will be set to once a day at 3:00 AM.

File `garbage_collector.yml` looks like this:

```yaml
collection_rate:
  minute: "5,25,45"
```
