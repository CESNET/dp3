# Garbage Collector

The `GarbageCollector` is responsible for removing entities that have expired [lifetimes](../lifetimes/).

There is only one parameter for the `GarbageCollector` as of now, the `collection_rate`. It is a cron schedule for running the garbage collector procedures, for collecting entities with both [TTL](../lifetimes/#time-to-live-tokens) and [weak](../lifetimes/#weak-entities) lifetimes. See CronExpression docs for details. By default, this will be set to once a day at 3:00 AM.

File `garbage_collector.yml` looks like this:

```
collection_rate:
  minute: "5,25,45"
```
