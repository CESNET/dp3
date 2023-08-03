# History manager

History manager is reponsible for deleting old records from master records in database.

Configuration file `history_manager.yml` is very simple:

```yaml
datapoint_cleaning:
  tick_rate: 10
```

Parameter `tick_rate` sets interval how often (in minutes) should DP³ check if any data in master record of observations and timeseries attributes isn't too old and if there's something too old, removes it. To control what is considered as "too old", see parameter `max_age` in *Database entities* configuration.
