# API

The API configuration file is currently used only to configure the logging of datapoints.
The file `api.yml` looks like this:

```yaml
cors:
  allow_origins: ["*"]

datapoint_logger:
  good_log: false
  bad_log: false  # (1)!
```

1. Use an absolute path, for example `/tmp/bad_dp.json.log`

The `cors` section is used to configure the [CORS](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) headers.
The `allow_origins` parameter is a list of allowed origins.
The default value is `["*"]`, which means that any origin is allowed.

Logging of datapoints is set in the `datapoint_logger` section.
The `good_log` and `bad_log` parameters are paths to files where the datapoints will be logged.
If set to `false`, the logging is disabled.
